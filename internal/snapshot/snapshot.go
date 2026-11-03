/*
Copyright 2026 idleCLI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package snapshot is a point-in-time copy of nodes + pods to simulate on.
package snapshot

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/node/topology"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
)

// sorted by name -> repeatable runs
type Snapshot struct {
	Nodes []corev1.Node

	// bound to a node in Nodes + running or starting (no unbound, finished, or terminating pods)
	Pods []corev1.Pod

	// still take room till they're gone, but never move
	Terminating []corev1.Pod

	Jobs map[types.NamespacedName]*batchv1.Job
	PDBs []policyv1.PodDisruptionBudget

	// what kube-scheduler's plugins read besides nodes + pods (volumes,
	// namespaces for pod affinity, workloads for default spreading)
	Objects []runtime.Object

	// fresh claims (in Objects) for ephemeral volumes of displaced pods
	freshClaims map[types.NamespacedName]bool

	// ReplicaSet -> its Deployment (or whatever controls it)
	rsOwners map[types.NamespacedName]*metav1.OwnerReference
}

// one concrete thing to lose, e.g. "zone a"
type Failure struct {
	Type   checksv1alpha1.ScenarioType
	Target string
	Nodes  []string
}

// a pod its controller recreates after a drain
type Displaced struct {
	Pod corev1.Pod

	// why an outage leaves it on the dead node. "" -> it moves
	StuckReason string
}

// nothing recreates it
type Lost struct {
	Pod    corev1.Pod
	Reason string
}

type Loss struct {
	Displaced []Displaced
	Lost      []Lost
}

// everything Take lists. The controller starts these informers up front
var Watched = []client.Object{
	&corev1.Node{}, &corev1.Pod{}, &batchv1.Job{}, &policyv1.PodDisruptionBudget{},
	&corev1.PersistentVolumeClaim{}, &corev1.PersistentVolume{}, &corev1.Namespace{},
	&corev1.Service{}, &corev1.ReplicationController{}, &appsv1.ReplicaSet{}, &appsv1.StatefulSet{},
	&storagev1.StorageClass{}, &storagev1.CSINode{}, &storagev1.CSIDriver{},
	&storagev1.CSIStorageCapacity{}, &storagev1.VolumeAttachment{},
}

func Take(ctx context.Context, c client.Reader) (*Snapshot, error) {
	var (
		nodes corev1.NodeList
		pods  corev1.PodList
		jobs  batchv1.JobList
		pdbs  policyv1.PodDisruptionBudgetList
	)
	for _, l := range []client.ObjectList{&nodes, &pods, &jobs, &pdbs} {
		if err := c.List(ctx, l); err != nil {
			return nil, fmt.Errorf("listing %T: %w", l, err)
		}
	}

	s := &Snapshot{Nodes: nodes.Items, PDBs: pdbs.Items, Jobs: map[types.NamespacedName]*batchv1.Job{}}
	for _, l := range []client.ObjectList{
		&corev1.PersistentVolumeClaimList{}, &corev1.PersistentVolumeList{}, &corev1.NamespaceList{},
		&corev1.ServiceList{}, &corev1.ReplicationControllerList{}, &appsv1.ReplicaSetList{}, &appsv1.StatefulSetList{},
		&storagev1.StorageClassList{}, &storagev1.CSINodeList{}, &storagev1.CSIDriverList{},
		&storagev1.CSIStorageCapacityList{}, &storagev1.VolumeAttachmentList{},
	} {
		if err := c.List(ctx, l); err != nil {
			return nil, fmt.Errorf("listing %T: %w", l, err)
		}
		objs, err := meta.ExtractList(l)
		if err != nil {
			return nil, err
		}
		s.Objects = append(s.Objects, objs...)
	}
	s.rsOwners = map[types.NamespacedName]*metav1.OwnerReference{}
	for _, o := range s.Objects {
		if rs, ok := o.(*appsv1.ReplicaSet); ok {
			if owner := metav1.GetControllerOf(rs); owner != nil {
				s.rsOwners[types.NamespacedName{Namespace: rs.Namespace, Name: rs.Name}] = owner
			}
		}
	}

	known := make(map[string]bool, len(s.Nodes))
	for _, n := range s.Nodes {
		known[n.Name] = true
	}
	for _, p := range pods.Items {
		if !known[p.Spec.NodeName] || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		// terminating -> its replacement usually exists already, don't move it too
		if p.DeletionTimestamp != nil {
			s.Terminating = append(s.Terminating, p)
		} else {
			s.Pods = append(s.Pods, p)
		}
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		s.Jobs[types.NamespacedName{Namespace: j.Namespace, Name: j.Name}] = j
	}

	slices.SortFunc(s.Nodes, func(a, b corev1.Node) int { return cmp.Compare(a.Name, b.Name) })
	byName := func(a, b corev1.Pod) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}
	slices.SortFunc(s.Pods, byName)
	slices.SortFunc(s.Terminating, byName)
	s.addFreshClaims()
	return s, nil
}

// one per zone for Zone. NodePool always gives one, even w/ no matching
// nodes -> a typo in the pool name shows up as 0 lost nodes
func (s *Snapshot) Failures(sc checksv1alpha1.Scenario) []Failure {
	switch sc.Type {
	case checksv1alpha1.ScenarioZone:
		label := sc.ZoneLabelOrDefault()
		byZone := map[string][]string{}
		for _, n := range s.Nodes {
			if zone := n.Labels[label]; zone != "" {
				byZone[zone] = append(byZone[zone], n.Name)
			}
		}
		out := make([]Failure, 0, len(byZone))
		for _, z := range slices.Sorted(maps.Keys(byZone)) {
			out = append(out, Failure{Type: sc.Type, Target: z, Nodes: byZone[z]})
		}
		return out

	case checksv1alpha1.ScenarioBusiestNode:
		if n := s.busiestNode(); n != "" {
			return []Failure{{Type: sc.Type, Target: n, Nodes: []string{n}}}
		}
		return nil

	case checksv1alpha1.ScenarioNodePool:
		if sc.NodePool == nil {
			return nil
		}
		f := Failure{Type: sc.Type, Target: sc.NodePool.Name}
		for _, n := range s.Nodes {
			if n.Labels[sc.NodePool.LabelKey] == sc.NodePool.Name {
				f.Nodes = append(f.Nodes, n.Name)
			}
		}
		return []Failure{f}
	}
	return nil
}

// nodes w/o a zone label value. Zone failures skip them (they stay as capacity)
func (s *Snapshot) Unzoned(label string) []string {
	var out []string
	for _, n := range s.Nodes {
		if n.Labels[label] == "" {
			out = append(out, n.Name)
		}
	}
	return out
}

// node whose movable pods take the biggest share of cluster CPU or memory.
// Ties -> first name
func (s *Snapshot) busiestNode() string {
	var totalCPU, totalMem int64
	for _, n := range s.Nodes {
		totalCPU += n.Status.Allocatable.Cpu().MilliValue()
		totalMem += n.Status.Allocatable.Memory().Value()
	}

	// same accounting as kube-scheduler 1.37 (in-place resize + pod-level resources on by default)
	opts := resourcehelper.PodResourcesOptions{UseStatusResources: true, InPlacePodLevelResourcesVerticalScalingEnabled: true}
	cpu, mem := map[string]int64{}, map[string]int64{}
	for i := range s.Pods {
		p := &s.Pods[i]
		if goesWithNode(p) || metav1.GetControllerOf(p) == nil {
			continue
		}
		req := resourcehelper.PodRequests(p, opts)
		cpu[p.Spec.NodeName] += req.Cpu().MilliValue()
		mem[p.Spec.NodeName] += req.Memory().Value()
	}

	best, bestShare := "", -1.0
	for _, n := range s.Nodes {
		share := max(fraction(cpu[n.Name], totalCPU), fraction(mem[n.Name], totalMem))
		if share > bestShare {
			best, bestShare = n.Name, share
		}
	}
	return best
}

func fraction(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return float64(part) / float64(whole)
}

// cluster after draining nodes + what happens to their pods. Drained nodes stay
// cordoned w/ their DaemonSet + static pods (kube-scheduler still counts them,
// e.g. for zone spread). Displaced pods are copies w/ nodeName cleared. Shares
// objects w/ s, don't modify
func (s *Snapshot) Drain(nodes []string, ev *checksv1alpha1.NodeEviction) (*Snapshot, Loss) {
	gone := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		gone[n] = true
	}

	drained := &Snapshot{Jobs: s.Jobs, PDBs: s.PDBs, Objects: s.Objects}
	zoneOf := make(map[string]string, len(s.Nodes))
	for i := range s.Nodes {
		n := &s.Nodes[i]
		zoneOf[n.Name] = topology.GetZoneKey(n)
		if gone[n.Name] {
			drained.Nodes = append(drained.Nodes, *cordoned(n))
		} else {
			drained.Nodes = append(drained.Nodes, *n)
		}
	}
	for _, p := range s.Terminating {
		if !gone[p.Spec.NodeName] {
			drained.Terminating = append(drained.Terminating, p)
		}
	}
	stopped := s.evictionStopped(gone, ev)

	// evictions count as Job failures (backoffLimit) -> tally per Job first
	evictedFromJob := map[types.NamespacedName]int32{}
	for i := range s.Pods {
		p := &s.Pods[i]
		if job := s.jobOf(p); gone[p.Spec.NodeName] && job != nil {
			evictedFromJob[client.ObjectKeyFromObject(job)]++
		}
	}

	var loss Loss
	for i := range s.Pods {
		p := &s.Pods[i]
		if !gone[p.Spec.NodeName] || goesWithNode(p) {
			drained.Pods = append(drained.Pods, *p)
			continue
		}
		if metav1.GetControllerOf(p) == nil {
			loss.Lost = append(loss.Lost, Lost{Pod: *p, Reason: "no controller to recreate it"})
			continue
		}
		job := s.jobOf(p)
		if job != nil && passesBackoffLimit(job, evictedFromJob[client.ObjectKeyFromObject(job)]) {
			loss.Lost = append(loss.Lost, Lost{Pod: *p, Reason: "Job fails: evicted pods push it past backoffLimit"})
			continue
		}
		moved := p.DeepCopy()
		moved.Spec.NodeName = ""
		s.useFreshClaims(moved)
		loss.Displaced = append(loss.Displaced, Displaced{
			Pod:         *moved,
			StuckReason: cmp.Or(stuckReason(p, job), stopped[zoneOf[p.Spec.NodeName]]),
		})
	}
	return drained, loss
}

// a replacement pod gets a new claim from its ephemeral volume's template, not
// the old one (which is bound to the old pod + often the old zone). Only for
// WaitForFirstConsumer classes: an Immediate class provisions wherever, so
// the volume is just dropped from the check
func (s *Snapshot) addFreshClaims() {
	classes := map[string]*storagev1.StorageClass{}
	var defaultClass *storagev1.StorageClass
	for _, o := range s.Objects {
		sc, ok := o.(*storagev1.StorageClass)
		if !ok {
			continue
		}
		classes[sc.Name] = sc
		// several defaults -> newest wins, then first by name (like the PVC admission plugin)
		if isDefaultClass(sc) && (defaultClass == nil || cmp.Or(
			defaultClass.CreationTimestamp.Compare(sc.CreationTimestamp.Time),
			cmp.Compare(sc.Name, defaultClass.Name)) < 0) {
			defaultClass = sc
		}
	}

	s.freshClaims = map[types.NamespacedName]bool{}
	for i := range s.Pods {
		p := &s.Pods[i]
		for _, v := range p.Spec.Volumes {
			if v.Ephemeral == nil || v.Ephemeral.VolumeClaimTemplate == nil {
				continue
			}
			tmpl := v.Ephemeral.VolumeClaimTemplate
			class := defaultClass
			if tmpl.Spec.StorageClassName != nil {
				class = classes[*tmpl.Spec.StorageClassName]
			}
			if class == nil || class.VolumeBindingMode == nil || *class.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
				continue
			}
			claim := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: freshClaimName(p, &v),
					Labels: tmpl.Labels, Annotations: tmpl.Annotations},
				Spec: *tmpl.Spec.DeepCopy(),
			}
			claim.Spec.StorageClassName = &class.Name
			s.Objects = append(s.Objects, claim)
			s.freshClaims[types.NamespacedName{Namespace: p.Namespace, Name: claim.Name}] = true
		}
	}
}

func isDefaultClass(sc *storagev1.StorageClass) bool {
	return sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
		sc.Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true"
}

func freshClaimName(p *corev1.Pod, v *corev1.Volume) string {
	return p.Name + "-" + v.Name + "-replacement"
}

// points ephemeral volumes at the fresh claims (see addFreshClaims)
func (s *Snapshot) useFreshClaims(p *corev1.Pod) {
	vols := make([]corev1.Volume, 0, len(p.Spec.Volumes))
	for _, v := range p.Spec.Volumes {
		if v.Ephemeral == nil {
			vols = append(vols, v)
			continue
		}
		name := freshClaimName(p, &v)
		if !s.freshClaims[types.NamespacedName{Namespace: p.Namespace, Name: name}] {
			continue
		}
		v.VolumeSource = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}
		vols = append(vols, v)
	}
	p.Spec.Volumes = vols
}

// like kubectl cordon, plus the taint the node controller adds for it
func cordoned(n *corev1.Node) *corev1.Node {
	n = n.DeepCopy()
	n.Spec.Unschedulable = true
	for _, t := range n.Spec.Taints {
		if t.Key == corev1.TaintNodeUnschedulable {
			return n
		}
	}
	n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: corev1.TaintNodeUnschedulable, Effect: corev1.TaintEffectNoSchedule})
	return n
}

// mirrors the node lifecycle controller: >2 nodes + >= threshold of a zone down
// -> partial disruption, small zones stop evicting. Every zone down -> nothing
// gets evicted. Nodes already NotReady count as down
func (s *Snapshot) evictionStopped(gone map[string]bool, ev *checksv1alpha1.NodeEviction) map[string]string {
	type zone struct{ total, down int32 }
	zones := map[string]*zone{}
	for i := range s.Nodes {
		n := &s.Nodes[i]
		k := topology.GetZoneKey(n)
		if zones[k] == nil {
			zones[k] = &zone{}
		}
		zones[k].total++
		if gone[n.Name] || !ready(n) {
			zones[k].down++
		}
	}

	stopped := map[string]string{}
	allDown := len(zones) > 0
	for k, z := range zones {
		if z.down < z.total {
			allDown = false
		}
		if z.down > 2 && z.down < z.total && z.down*100 >= ev.ThresholdPercent()*z.total && z.total <= ev.LargeClusterSize() {
			stopped[k] = "node controller stops evicting in a small zone that's mostly down"
		}
	}
	if allDown {
		for k := range zones {
			stopped[k] = "node controller stops evicting when every zone is down"
		}
	}
	return stopped
}

func ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// DaemonSet + static pods die w/ their node
func goesWithNode(p *corev1.Pod) bool {
	if _, ok := p.Annotations[corev1.MirrorPodAnnotationKey]; ok {
		return true
	}
	return ownedBy(p, "apps/v1", "DaemonSet")
}

var unreachable = corev1.Taint{Key: corev1.TaintNodeUnreachable, Effect: corev1.TaintEffectNoExecute}

func stuckReason(p *corev1.Pod, job *batchv1.Job) string {
	if ownedBy(p, "apps/v1", "StatefulSet") {
		return "StatefulSet pods wait for the node or pod to be deleted"
	}
	if job != nil && (job.Spec.PodFailurePolicy != nil ||
		job.Spec.PodReplacementPolicy != nil && *job.Spec.PodReplacementPolicy == batchv1.Failed) {
		return "Job only replaces pods once they've failed"
	}
	for i := range p.Spec.Tolerations {
		t := &p.Spec.Tolerations[i]
		if t.TolerationSeconds == nil && t.ToleratesTaint(klog.Background(), &unreachable, false) {
			return "tolerates node.kubernetes.io/unreachable forever"
		}
	}
	return ""
}

// skips Jobs w/ a pod failure policy (it can ignore evictions) or per-index limits
func passesBackoffLimit(job *batchv1.Job, evicted int32) bool {
	if job.Spec.PodFailurePolicy != nil || job.Spec.BackoffLimitPerIndex != nil {
		return false
	}
	limit := int32(6)
	if job.Spec.BackoffLimit != nil {
		limit = *job.Spec.BackoffLimit
	}
	return job.Status.Failed+evicted > limit
}

func (s *Snapshot) jobOf(p *corev1.Pod) *batchv1.Job {
	owner := metav1.GetControllerOf(p)
	if owner == nil || owner.APIVersion != "batch/v1" || owner.Kind != "Job" {
		return nil
	}
	return s.Jobs[types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}]
}

func ownedBy(p *corev1.Pod, apiVersion, kind string) bool {
	owner := metav1.GetControllerOf(p)
	return owner != nil && owner.APIVersion == apiVersion && owner.Kind == kind
}

// Workload is what a person would call the pod's owner, like "Deployment/web"
// (not the ReplicaSet, whose name changes every rollout). "" -> no controller
func (s *Snapshot) Workload(p *corev1.Pod) string {
	owner := metav1.GetControllerOf(p)
	if owner == nil {
		return ""
	}
	key := types.NamespacedName{Namespace: p.Namespace, Name: owner.Name}
	switch owner.Kind {
	case "ReplicaSet":
		if up := s.rsOwners[key]; up != nil {
			owner = up
		}
	case "Job":
		if j := s.Jobs[key]; j != nil {
			if up := metav1.GetControllerOf(j); up != nil {
				owner = up
			}
		}
	}
	return owner.Kind + "/" + owner.Name
}
