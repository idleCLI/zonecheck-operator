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

// Package placement asks kube-scheduler's own plugins where pods land.
package placement

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodevolumelimits"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

// default profile minus preemption + DRA. Default order + weights
var enabled = []config.Plugin{
	{Name: names.PrioritySort},
	{Name: names.NodeName},
	{Name: names.NodeUnschedulable},
	{Name: names.TaintToleration, Weight: 3},
	{Name: names.NodeAffinity, Weight: 2},
	{Name: names.NodePorts},
	{Name: names.NodeResourcesFit, Weight: 1},
	{Name: names.VolumeRestrictions},
	{Name: names.NodeVolumeLimits},
	{Name: names.VolumeBinding},
	{Name: names.VolumeZone},
	{Name: names.PodTopologySpread, Weight: 2},
	{Name: names.InterPodAffinity, Weight: 2},
	{Name: names.NodeResourcesBalancedAllocation, Weight: 1},
	{Name: names.ImageLocality, Weight: 1},
	{Name: names.DefaultBinder},
}

// Node "" -> didn't fit, and Reason is kube-scheduler's event message
type Result struct {
	Pod    *corev1.Pod
	Node   string
	Reason string
}

type Scheduler struct {
	sched   framework.Framework
	lister  *swapLister
	factory informers.SharedInformerFactory
	stop    context.CancelFunc
}

// the framework keeps one lister. Each Place swaps in its own snapshot
type swapLister struct{ fwk.SharedLister }

// objects are what the plugins read besides nodes + pods: PVCs, PVs, storage
// classes, CSI objects, namespaces, services + workloads. They go in through
// informers on a fake clientset, like kube-scheduler's own setup. Close when done
func New(ctx context.Context, objects []runtime.Object) (*Scheduler, error) {
	// the framework writes to these. Unregistered -> they stay off our /metrics
	initMetrics()
	profile, err := newProfile()
	if err != nil {
		return nil, err
	}
	client := fake.NewClientset(objects...)
	factory := informers.NewSharedInformerFactory(client, 0)
	lister := &swapLister{}
	sched, err := frameworkruntime.NewFramework(ctx, plugins.NewInTreeRegistry(), profile,
		frameworkruntime.WithSnapshotSharedLister(lister),
		frameworkruntime.WithInformerFactory(factory),
		frameworkruntime.WithClientSet(client),
		frameworkruntime.WithSharedCSIManager(nodevolumelimits.NewCSIManager(factory.Storage().V1().CSINodes().Lister())))
	if err != nil {
		return nil, fmt.Errorf("building scheduler framework: %w", err)
	}

	ctx, stop := context.WithCancel(ctx)
	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			stop()
			return nil, fmt.Errorf("syncing %v", typ)
		}
	}
	return &Scheduler{sched: sched, lister: lister, factory: factory, stop: stop}, nil
}

func (s *Scheduler) Close() error {
	s.stop()
	s.factory.Shutdown()
	return s.sched.Close()
}

// like kube-scheduler after a drain: highest priority first, each pod uses up
// room. Name order + name tiebreaks where it races/randomizes -> same answer
// every run. No preemption
func (s *Scheduler) Place(ctx context.Context, nodes []corev1.Node, running, pending []corev1.Pod) ([]Result, error) {
	nodePtrs := make([]*corev1.Node, len(nodes))
	for i := range nodes {
		nodePtrs[i] = &nodes[i]
	}
	podPtrs := make([]*corev1.Pod, len(running))
	for i := range running {
		podPtrs[i] = &running[i]
	}
	snap := cache.NewSnapshot(podPtrs, nodePtrs)
	if err := snap.StartMutations(); err != nil {
		return nil, err
	}
	s.lister.SharedLister = snap
	all, err := snap.NodeInfos().List()
	if err != nil {
		return nil, err
	}
	p := &placer{sched: s.sched, nodes: slices.Clone(all)}
	slices.SortFunc(p.nodes, func(a, b fwk.NodeInfo) int { return cmp.Compare(a.Node().Name, b.Node().Name) })

	queue := make([]*corev1.Pod, len(pending))
	for i := range pending {
		queue[i] = pending[i].DeepCopy()
	}
	slices.SortStableFunc(queue, func(a, b *corev1.Pod) int {
		return cmp.Or(
			cmp.Compare(corev1helpers.PodPriority(b), corev1helpers.PodPriority(a)),
			a.CreationTimestamp.Compare(b.CreationTimestamp.Time),
			cmp.Compare(a.Namespace, b.Namespace),
			cmp.Compare(a.Name, b.Name),
		)
	})

	// Reserve claims volumes (e.g. a free PV) so the next pod can't take them.
	// The scheduler is shared across failures -> undo it all at the end
	type reservation struct {
		state fwk.CycleState
		pod   *corev1.Pod
		node  string
	}
	var reserved []reservation
	defer func() {
		for _, r := range slices.Backward(reserved) {
			s.sched.RunReservePluginsUnreserve(ctx, r.state, r.pod, r.node)
		}
	}()

	results := make([]Result, 0, len(queue))
	for _, pod := range queue {
		pod.Spec.NodeName = ""
		pod.Status.NominatedNodeName = ""
		node, reason, state, err := p.schedule(ctx, pod)
		if err != nil {
			return nil, fmt.Errorf("scheduling %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if node != "" {
			if status := s.sched.RunReservePluginsReserve(ctx, state, pod, node); !status.IsSuccess() {
				s.sched.RunReservePluginsUnreserve(ctx, state, pod, node)
				node, reason = "", status.Message()
			} else {
				reserved = append(reserved, reservation{state, pod, node})
			}
		}
		if node != "" {
			pod.Spec.NodeName = node
			info, err := framework.NewPodInfo(pod)
			if err != nil {
				return nil, err
			}
			if err := snap.AddPod(info, node); err != nil {
				return nil, err
			}
		}
		results = append(results, Result{Pod: pod, Node: node, Reason: reason})
	}
	return results, nil
}

var initMetrics = sync.OnceFunc(schedulermetrics.InitMetrics)

func newProfile() (*config.KubeSchedulerProfile, error) {
	cfg, err := latest.Default()
	if err != nil {
		return nil, fmt.Errorf("loading kube-scheduler defaults: %w", err)
	}
	profile := cfg.Profiles[0]
	profile.Plugins = &config.Plugins{MultiPoint: config.PluginSet{Enabled: enabled}}
	return &profile, nil
}

type placer struct {
	sched framework.Framework
	nodes []fwk.NodeInfo // sorted by name

	// where the next search starts (kube-scheduler's nextStartNodeIndex)
	next int
}

// kube-scheduler's numFeasibleNodesToFind w/ the default percentageOfNodesToScore
func feasibleToFind(nodes int) int {
	if nodes < 100 {
		return nodes
	}
	percent := max(50-nodes/125, 5)
	return max(nodes*percent/100, 100)
}

// mirrors kube-scheduler's schedulePod: PreFilter -> Filter till enough fit -> Score
func (p *placer) schedule(ctx context.Context, pod *corev1.Pod) (node, reason string, state fwk.CycleState, err error) {
	state = framework.NewCycleState()
	if len(p.nodes) == 0 {
		return "", "no nodes left", state, nil
	}

	diagnosis := framework.Diagnosis{NodeToStatus: framework.NewDefaultNodeToStatus()}
	fitError := func() string {
		return (&framework.FitError{Pod: pod, NumAllNodes: len(p.nodes), Diagnosis: diagnosis}).Error()
	}

	preResult, status, rejectedBy := p.sched.RunPreFilterPlugins(ctx, state, pod)
	diagnosis.UnschedulablePlugins = rejectedBy
	if !status.IsSuccess() {
		if !status.IsRejected() {
			return "", "", state, status.AsError()
		}
		diagnosis.NodeToStatus.SetAbsentNodesStatus(status)
		diagnosis.PreFilterMsg = status.Message()
		return "", fitError(), state, nil
	}

	candidates := p.nodes
	if !preResult.AllNodes() {
		candidates = nil
		for _, n := range p.nodes {
			if preResult.NodeNames.Has(n.Node().Name) {
				candidates = append(candidates, n)
			}
		}
		diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
			fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(rejectedBy))))
	}

	feasible, err := p.filter(ctx, state, pod, candidates, &diagnosis)
	if err != nil {
		return "", "", state, err
	}
	switch len(feasible) {
	case 0:
		return "", fitError(), state, nil
	case 1:
		return feasible[0].Node().Name, "", state, nil
	}

	if status := p.sched.RunPreScorePlugins(ctx, state, pod, feasible); !status.IsSuccess() {
		return "", "", state, status.AsError()
	}
	scores, status := p.sched.RunScorePlugins(ctx, state, pod, feasible)
	if !status.IsSuccess() {
		return "", "", state, status.AsError()
	}
	best := slices.MinFunc(scores, func(a, b fwk.NodePluginScores) int {
		return cmp.Or(cmp.Compare(b.TotalScore, a.TotalScore), cmp.Compare(a.Name, b.Name))
	})
	return best.Name, "", state, nil
}

// from p.next till enough fit. Parallel per batch, read back in order ->
// timing can't change the answer
func (p *placer) filter(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, candidates []fwk.NodeInfo, diagnosis *framework.Diagnosis) ([]fwk.NodeInfo, error) {
	want := feasibleToFind(len(candidates))
	var feasible []fwk.NodeInfo
	checked := 0
	for checked < len(candidates) && len(feasible) < want {
		batch := make([]fwk.NodeInfo, min(want-len(feasible), len(candidates)-checked))
		for i := range batch {
			batch[i] = candidates[(p.next+checked+i)%len(candidates)]
		}
		statuses := make([]*fwk.Status, len(batch))
		p.sched.Parallelizer().Until(ctx, len(batch), func(i int) {
			statuses[i] = p.sched.RunFilterPlugins(ctx, state, pod, batch[i])
		}, "zonecheck-filter")

		for i, status := range statuses {
			checked++
			switch {
			case status.Code() == fwk.Error:
				return nil, status.AsError()
			case status.IsSuccess():
				feasible = append(feasible, batch[i])
			default:
				diagnosis.NodeToStatus.Set(batch[i].Node().Name, status)
				diagnosis.AddPluginStatus(status)
			}
		}
	}
	p.next = (p.next + checked) % len(p.nodes)
	return feasible, nil
}
