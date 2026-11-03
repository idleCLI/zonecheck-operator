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

package placement

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

func node(name string) corev1.Node {
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelHostname: name}},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("16Gi"),
			corev1.ResourcePods:   resource.MustParse("110"),
		}},
	}
}

func pod(name, nodeName, cpu string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: k8stypes.UID("uid-" + name)},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{Name: "c", Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
			}}},
		},
	}
}

// pod name -> node, or "- reason" if it didn't fit
func placed(t *testing.T, nodes []corev1.Node, running, pending []corev1.Pod) map[string]string {
	t.Helper()
	return placedWith(t, nil, nodes, running, pending)
}

func placedWith(t *testing.T, objects []runtime.Object, nodes []corev1.Node, running, pending []corev1.Pod) map[string]string {
	t.Helper()
	s, err := New(t.Context(), objects)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	results, err := s.Place(t.Context(), nodes, running, pending)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range results {
		if r.Node == "" {
			out[r.Pod.Name] = "- " + r.Reason
		} else {
			out[r.Pod.Name] = r.Node
		}
	}
	return out
}

func TestPlaceResources(t *testing.T) {
	nodes := []corev1.Node{node("a"), node("b")}
	running := []corev1.Pod{pod("busy", "a", "3")}

	got := placed(t, nodes, running, []corev1.Pod{pod("small", "", "1"), pod("huge", "", "8")})

	if got["small"] != "b" {
		t.Errorf("small went to %q, want the emptier node b", got["small"])
	}
	if want := "- 0/2 nodes are available: 2 Insufficient cpu."; got["huge"] != want {
		t.Errorf("huge = %q, want %q", got["huge"], want)
	}
}

func TestPlaceUsesUpRoomInPriorityOrder(t *testing.T) {
	low := pod("low", "", "3")
	high := pod("high", "", "3")
	high.Spec.Priority = new(int32(1000))

	got := placed(t, []corev1.Node{node("a")}, nil, []corev1.Pod{low, high})

	if got["high"] != "a" {
		t.Errorf("high = %q, want a", got["high"])
	}
	if !strings.HasPrefix(got["low"], "- 0/1 nodes are available: 1 Insufficient cpu") {
		t.Errorf("low = %q, want no room", got["low"])
	}
}

func TestPlaceOlderPodsFirstAtEqualPriority(t *testing.T) {
	older := pod("older", "", "3")
	older.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	newer := pod("newer", "", "3")
	newer.CreationTimestamp = metav1.Now()

	got := placed(t, []corev1.Node{node("a")}, nil, []corev1.Pod{newer, older})
	if got["older"] != "a" || got["newer"] == "a" {
		t.Errorf("got %v, want older placed first", got)
	}
}

func TestPlaceRules(t *testing.T) {
	tainted := node("tainted")
	tainted.Spec.Taints = []corev1.Taint{{Key: "gpu", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	cordoned := node("cordoned")
	cordoned.Spec.Unschedulable = true
	labeled := node("labeled")
	labeled.Labels["disk"] = "ssd"

	tolerant := pod("tolerant", "", "1")
	tolerant.Spec.Tolerations = []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists}}
	tolerant.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "tainted"}

	picky := pod("picky", "", "1")
	picky.Spec.NodeSelector = map[string]string{"disk": "nvme"}

	wantsSSD := pod("wants-ssd", "", "1")
	wantsSSD.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "disk", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"}}},
		}}},
	}}

	withPort := func(name, nodeName string) corev1.Pod {
		p := pod(name, nodeName, "0")
		p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80, HostPort: 8080, Protocol: corev1.ProtocolTCP}}
		p.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "labeled"}
		return p
	}

	got := placed(t,
		[]corev1.Node{tainted, cordoned, labeled},
		[]corev1.Pod{withPort("port-holder", "labeled")},
		[]corev1.Pod{tolerant, picky, wantsSSD, withPort("port-taker", "")},
	)

	want := map[string]string{
		"tolerant":  "tainted",
		"wants-ssd": "labeled",
	}
	for name, node := range want {
		if got[name] != node {
			t.Errorf("%s = %q, want %q", name, got[name], node)
		}
	}
	wantReason := map[string]string{
		"picky":      "didn't match Pod's node affinity/selector",
		"port-taker": "didn't have free ports for the requested pod ports",
	}
	for name, reason := range wantReason {
		if !strings.Contains(got[name], reason) {
			t.Errorf("%s = %q, want reason containing %q", name, got[name], reason)
		}
	}
	for _, reason := range []string{"untolerated taint", "unschedulable"} {
		if !strings.Contains(got["picky"], reason) {
			t.Errorf("picky = %q, want it to mention %q", got["picky"], reason)
		}
	}
}

func TestPlaceTiesGoToFirstNode(t *testing.T) {
	for range 5 {
		got := placed(t, []corev1.Node{node("c"), node("a"), node("b")}, nil, []corev1.Pod{pod("p", "", "1")})
		if got["p"] != "a" {
			t.Fatalf("p = %q, want a", got["p"])
		}
	}
}

func TestPlaceNoNodes(t *testing.T) {
	got := placed(t, nil, nil, []corev1.Pod{pod("p", "", "1")})
	if got["p"] != "- no nodes left" {
		t.Errorf("p = %q", got["p"])
	}
}

func TestPlaceDoesNotModifyInput(t *testing.T) {
	pending := []corev1.Pod{pod("p", "", "1")}
	placed(t, []corev1.Node{node("a")}, nil, pending)
	if pending[0].Spec.NodeName != "" {
		t.Errorf("pending pod was bound to %q", pending[0].Spec.NodeName)
	}
}

func TestFeasibleToFind(t *testing.T) {
	// has to match kube-scheduler's numFeasibleNodesToFind
	for nodes, want := range map[int]int{50: 50, 99: 99, 100: 100, 500: 230, 1000: 420, 5000: 500, 20000: 1000} {
		if got := feasibleToFind(nodes); got != want {
			t.Errorf("feasibleToFind(%d) = %d, want %d", nodes, got, want)
		}
	}
}

func bigCluster(n int) []corev1.Node {
	nodes := make([]corev1.Node, n)
	for i := range nodes {
		nodes[i] = node(fmt.Sprintf("n%04d", i))
	}
	return nodes
}

func TestPlaceSamplingStillFindsTheOnlyFit(t *testing.T) {
	nodes := bigCluster(250)
	for i := range nodes[:249] {
		nodes[i].Spec.Taints = []corev1.Taint{{Key: "busy", Effect: corev1.TaintEffectNoSchedule}}
	}
	got := placed(t, nodes, nil, []corev1.Pod{pod("p", "", "1")})
	if got["p"] != "n0249" {
		t.Errorf("p = %q, want n0249", got["p"])
	}
}

func TestPlaceIsRepeatableOnBigClusters(t *testing.T) {
	pending := make([]corev1.Pod, 300)
	for i := range pending {
		pending[i] = pod(fmt.Sprintf("p%03d", i), "", "500m")
	}
	first := placed(t, bigCluster(150), nil, pending)
	for range 3 {
		if again := placed(t, bigCluster(150), nil, pending); fmt.Sprint(again) != fmt.Sprint(first) {
			t.Fatal("placement changed between runs")
		}
	}
}

func BenchmarkPlace1000Nodes(b *testing.B) {
	nodes := bigCluster(1000)
	running := make([]corev1.Pod, 0, 20*len(nodes))
	for _, n := range nodes {
		for j := range 20 {
			running = append(running, pod(fmt.Sprintf("%s-%d", n.Name, j), n.Name, "100m"))
		}
	}
	pending := make([]corev1.Pod, len(running)/3)
	for i := range pending {
		pending[i] = pod(fmt.Sprintf("p%05d", i), "", "100m")
	}
	s, err := New(b.Context(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for b.Loop() {
		if _, err := s.Place(b.Context(), nodes, running, pending); err != nil {
			b.Fatal(err)
		}
	}
}

func zonedNode(name, zone string) corev1.Node {
	n := node(name)
	n.Labels[corev1.LabelTopologyZone] = zone
	return n
}

// what snapshot.Drain does to a lost node
func drained(n corev1.Node) corev1.Node {
	n.Spec.Unschedulable = true
	n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: corev1.TaintNodeUnschedulable, Effect: corev1.TaintEffectNoSchedule})
	return n
}

func TestPlaceVolumePinnedToLostZone(t *testing.T) {
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "zonal"}, Provisioner: "disk.csi.example.com"}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-a"},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "zonal",
			Capacity:         corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			ClaimRef:         &corev1.ObjectReference{Namespace: "default", Name: "data"},
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
				Driver: "disk.csi.example.com", VolumeHandle: "vol-a",
			}},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}},
			}}}},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data",
			Annotations: map[string]string{"pv.kubernetes.io/bind-completed": "yes"}},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: new("zonal"),
			VolumeName:       "pv-a",
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	db := pod("db-0", "", "1")
	db.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
	}}}

	got := placedWith(t, []runtime.Object{sc, pv, pvc},
		[]corev1.Node{drained(zonedNode("a1", "a")), zonedNode("b1", "b"), zonedNode("c1", "c")},
		nil, []corev1.Pod{db, pod("stateless", "", "1")})

	want := "- 0/3 nodes are available: 1 node(s) were unschedulable, 2 node(s) didn't match PersistentVolume's node affinity."
	if got["db-0"] != want {
		t.Errorf("db-0 = %q, want %q", got["db-0"], want)
	}
	if got["stateless"] == "" || strings.HasPrefix(got["stateless"], "-") {
		t.Errorf("stateless = %q, want it placed", got["stateless"])
	}
}

func TestPlaceZoneSpreadAfterLosingAZone(t *testing.T) {
	spread := func(name, nodeName string, policy *corev1.NodeInclusionPolicy) corev1.Pod {
		p := pod(name, nodeName, "100m")
		p.Labels = map[string]string{"app": "web"}
		p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew:           1,
			TopologyKey:       corev1.LabelTopologyZone,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			NodeTaintsPolicy:  policy,
		}}
		return p
	}
	nodes := []corev1.Node{drained(zonedNode("a1", "a")), zonedNode("b1", "b"), zonedNode("c1", "c")}

	for name, tc := range map[string]struct {
		policy *corev1.NodeInclusionPolicy
		fits   bool
	}{
		// the lost zone still counts, w/ 0 pods -> nothing else is allowed more
		"taints ignored (default)": {nil, false},
		"taints honored":           {new(corev1.NodeInclusionPolicyHonor), true},
	} {
		t.Run(name, func(t *testing.T) {
			running := []corev1.Pod{
				spread("web-b", "b1", tc.policy), spread("web-c", "c1", tc.policy),
			}
			got := placed(t, nodes, running, []corev1.Pod{spread("web-a", "", tc.policy)})
			if fits := !strings.HasPrefix(got["web-a"], "-"); fits != tc.fits {
				t.Errorf("web-a = %q, want fits=%v", got["web-a"], tc.fits)
			}
			if !tc.fits && !strings.Contains(got["web-a"], "didn't match pod topology spread constraints") {
				t.Errorf("web-a = %q, want a topology spread reason", got["web-a"])
			}
		})
	}
}

func TestPlacePodAntiAffinity(t *testing.T) {
	oneper := func(name, nodeName string) corev1.Pod {
		p := pod(name, nodeName, "100m")
		p.Labels = map[string]string{"app": "etcd"}
		p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:   corev1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "etcd"}},
			}},
		}}
		return p
	}
	got := placed(t, []corev1.Node{node("a"), node("b")},
		[]corev1.Pod{oneper("etcd-0", "a"), oneper("etcd-1", "b")},
		[]corev1.Pod{oneper("etcd-2", "")})
	if !strings.Contains(got["etcd-2"], "didn't match pod anti-affinity rules") {
		t.Errorf("etcd-2 = %q, want an anti-affinity reason", got["etcd-2"])
	}
}

func TestPlaceReserveStopsTwoPodsTakingOnePV(t *testing.T) {
	sc := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: "local"},
		Provisioner:       "kubernetes.io/no-provisioner",
		VolumeBindingMode: new(storagev1.VolumeBindingWaitForFirstConsumer),
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "free"},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName:       "local",
			Capacity:               corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeSource: corev1.PersistentVolumeSource{Local: &corev1.LocalVolumeSource{Path: "/mnt/disk"}},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}},
			}}}},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeAvailable},
	}
	claim := func(name string) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: new("local"),
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
		}
	}
	using := func(name, claimName string) corev1.Pod {
		p := pod(name, "", "100m")
		p.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
		}}}
		return p
	}

	s, err := New(t.Context(), []runtime.Object{sc, pv, claim("first"), claim("second")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// twice: the reservation from the first run has to be undone for the second
	for run := range 2 {
		results, err := s.Place(t.Context(), []corev1.Node{node("a")}, nil,
			[]corev1.Pod{using("pod-1", "first"), using("pod-2", "second")})
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Node != "a" {
			t.Errorf("run %d: pod-1 = %q %q, want a", run, results[0].Node, results[0].Reason)
		}
		if results[1].Node != "" || !strings.Contains(results[1].Reason, "didn't find available persistent volumes to bind") {
			t.Errorf("run %d: pod-2 = %q %q, want no PV left", run, results[1].Node, results[1].Reason)
		}
	}
}
