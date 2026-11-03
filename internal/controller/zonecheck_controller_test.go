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

package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
	"github.com/idleCLI/zonecheck-operator/internal/metrics"
)

var _ = Describe("ZoneCheck Controller", func() {
	ctx := context.Background()

	newCheck := func(name string, scenarios ...checksv1alpha1.Scenario) *checksv1alpha1.ZoneCheck {
		return &checksv1alpha1.ZoneCheck{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       checksv1alpha1.ZoneCheckSpec{Scenarios: scenarios},
		}
	}

	var results *metrics.Collector
	BeforeEach(func() { results = metrics.New() })

	reconcileAs := func(c client.Client, name string) (reconcile.Result, error) {
		r := &ZoneCheckReconciler{Client: c, Scheme: c.Scheme(), Metrics: results}
		return r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	}

	Context("When reconciling a resource", func() {
		const name = "test-resource"
		key := types.NamespacedName{Name: name}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, newCheck(name,
				checksv1alpha1.Scenario{Type: checksv1alpha1.ScenarioZone},
			))).To(Succeed())
		})

		AfterEach(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, newCheck(name)))).To(Succeed())
		})

		It("applies defaults", func() {
			zc := &checksv1alpha1.ZoneCheck{}
			Expect(k8sClient.Get(ctx, key, zc)).To(Succeed())
			Expect(zc.Spec.Interval.Duration).To(Equal(5 * time.Minute))
			Expect(zc.Spec.Scenarios[0].ZoneLabelOrDefault()).To(Equal(corev1.LabelTopologyZone))
		})

		It("runs, records the run, and waits out the interval", func() {
			res, err := reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(5 * time.Minute))

			zc := &checksv1alpha1.ZoneCheck{}
			Expect(k8sClient.Get(ctx, key, zc)).To(Succeed())
			Expect(zc.Status.LastRunTime).NotTo(BeNil())
			Expect(zc.Status.ObservedGeneration).To(Equal(zc.Generation))
			Expect(meta.IsStatusConditionTrue(zc.Status.Conditions, checksv1alpha1.ConditionReady)).To(BeTrue())
			firstRun := zc.Status.LastRunTime.Time

			By("being woken early, like on a resync")
			res, err = reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 4*time.Minute))
			Expect(res.RequeueAfter).To(BeNumerically("<=", 5*time.Minute))
			Expect(k8sClient.Get(ctx, key, zc)).To(Succeed())
			Expect(zc.Status.LastRunTime.Time).To(Equal(firstRun))

			By("running right away after a restart, so the metrics aren't empty for a whole interval")
			results = metrics.New()
			res, err = reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
			Expect(results.Has(name)).To(BeTrue())

			By("running again right away when the spec changes")
			zc.Spec.Scenarios = append(zc.Spec.Scenarios, checksv1alpha1.Scenario{Type: checksv1alpha1.ScenarioBusiestNode})
			Expect(k8sClient.Update(ctx, zc)).To(Succeed())
			res, err = reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(5 * time.Minute))
			Expect(k8sClient.Get(ctx, key, zc)).To(Succeed())
			Expect(zc.Status.ObservedGeneration).To(Equal(zc.Generation))
		})

		It("drops a deleted ZoneCheck's metrics", func() {
			_, err := reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(testutil.CollectAndCount(results)).To(BeNumerically(">", 0))

			Expect(k8sClient.Delete(ctx, newCheck(name))).To(Succeed())
			_, err = reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())
			Expect(testutil.CollectAndCount(results)).To(BeZero())
		})
	})

	Context("With nodes and pods", func() {
		const name = "results"
		var objs []client.Object

		makeNode := func(name, zone string) *corev1.Node {
			n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
			if zone != "" {
				n.Labels[corev1.LabelTopologyZone] = zone
			}
			Expect(k8sClient.Create(ctx, n)).To(Succeed())
			n.Status = corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("16Gi"),
					corev1.ResourcePods:   resource.MustParse("110"),
				},
			}
			Expect(k8sClient.Status().Update(ctx, n)).To(Succeed())
			// API server taints new nodes not-ready + no node controller here to lift it
			n.Spec.Taints = nil
			Expect(k8sClient.Update(ctx, n)).To(Succeed())
			return n
		}
		makePod := func(name, nodeName, ownerKind, cpu string, opts ...func(*corev1.Pod)) *corev1.Pod {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
				Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{
					Name: "c", Image: "x",
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}},
				}}},
			}
			if ownerKind != "" {
				p.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: appsv1.SchemeGroupVersion.String(), Kind: ownerKind, Name: name, UID: "1234", Controller: new(true),
				}}
			}
			for _, opt := range opts {
				opt(p)
			}
			Expect(k8sClient.Create(ctx, p)).To(Succeed())
			// API server sets Pending, and Pending/unready pods can skip PDBs
			p.Status.Phase = corev1.PodRunning
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			return p
		}
		// a disk that only zone z1 can mount, bound to PVC "data"
		makeVolume := func() []client.Object {
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv-z1"},
				Spec: corev1.PersistentVolumeSpec{
					Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					ClaimRef:    &corev1.ObjectReference{Namespace: "default", Name: "data"},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
						Driver: "disk.csi.example.com", VolumeHandle: "vol-1",
					}},
					NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"z1"},
						}},
					}}}},
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "data",
					Annotations: map[string]string{"pv.kubernetes.io/bind-completed": "yes"}},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
					VolumeName:       "pv-z1",
					StorageClassName: new(""),
				},
			}
			Expect(k8sClient.Create(ctx, pv)).To(Succeed())
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
			pvc.Status.Phase = corev1.ClaimBound
			Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
			return []client.Object{pv, pvc}
		}
		usesData := func(p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}}
		}
		webLabel := func(p *corev1.Pod) { p.Labels = map[string]string{"app": "web"} }
		// ephemeral volume whose current claim sits on a z1-only disk.
		// a replacement pod gets a new claim -> shouldn't be stuck on z1
		makeCache := func() []client.Object {
			sc := &storagev1.StorageClass{
				ObjectMeta:        metav1.ObjectMeta{Name: "wffc"},
				Provisioner:       "disk.csi.example.com",
				VolumeBindingMode: new(storagev1.VolumeBindingWaitForFirstConsumer),
			}
			Expect(k8sClient.Create(ctx, sc)).To(Succeed())
			pod := makePod("cache-1", "n1", "ReplicaSet", "1", func(p *corev1.Pod) {
				p.Spec.Volumes = []corev1.Volume{{Name: "tmp", VolumeSource: corev1.VolumeSource{
					Ephemeral: &corev1.EphemeralVolumeSource{VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
						Spec: corev1.PersistentVolumeClaimSpec{
							AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
							Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
							StorageClassName: new("wffc"),
						},
					}},
				}}}
			})
			pv := &corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "pv-cache"},
				Spec: corev1.PersistentVolumeSpec{
					Capacity:         corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: "wffc",
					ClaimRef:         &corev1.ObjectReference{Namespace: "default", Name: "cache-1-tmp"},
					PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
						Driver: "disk.csi.example.com", VolumeHandle: "vol-2",
					}},
					NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"z1"},
						}},
					}}}},
				},
			}
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cache-1-tmp",
					Annotations: map[string]string{"pv.kubernetes.io/bind-completed": "yes"},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: new(true),
					}}},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
					VolumeName:       "pv-cache",
					StorageClassName: new("wffc"),
				},
			}
			Expect(k8sClient.Create(ctx, pv)).To(Succeed())
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
			pvc.Status.Phase = corev1.ClaimBound
			Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
			return []client.Object{pod, pvc, pv, sc}
		}

		BeforeEach(func() {
			objs = append(makeVolume(),
				makeNode("n1", "z1"), makeNode("n2", "z2"), makeNode("n3", ""),
				makePod("db-0", "n1", "StatefulSet", "1", usesData), makePod("scratch", "n1", "", "1"),
				makePod("web-1", "n1", "ReplicaSet", "1", webLabel), makePod("big-1", "n1", "ReplicaSet", "6"),
			)
			objs = append(objs, makeCache()...)
			pdb := &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web"},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: new(intstr.FromInt32(0)),
					Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				},
			}
			Expect(k8sClient.Create(ctx, pdb)).To(Succeed())
			// what the disruption controller would write
			pdb.Status = policyv1.PodDisruptionBudgetStatus{ObservedGeneration: pdb.Generation, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 1}
			Expect(k8sClient.Status().Update(ctx, pdb)).To(Succeed())
			objs = append(objs, pdb)
			Expect(k8sClient.Create(ctx, newCheck(name, checksv1alpha1.Scenario{Type: checksv1alpha1.ScenarioZone}))).To(Succeed())
		})

		AfterEach(func() {
			// PV/PVC protection finalizers, no controller here to lift them
			noFinalizers := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"finalizers":null}}`))
			for _, o := range objs {
				Expect(k8sClient.Patch(ctx, o, noFinalizers)).To(Succeed())
				Expect(k8sClient.Delete(ctx, o, client.GracePeriodSeconds(0))).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, newCheck(name))).To(Succeed())
		})

		It("writes a result per zone and flags unzoned nodes", func() {
			_, err := reconcileAs(k8sClient, name)
			Expect(err).NotTo(HaveOccurred())

			zc := &checksv1alpha1.ZoneCheck{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, zc)).To(Succeed())
			Expect(zc.Status.Results).To(HaveLen(2))

			z1 := zc.Status.Results[0]
			Expect(z1.Target).To(Equal("z1"))
			Expect(z1.LostNodes).To(Equal(int32(1)))
			Expect(z1.DisplacedPods).To(Equal(int32(4)))
			Expect(z1.UnschedulablePods).To(Equal(int32(2)))
			Expect(z1.DrainBlockedPods).To(Equal(int32(1)))
			Expect(z1.OutageStuckPods).To(Equal(int32(1)))
			Expect(z1.LostPods).To(Equal(int32(1)))
			Expect(z1.Pods).To(ConsistOf(
				checksv1alpha1.AffectedPod{Namespace: "default", Name: "big-1", Owner: "ReplicaSet/big-1",
					Issue:  checksv1alpha1.PodUnschedulable,
					Reason: "0/3 nodes are available: 1 node(s) were unschedulable, 2 Insufficient cpu."},
				checksv1alpha1.AffectedPod{Namespace: "default", Name: "db-0", Owner: "StatefulSet/db-0",
					Issue:  checksv1alpha1.PodUnschedulable,
					Reason: "0/3 nodes are available: 1 node(s) were unschedulable, 2 node(s) didn't match PersistentVolume's node affinity."},
				checksv1alpha1.AffectedPod{Namespace: "default", Name: "web-1", Owner: "ReplicaSet/web-1",
					Issue: checksv1alpha1.PodDrainBlocked, Reason: "PodDisruptionBudget default/web allows no disruptions"},
				checksv1alpha1.AffectedPod{Namespace: "default", Name: "scratch", Issue: checksv1alpha1.PodLost,
					Reason: "no controller to recreate it"},
				checksv1alpha1.AffectedPod{Namespace: "default", Name: "db-0", Owner: "StatefulSet/db-0",
					Issue: checksv1alpha1.PodOutageStuck, Reason: "StatefulSet pods wait for the node or pod to be deleted"},
			))
			Expect(testutil.CollectAndCompare(results, strings.NewReader(`
# HELP zonecheck_affected_pods Pods the simulated failure causes trouble for, by issue.
# TYPE zonecheck_affected_pods gauge
zonecheck_affected_pods{issue="DrainBlocked",scenario="Zone",target="z1",zonecheck="results"} 1
zonecheck_affected_pods{issue="Lost",scenario="Zone",target="z1",zonecheck="results"} 1
zonecheck_affected_pods{issue="OutageStuck",scenario="Zone",target="z1",zonecheck="results"} 1
zonecheck_affected_pods{issue="Unschedulable",scenario="Zone",target="z1",zonecheck="results"} 2
zonecheck_affected_pods{issue="DrainBlocked",scenario="Zone",target="z2",zonecheck="results"} 0
zonecheck_affected_pods{issue="Lost",scenario="Zone",target="z2",zonecheck="results"} 0
zonecheck_affected_pods{issue="OutageStuck",scenario="Zone",target="z2",zonecheck="results"} 0
zonecheck_affected_pods{issue="Unschedulable",scenario="Zone",target="z2",zonecheck="results"} 0
`), "zonecheck_affected_pods")).To(Succeed())

			Expect(zc.Status.Results[1].Target).To(Equal("z2"))
			Expect(zc.Status.Results[1].DisplacedPods).To(BeZero())
			Expect(zc.Status.Results[1].UnschedulablePods).To(BeZero())

			zoned := meta.FindStatusCondition(zc.Status.Conditions, checksv1alpha1.ConditionZoneLabelsComplete)
			Expect(zoned).NotTo(BeNil())
			Expect(zoned.Status).To(Equal(metav1.ConditionFalse))
			Expect(zoned.Message).To(ContainSubstring("n3"))
		})

		It("runs with only the permissions in config/rbac/role.yaml", func() {
			raw, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
			Expect(err).NotTo(HaveOccurred())
			role := &rbacv1.ClusterRole{}
			Expect(yaml.Unmarshal(raw, role)).To(Succeed())
			role.Name = "zonecheck-test-manager"
			binding := &rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: role.Name},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
				Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "zonecheck-manager"}},
			}
			Expect(k8sClient.Create(ctx, role)).To(Succeed())
			Expect(k8sClient.Create(ctx, binding)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Delete(ctx, binding)).To(Succeed())
				Expect(k8sClient.Delete(ctx, role)).To(Succeed())
			})

			as := func(user string) client.Client {
				c := rest.CopyConfig(cfg)
				c.Impersonate = rest.ImpersonationConfig{UserName: user}
				cl, err := client.New(c, client.Options{Scheme: k8sClient.Scheme()})
				Expect(err).NotTo(HaveOccurred())
				return cl
			}

			By("failing as a user with no roles, so the check means something")
			_, err = reconcileAs(as("nobody"), name)
			Expect(apierrors.IsForbidden(err)).To(BeTrue(), "got %v", err)

			_, err = reconcileAs(as("zonecheck-manager"), name)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Validation", func() {
		createRaw := func(spec map[string]any) error {
			u := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "checks.zonecheck.dev/v1alpha1",
				"kind":       "ZoneCheck",
				"metadata":   map[string]any{"name": "validation"},
				"spec":       spec,
			}}
			err := k8sClient.Create(ctx, u)
			if err == nil {
				Expect(k8sClient.Delete(ctx, u)).To(Succeed())
			}
			return err
		}
		zoneOnly := []any{map[string]any{"type": "Zone"}}

		It("requires scenarios", func() {
			err := createRaw(map[string]any{"interval": "5m"})
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
			Expect(err).To(MatchError(ContainSubstring("spec.scenarios")))
			Expect(createRaw(map[string]any{"scenarios": []any{}})).To(MatchError(ContainSubstring("spec.scenarios")))
		})

		DescribeTable("checks the interval",
			func(interval string, ok bool) {
				err := createRaw(map[string]any{"interval": interval, "scenarios": zoneOnly})
				if !ok {
					Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
					Expect(err).To(MatchError(ContainSubstring("interval")))
					return
				}
				Expect(err).NotTo(HaveOccurred())
				// whatever the API server takes, the Go client has to parse
				_, err = time.ParseDuration(interval)
				Expect(err).NotTo(HaveOccurred())
			},
			Entry("1m is the minimum", "1m", true),
			Entry("compound", "1h30m", true),
			Entry("fractional", "1.5h", true),
			Entry("seconds", "90s", true),
			Entry("zero", "0s", false),
			Entry("negative", "-5m", false),
			Entry("too short", "30s", false),
			Entry("microseconds", "1µs", false),
			Entry("not a duration", "banana", false),
			Entry("days aren't a Go duration", "1d", false),
			Entry("units are case sensitive", "5M", false),
		)

		DescribeTable("checks scenarios",
			func(scenario map[string]any, wantErr string) {
				err := createRaw(map[string]any{"scenarios": []any{scenario}})
				if wantErr == "" {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(err).To(MatchError(ContainSubstring(wantErr)))
				}
			},
			Entry("valid pool", map[string]any{"type": "NodePool",
				"nodePool": map[string]any{"labelKey": "example.com/pool", "name": "gpu"}}, ""),
			Entry("pool missing", map[string]any{"type": "NodePool"}, "nodePool is required"),
			Entry("pool on another type", map[string]any{"type": "BusiestNode",
				"nodePool": map[string]any{"labelKey": "pool", "name": "a"}}, "nodePool is required"),
			Entry("zoneLabel on another type", map[string]any{"type": "BusiestNode", "zoneLabel": "zone"},
				"zoneLabel is only allowed"),
			Entry("bad zone label", map[string]any{"type": "Zone", "zoneLabel": "bad key!"}, "valid label key"),
			Entry("bad pool key", map[string]any{"type": "NodePool",
				"nodePool": map[string]any{"labelKey": "a/b/c", "name": "gpu"}}, "valid label key"),
			Entry("bad pool name", map[string]any{"type": "NodePool",
				"nodePool": map[string]any{"labelKey": "pool", "name": "has space"}}, "valid label value"),
		)

		DescribeTable("checks nodeEviction",
			func(eviction map[string]any, ok bool) {
				err := createRaw(map[string]any{"scenarios": zoneOnly, "nodeEviction": eviction})
				if ok {
					Expect(err).NotTo(HaveOccurred())
				} else {
					Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
				}
			},
			Entry("defaults", map[string]any{}, true),
			Entry("custom", map[string]any{"unhealthyZoneThresholdPercent": 70, "largeClusterSizeThreshold": 100000}, true),
			Entry("0 percent", map[string]any{"unhealthyZoneThresholdPercent": 0}, false),
			Entry("over 100 percent", map[string]any{"unhealthyZoneThresholdPercent": 101}, false),
			Entry("negative size", map[string]any{"largeClusterSizeThreshold": -1}, false),
		)
	})
})
