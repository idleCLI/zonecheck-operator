//go:build e2e

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

package e2e

import (
	"cmp"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
	"github.com/idleCLI/zonecheck-operator/test/utils"
)

const (
	testNS   = "zonecheck-e2e"
	lostZone = "zone-a"
	// made-up resource, 2 per node -> capacity doesn't depend on the machine running this
	slot  = corev1.ResourceName("zonecheck.dev/slot")
	image = "registry.k8s.io/pause:3.10"
)

// one workload per issue zonecheck reports. Predict losing zone-a, drain it for real, compare
var _ = Describe("Zone drain", Ordered, func() {
	ctx := context.Background()
	var (
		c     client.Client
		zones map[string][]string
	)

	BeforeAll(func() {
		cfg, err := config.GetConfig()
		Expect(err).NotTo(HaveOccurred())
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
		Expect(checksv1alpha1.AddToScheme(scheme)).To(Succeed())
		c, err = client.New(cfg, client.Options{Scheme: scheme})
		Expect(err).NotTo(HaveOccurred())

		var nodes corev1.NodeList
		Expect(c.List(ctx, &nodes)).To(Succeed())
		zones = map[string][]string{}
		for _, n := range nodes.Items {
			if z := n.Labels[corev1.LabelTopologyZone]; z != "" {
				zones[z] = append(zones[z], n.Name)
			}
		}
		Expect(zones).To(HaveLen(3), "needs 3 zones, see test/e2e/kind-config.yaml")
		for _, names := range zones {
			Expect(names).To(HaveLen(2), "needs 2 nodes per zone, see test/e2e/kind-config.yaml")
		}

		By("giving every zoned node 2 slots")
		for _, names := range zones {
			for _, name := range names {
				Expect(c.Status().Patch(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}},
					client.RawPatch(types.MergePatchType,
						[]byte(`{"status":{"capacity":{"zonecheck.dev/slot":"2"},"allocatable":{"zonecheck.dev/slot":"2"}}}`)),
				)).To(Succeed())
				Eventually(func(g Gomega) {
					n := &corev1.Node{}
					g.Expect(c.Get(ctx, client.ObjectKey{Name: name}, n)).To(Succeed())
					g.Expect(n.Status.Allocatable[slot]).To(Equal(resource.MustParse("2")))
				}).Should(Succeed())
			}
		}

		Expect(c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}})).To(Succeed())
	})

	AfterAll(func() {
		for _, names := range zones {
			_, _ = utils.Run(exec.Command("kubectl", append([]string{"uncordon"}, names...)...))
		}
		_ = c.Delete(ctx, &checksv1alpha1.ZoneCheck{ObjectMeta: metav1.ObjectMeta{Name: "e2e"}})
		_ = c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}})
		_ = c.Delete(ctx, &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "zonecheck-e2e-data"}})
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			for _, args := range [][]string{
				{"get", "pods", "-n", testNS, "-o", "wide"},
				{"get", "zonecheck", "e2e", "-o", "yaml"},
				{"get", "events", "-n", testNS, "--sort-by=.lastTimestamp"},
			} {
				out, _ := utils.Run(exec.Command("kubectl", args...))
				_, _ = fmt.Fprintln(GinkgoWriter, out)
			}
		}
	})

	It("predicts what a real drain does", func() {
		setCordon := func(cordon bool, names ...string) {
			for _, name := range names {
				Expect(c.Patch(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}, client.RawPatch(
					types.MergePatchType, fmt.Appendf(nil, `{"spec":{"unschedulable":%t}}`, cordon)))).To(Succeed())
			}
		}
		var others []string
		for z, names := range zones {
			if z != lostZone {
				others = append(others, names...)
			}
		}

		By("putting api on zone-a (other zones cordoned while it starts)")
		setCordon(true, others...)
		Expect(c.Create(ctx, deployment("api", 2, nil))).To(Succeed())
		waitAvailable(ctx, c, "api")
		setCordon(false, others...)
		Expect(c.Create(ctx, &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "api"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: new(intstr.FromInt32(0)),
				Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}},
			},
		})).To(Succeed())

		By("creating the rest")
		scratch := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "scratch"},
			Spec:       podSpec(),
		}
		scratch.Spec.NodeName = zones[lostZone][0]
		Expect(c.Create(ctx, scratch)).To(Succeed())

		Expect(c.Create(ctx, &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "zonecheck-e2e-data"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				ClaimRef:    &corev1.ObjectReference{Namespace: testNS, Name: "data"},
				PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/zonecheck-e2e", Type: new(corev1.HostPathDirectoryOrCreate),
				}},
				NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{lostZone},
						}},
					}},
				}},
			},
		})).To(Succeed())
		Expect(c.Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
				VolumeName:       "zonecheck-e2e-data",
				StorageClassName: new(""),
			},
		})).To(Succeed())
		Expect(c.Create(ctx, deployment("data", 1, func(s *corev1.PodSpec) {
			s.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
			}}}
		}))).To(Succeed())

		spreadByZone := func(policy *corev1.NodeInclusionPolicy) func(*corev1.PodSpec) {
			return func(s *corev1.PodSpec) {
				s.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
					MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule,
					NodeTaintsPolicy: policy,
				}}
			}
		}
		// default nodeTaintsPolicy -> stuck after the drain, Honor -> fine
		Expect(c.Create(ctx, deployment("spread", 3, spreadByZone(nil)))).To(Succeed())
		Expect(c.Create(ctx, deployment("spread-honor", 3, spreadByZone(new(corev1.NodeInclusionPolicyHonor))))).To(Succeed())
		// 9 pods on 12 slots. zone-a's 4 go -> 1 pod has no room
		Expect(c.Create(ctx, deployment("web", 9, func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests = corev1.ResourceList{slot: resource.MustParse("1")}
			s.Containers[0].Resources.Limits = corev1.ResourceList{slot: resource.MustParse("1")}
		}))).To(Succeed())

		for _, name := range []string{"data", "spread", "spread-honor", "web"} {
			waitAvailable(ctx, c, name)
		}
		Eventually(func(g Gomega) {
			p := &corev1.Pod{}
			g.Expect(c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "scratch"}, p)).To(Succeed())
			g.Expect(p.Status.Phase).To(Equal(corev1.PodRunning))
		}).Should(Succeed())
		By("waiting for the disruption controller to process the PDB")
		Eventually(func(g Gomega) {
			pdb := &policyv1.PodDisruptionBudget{}
			g.Expect(c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "api"}, pdb)).To(Succeed())
			g.Expect(pdb.Status.ObservedGeneration).To(Equal(pdb.Generation))
			g.Expect(pdb.Status.CurrentHealthy).To(Equal(int32(2)))
		}).Should(Succeed())

		By("asking zonecheck")
		Expect(c.Create(ctx, &checksv1alpha1.ZoneCheck{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e"},
			Spec: checksv1alpha1.ZoneCheckSpec{
				Interval:  &metav1.Duration{Duration: time.Hour},
				Scenarios: []checksv1alpha1.Scenario{{Type: checksv1alpha1.ScenarioZone}},
			},
		})).To(Succeed())
		var predicted map[string]int
		Eventually(func(g Gomega) {
			zc := &checksv1alpha1.ZoneCheck{}
			g.Expect(c.Get(ctx, client.ObjectKey{Name: "e2e"}, zc)).To(Succeed())
			g.Expect(meta.IsStatusConditionTrue(zc.Status.Conditions, checksv1alpha1.ConditionReady)).To(BeTrue())
			i := slices.IndexFunc(zc.Status.Results, func(r checksv1alpha1.ScenarioResult) bool { return r.Target == lostZone })
			g.Expect(i).NotTo(Equal(-1))
			res := zc.Status.Results[i]
			g.Expect(res.OmittedPods).To(BeZero())
			predicted = map[string]int{}
			for _, p := range res.Pods {
				if p.Namespace == testNS {
					predicted[string(p.Issue)+" "+cmp.Or(p.Owner, "Pod/"+p.Name)]++
				}
			}
		}, 3*time.Minute).Should(Succeed())
		_, _ = fmt.Fprintf(GinkgoWriter, "predicted: %v\n", predicted)

		// empty prediction -> fail, else a broken setup could pass
		Expect(predicted).To(Equal(map[string]int{
			"Unschedulable Deployment/web":    1,
			"Unschedulable Deployment/data":   1,
			"Unschedulable Deployment/spread": 1,
			"DrainBlocked Deployment/api":     2,
			"Lost Pod/scratch":                1,
		}))

		By("draining zone-a for real")
		out, err := utils.Run(exec.Command("kubectl", append([]string{"drain",
			"--ignore-daemonsets", "--delete-emptydir-data", "--force", "--timeout=60s"}, zones[lostZone]...)...))
		// api's PDB holds it up till the timeout
		Expect(err).To(HaveOccurred(), "drain should stall on api's PDB:\n%s", out)

		lost := map[string]bool{}
		for _, n := range zones[lostZone] {
			lost[n] = true
		}
		var actual map[string]int
		Eventually(func(g Gomega) {
			var deploys appsv1.DeploymentList
			g.Expect(c.List(ctx, &deploys, client.InNamespace(testNS))).To(Succeed())
			for _, d := range deploys.Items {
				g.Expect(d.Status.Replicas).To(Equal(*d.Spec.Replicas), "%s still replacing pods", d.Name)
			}
			var pods corev1.PodList
			g.Expect(c.List(ctx, &pods, client.InNamespace(testNS))).To(Succeed())
			actual = map[string]int{}
			for _, p := range pods.Items {
				g.Expect(p.DeletionTimestamp).To(BeNil(), "%s still going", p.Name)
				owner := "Pod/" + p.Name
				if app := p.Labels["app"]; app != "" {
					owner = "Deployment/" + app
				}
				switch {
				case p.Spec.NodeName == "":
					i := slices.IndexFunc(p.Status.Conditions, func(c corev1.PodCondition) bool {
						return c.Type == corev1.PodScheduled
					})
					g.Expect(i).NotTo(Equal(-1), "%s not tried by the scheduler yet", p.Name)
					g.Expect(p.Status.Conditions[i].Reason).To(Equal(corev1.PodReasonUnschedulable))
					actual["Unschedulable "+owner]++
				case lost[p.Spec.NodeName]:
					actual["DrainBlocked "+owner]++
				}
			}
			if !slices.ContainsFunc(pods.Items, func(p corev1.Pod) bool { return p.Name == "scratch" }) {
				actual["Lost Pod/scratch"]++
			}
		}, 3*time.Minute).Should(Succeed())

		Expect(actual).To(Equal(predicted))
	})
})

func podSpec() corev1.PodSpec {
	return corev1.PodSpec{
		TerminationGracePeriodSeconds: new(int64(0)),
		Containers:                    []corev1.Container{{Name: "pause", Image: image}},
	}
}

func deployment(name string, replicas int32, mutate func(*corev1.PodSpec)) *appsv1.Deployment {
	labels := map[string]string{"app": name}
	spec := podSpec()
	if mutate != nil {
		mutate(&spec)
	}
	for i := range spec.TopologySpreadConstraints {
		spec.TopologySpreadConstraints[i].LabelSelector = &metav1.LabelSelector{MatchLabels: labels}
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: spec},
		},
	}
}

func waitAvailable(ctx context.Context, c client.Client, name string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		d := &appsv1.Deployment{}
		g.Expect(c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, d)).To(Succeed())
		g.Expect(d.Status.AvailableReplicas).To(Equal(*d.Spec.Replicas))
	}, 3*time.Minute).Should(Succeed(), "waiting for %s", name)
}
