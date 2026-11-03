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

package snapshot

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func pdb(name string, selector map[string]string, allowed int32) policyv1.PodDisruptionBudget {
	p := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Spec:       policyv1.PodDisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1))},
		Status:     policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: allowed},
	}
	if selector != nil {
		p.Spec.Selector = &metav1.LabelSelector{MatchLabels: selector}
	}
	return p
}

func labeled(name, app string) corev1.Pod {
	p := rs(name, "")
	p.UID = types.UID(name)
	p.Labels = map[string]string{"app": app}
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return p
}

// pod name -> reason, for the blocked ones
func blockedNames(s *Snapshot, pods []corev1.Pod, fits ...string) map[string]string {
	fit := map[types.UID]bool{}
	for _, f := range fits {
		fit[types.UID(f)] = true
	}
	out := map[string]string{}
	for uid, reason := range s.DrainBlocked(pods, fit) {
		out[string(uid)] = reason
	}
	return out
}

func TestDrainBlocked(t *testing.T) {
	alwaysAllow := pdb("sloppy", map[string]string{"app": "sloppy"}, 0)
	alwaysAllow.Spec.UnhealthyPodEvictionPolicy = new(policyv1.AlwaysAllow)
	notReady := labeled("sloppy-1", "sloppy")
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse

	s := &Snapshot{PDBs: []policyv1.PodDisruptionBudget{
		pdb("strict", map[string]string{"app": "db"}, 0),
		pdb("loose", map[string]string{"app": "web"}, 1),
		pdb("overlap-1", map[string]string{"app": "cache"}, 5),
		pdb("overlap-2", map[string]string{"app": "cache"}, 5),
		pdb("matches-nothing", nil, 0),
		alwaysAllow,
	}}
	pods := []corev1.Pod{
		labeled("db-1", "db"), labeled("db-2", "db"),
		labeled("web-1", "web"), labeled("web-2", "web"), labeled("web-3", "web"),
		labeled("cache-1", "cache"),
		labeled("free-1", "nothing-covers-me"),
		notReady,
	}

	got := blockedNames(s, pods, "db-1", "web-1")
	want := map[string]string{
		// no budget at all: even pods that fit stay put
		"db-1": "PodDisruptionBudget default/strict allows no disruptions",
		"db-2": "PodDisruptionBudget default/strict allows no disruptions",
		// budget of 1: web-1 fits, web-2 uses the budget up, web-3 is stuck
		"web-3":   "PodDisruptionBudget default/loose runs out: too many of its pods have nowhere to go",
		"cache-1": "more than one PodDisruptionBudget covers it, which the Eviction API refuses",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("blocked =\n%v\nwant\n%v", got, want)
	}
}

func TestDrainBlockedNoPDBs(t *testing.T) {
	if got := blockedNames(&Snapshot{}, []corev1.Pod{labeled("a", "web")}); len(got) != 0 {
		t.Errorf("blocked = %v", got)
	}
}

func TestDrainBlockedEvictionAPIRules(t *testing.T) {
	healthyApp := pdb("healthy", map[string]string{"app": "healthy"}, 0)
	healthyApp.Status.CurrentHealthy, healthyApp.Status.DesiredHealthy = 3, 3
	degradedApp := pdb("degraded", map[string]string{"app": "degraded"}, 0)
	degradedApp.Status.CurrentHealthy, degradedApp.Status.DesiredHealthy = 1, 0
	stale := pdb("stale", map[string]string{"app": "stale"}, 5)
	stale.Generation, stale.Status.ObservedGeneration = 2, 1

	unready := func(name, app string) corev1.Pod {
		p := labeled(name, app)
		p.Status.Conditions[0].Status = corev1.ConditionFalse
		return p
	}
	pending := labeled("starting", "healthy")
	pending.Status.Phase = corev1.PodPending

	s := &Snapshot{PDBs: []policyv1.PodDisruptionBudget{healthyApp, degradedApp, stale}}
	got := blockedNames(s, []corev1.Pod{
		pending,
		unready("healthy-unready", "healthy"),
		labeled("healthy-ready", "healthy"),
		unready("degraded-unready", "degraded"),
		labeled("stale-1", "stale"),
	}, "healthy-ready", "stale-1")

	want := map[string]string{
		// IfHealthyBudget: an unready pod goes only while the rest of the app is healthy
		"healthy-ready":    "PodDisruptionBudget default/healthy allows no disruptions",
		"degraded-unready": "PodDisruptionBudget default/degraded allows no disruptions",
		"stale-1":          "PodDisruptionBudget default/stale is still being processed",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("blocked =\n%v\nwant\n%v", got, want)
	}
}
