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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// pods a drain can't evict, following the Eviction API (pkg/registry/core/pod/storage/eviction.go).
// Best case: one at a time, pods that fit first, so each replacement is Ready
// + hands the budget back before the next eviction -> a lower bound
func (s *Snapshot) DrainBlocked(displaced []corev1.Pod, fits map[types.UID]bool) map[types.UID]string {
	type budget struct {
		pdb  *policyv1.PodDisruptionBudget
		sel  labels.Selector
		pods []*corev1.Pod
	}
	budgets := make([]*budget, 0, len(s.PDBs))
	for i := range s.PDBs {
		sel, err := metav1.LabelSelectorAsSelector(s.PDBs[i].Spec.Selector)
		if err != nil {
			continue
		}
		budgets = append(budgets, &budget{pdb: &s.PDBs[i], sel: sel})
	}

	blocked := map[types.UID]string{}
	for i := range displaced {
		p := &displaced[i]
		// Pending pods skip the PDB check entirely
		if p.Status.Phase == corev1.PodPending {
			continue
		}
		var covering []*budget
		for _, b := range budgets {
			if b.pdb.Namespace == p.Namespace && b.sel.Matches(labels.Set(p.Labels)) {
				covering = append(covering, b)
			}
		}
		switch {
		case len(covering) > 1:
			blocked[p.UID] = "more than one PodDisruptionBudget covers it, which the Eviction API refuses"
		case len(covering) == 1 && !evictableWhileUnready(p, covering[0].pdb):
			covering[0].pods = append(covering[0].pods, p)
		}
	}

	for _, b := range budgets {
		name := b.pdb.Namespace + "/" + b.pdb.Name
		left := b.pdb.Status.DisruptionsAllowed
		for _, p := range b.pods {
			switch {
			case b.pdb.Status.ObservedGeneration < b.pdb.Generation:
				blocked[p.UID] = fmt.Sprintf("PodDisruptionBudget %s is still being processed", name)
			case b.pdb.Status.DisruptionsAllowed <= 0:
				blocked[p.UID] = fmt.Sprintf("PodDisruptionBudget %s allows no disruptions", name)
			case fits[p.UID]:
			case left > 0:
				left--
			default:
				blocked[p.UID] = fmt.Sprintf("PodDisruptionBudget %s runs out: too many of its pods have nowhere to go", name)
			}
		}
	}
	return blocked
}

// unready pods go w/o touching the budget: always under AlwaysAllow, and under
// IfHealthyBudget (the default) while the app is otherwise healthy
func evictableWhileUnready(p *corev1.Pod, pdb *policyv1.PodDisruptionBudget) bool {
	if podReady(p) {
		return false
	}
	if pdb.Spec.UnhealthyPodEvictionPolicy != nil && *pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow {
		return true
	}
	return pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy && pdb.Status.DesiredHealthy > 0
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
