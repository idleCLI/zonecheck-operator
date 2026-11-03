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
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
	"github.com/idleCLI/zonecheck-operator/internal/metrics"
	"github.com/idleCLI/zonecheck-operator/internal/placement"
	"github.com/idleCLI/zonecheck-operator/internal/snapshot"
)

// keep in sync w/ the MaxItems/MaxLength markers in the API
const (
	maxResults    = 64
	maxListedPods = 25
	maxReasonLen  = 512
)

type ZoneCheckReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Metrics *metrics.Collector
}

// +kubebuilder:rbac:groups=checks.zonecheck.dev,resources=zonechecks,verbs=get;list;watch
// +kubebuilder:rbac:groups=checks.zonecheck.dev,resources=zonechecks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes;pods;persistentvolumeclaims;persistentvolumes;namespaces;services;replicationcontrollers,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses;csinodes;csidrivers;csistoragecapacities;volumeattachments,verbs=get;list;watch

func (r *ZoneCheckReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var zc checksv1alpha1.ZoneCheck
	if err := r.Get(ctx, req.NamespacedName, &zc); err != nil {
		if apierrors.IsNotFound(err) {
			r.Metrics.Forget(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// the CRD defaults it, but objects built in Go don't get that
	interval := 5 * time.Minute
	if zc.Spec.Interval != nil {
		interval = zc.Spec.Interval.Duration
	}

	// don't run early on a resync. After a restart the metrics are empty -> run now
	if zc.Status.ObservedGeneration == zc.Generation && zc.Status.LastRunTime != nil && r.Metrics.Has(zc.Name) {
		if wait := time.Until(zc.Status.LastRunTime.Add(interval)); wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
	}

	start := time.Now()
	base := zc.DeepCopy()
	fail := func(reason string, err error) (ctrl.Result, error) {
		r.Metrics.Failed(zc.Name, time.Since(start))
		meta.SetStatusCondition(&zc.Status.Conditions, metav1.Condition{
			Type: checksv1alpha1.ConditionReady, Status: metav1.ConditionFalse,
			Reason: reason, Message: err.Error(), ObservedGeneration: zc.Generation,
		})
		if perr := r.Status().Patch(ctx, &zc, client.MergeFrom(base)); perr != nil {
			log.Error(perr, "Failed to record failed run", "reason", reason)
		}
		return ctrl.Result{}, err
	}

	snap, err := snapshot.Take(ctx, r.Client)
	if err != nil {
		return fail("SnapshotFailed", err)
	}
	sched, err := placement.New(ctx, snap.Objects)
	if err != nil {
		return fail("SimulationFailed", err)
	}
	defer func() {
		if err := sched.Close(); err != nil {
			log.Error(err, "Failed to close scheduler")
		}
	}()
	results, scenarios, err := simulate(ctx, sched, snap, &zc.Spec)
	if err != nil {
		return fail("SimulationFailed", err)
	}
	now := time.Now()
	r.Metrics.Succeeded(zc.Name, now, now.Sub(start), scenarios)
	zc.Status.Results = results
	if len(zc.Status.Results) > maxResults {
		log.Info("Dropped results past the status limit", "results", len(zc.Status.Results), "limit", maxResults)
		zc.Status.Results = zc.Status.Results[:maxResults]
	}
	setZoneLabelCondition(&zc, snap)
	meta.SetStatusCondition(&zc.Status.Conditions, metav1.Condition{
		Type: checksv1alpha1.ConditionReady, Status: metav1.ConditionTrue,
		Reason: "RunSucceeded", ObservedGeneration: zc.Generation,
	})
	zc.Status.ObservedGeneration = zc.Generation
	zc.Status.LastRunTime = &metav1.Time{Time: now}
	if err := r.Status().Patch(ctx, &zc, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("Finished run", "nodes", len(snap.Nodes), "pods", len(snap.Pods), "results", len(zc.Status.Results))
	return ctrl.Result{RequeueAfter: interval}, nil
}

func simulate(ctx context.Context, sched *placement.Scheduler, snap *snapshot.Snapshot, spec *checksv1alpha1.ZoneCheckSpec) ([]checksv1alpha1.ScenarioResult, []metrics.Scenario, error) {
	var (
		results   []checksv1alpha1.ScenarioResult
		scenarios []metrics.Scenario
	)
	for _, sc := range spec.Scenarios {
		for _, f := range snap.Failures(sc) {
			drained, loss := snap.Drain(f.Nodes, spec.NodeEviction)

			displaced := make([]corev1.Pod, len(loss.Displaced))
			for i, d := range loss.Displaced {
				displaced[i] = d.Pod
			}
			placed, err := sched.Place(ctx, drained.Nodes, slices.Concat(drained.Pods, drained.Terminating), displaced)
			if err != nil {
				return nil, nil, fmt.Errorf("%s %s: %w", f.Type, f.Target, err)
			}
			fits := make(map[types.UID]bool, len(placed))
			for _, p := range placed {
				fits[p.Pod.UID] = p.Node != ""
			}
			blocked := snap.DrainBlocked(displaced, fits)

			res := checksv1alpha1.ScenarioResult{
				Type:          f.Type,
				Target:        f.Target,
				LostNodes:     int32(len(f.Nodes)),
				DisplacedPods: int32(len(loss.Displaced)),
				LostPods:      int32(len(loss.Lost)),
			}
			m := metrics.Scenario{
				Type: f.Type, Target: f.Target, LostNodes: len(f.Nodes), DisplacedPods: len(loss.Displaced),
				Affected: map[metrics.Workload]int{},
			}
			affect := func(p *corev1.Pod, issue checksv1alpha1.PodIssue, reason string) {
				workload := snap.Workload(p)
				addPod(&res, p, workload, issue, reason)
				if workload == "" {
					workload = "Pod/" + p.Name
				}
				m.Affected[metrics.Workload{Namespace: p.Namespace, Name: workload, Issue: issue}]++
			}

			for _, p := range placed {
				if p.Node == "" {
					res.UnschedulablePods++
					affect(p.Pod, checksv1alpha1.PodUnschedulable, p.Reason)
				}
			}
			for i := range displaced {
				if reason, ok := blocked[displaced[i].UID]; ok {
					res.DrainBlockedPods++
					affect(&displaced[i], checksv1alpha1.PodDrainBlocked, reason)
				}
			}
			for _, l := range loss.Lost {
				affect(&l.Pod, checksv1alpha1.PodLost, l.Reason)
			}
			for _, d := range loss.Displaced {
				if d.StuckReason != "" {
					res.OutageStuckPods++
					affect(&d.Pod, checksv1alpha1.PodOutageStuck, d.StuckReason)
				}
			}
			results = append(results, res)
			scenarios = append(scenarios, m)
		}
	}
	return results, scenarios, nil
}

func addPod(res *checksv1alpha1.ScenarioResult, p *corev1.Pod, workload string, issue checksv1alpha1.PodIssue, reason string) {
	if len(res.Pods) >= maxListedPods {
		res.OmittedPods++
		return
	}
	if len(reason) > maxReasonLen {
		reason = reason[:maxReasonLen-3] + "..."
	}
	res.Pods = append(res.Pods, checksv1alpha1.AffectedPod{
		Namespace: p.Namespace, Name: p.Name, Owner: workload, Issue: issue, Reason: reason,
	})
}

func setZoneLabelCondition(zc *checksv1alpha1.ZoneCheck, snap *snapshot.Snapshot) {
	var missing []string
	anyZone := false
	for _, sc := range zc.Spec.Scenarios {
		if sc.Type != checksv1alpha1.ScenarioZone {
			continue
		}
		anyZone = true
		if unzoned := snap.Unzoned(sc.ZoneLabelOrDefault()); len(unzoned) > 0 {
			missing = append(missing, fmt.Sprintf("%d nodes have no %s label (%s)",
				len(unzoned), sc.ZoneLabelOrDefault(), strings.Join(unzoned[:min(len(unzoned), 10)], ", ")))
		}
	}

	switch {
	case !anyZone:
		meta.RemoveStatusCondition(&zc.Status.Conditions, checksv1alpha1.ConditionZoneLabelsComplete)
	case len(missing) > 0:
		meta.SetStatusCondition(&zc.Status.Conditions, metav1.Condition{
			Type: checksv1alpha1.ConditionZoneLabelsComplete, Status: metav1.ConditionFalse,
			Reason: "NodesMissingZoneLabel", Message: strings.Join(missing, "; "), ObservedGeneration: zc.Generation,
		})
	default:
		meta.SetStatusCondition(&zc.Status.Conditions, metav1.Condition{
			Type: checksv1alpha1.ConditionZoneLabelsComplete, Status: metav1.ConditionTrue,
			Reason: "AllNodesZoned", ObservedGeneration: zc.Generation,
		})
	}
}

func (r *ZoneCheckReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// start these w/ the manager -> first run doesn't wait, missing RBAC fails at startup
	for _, obj := range snapshot.Watched {
		if _, err := mgr.GetCache().GetInformer(context.Background(), obj); err != nil {
			return err
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		// skip status-only updates, else every status write kicks off a run
		For(&checksv1alpha1.ZoneCheck{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("zonecheck").
		Complete(r)
}
