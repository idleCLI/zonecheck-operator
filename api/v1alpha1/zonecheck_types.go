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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ScenarioType is the kind of failure to simulate.
// +kubebuilder:validation:Enum=Zone;BusiestNode;NodePool
type ScenarioType string

const (
	// ScenarioZone removes every node in one zone, once per zone.
	ScenarioZone ScenarioType = "Zone"
	// ScenarioBusiestNode removes the node whose movable pods would need the
	// most room elsewhere, measured as a share of the cluster's capacity.
	ScenarioBusiestNode ScenarioType = "BusiestNode"
	// ScenarioNodePool removes every node in a named node pool.
	ScenarioNodePool ScenarioType = "NodePool"
)

// ZoneCheckSpec defines which failures to simulate and how often.
type ZoneCheckSpec struct {
	// interval between simulation runs. Minimum 1m.
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1m')",message="interval must be a duration of at least 1m, like 5m or 1h30m"
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`

	// scenarios to simulate on every run.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +required
	Scenarios []Scenario `json:"scenarios"`

	// nodeEviction should match your control plane's eviction settings.
	// They decide whether an outage evicts pods at all.
	// +optional
	NodeEviction *NodeEviction `json:"nodeEviction,omitempty"`
}

// Scenario describes one kind of failure.
// +kubebuilder:validation:XValidation:rule="self.type == 'NodePool' ? has(self.nodePool) : !has(self.nodePool)",message="nodePool is required for type NodePool and not allowed otherwise"
// +kubebuilder:validation:XValidation:rule="self.type == 'Zone' || !has(self.zoneLabel)",message="zoneLabel is only allowed for type Zone"
type Scenario struct {
	// type of failure to simulate.
	// +required
	Type ScenarioType `json:"type"`

	// zoneLabel is the node label that holds the zone. Each distinct value
	// is simulated as its own failure. Defaults to topology.kubernetes.io/zone.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=317
	// +kubebuilder:validation:XValidation:rule="!format.qualifiedName().validate(self).hasValue()",message="zoneLabel must be a valid label key"
	// +optional
	ZoneLabel string `json:"zoneLabel,omitempty"`

	// nodePool selects the pool to remove for type NodePool.
	// +optional
	NodePool *NodePoolRef `json:"nodePool,omitempty"`
}

// topology.kubernetes.io/zone if unset. Not a CRD default -> it'd land on every scenario type
func (s Scenario) ZoneLabelOrDefault() string {
	if s.ZoneLabel == "" {
		return corev1.LabelTopologyZone
	}
	return s.ZoneLabel
}

// NodePoolRef identifies a node pool by a label key and value, since every
// provider labels pools differently.
type NodePoolRef struct {
	// labelKey is the node label that names the pool.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=317
	// +kubebuilder:validation:XValidation:rule="!format.qualifiedName().validate(self).hasValue()",message="labelKey must be a valid label key"
	// +required
	LabelKey string `json:"labelKey"`

	// name is the label value for the pool.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.labelValue().validate(self).hasValue()",message="name must be a valid label value"
	// +required
	Name string `json:"name"`
}

// NodeEviction mirrors kube-controller-manager's eviction flags. When enough
// of a zone goes down at once, the node controller slows or stops evicting
// pods, so they stay on the dead nodes.
type NodeEviction struct {
	// unhealthyZoneThresholdPercent is --unhealthy-zone-threshold as a
	// percent. Defaults to 55.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	UnhealthyZoneThresholdPercent *int32 `json:"unhealthyZoneThresholdPercent,omitempty"`

	// largeClusterSizeThreshold is --large-cluster-size-threshold. Zones this
	// size or smaller stop evicting entirely during a partial outage.
	// Defaults to 50.
	// +kubebuilder:validation:Minimum=0
	// +optional
	LargeClusterSizeThreshold *int32 `json:"largeClusterSizeThreshold,omitempty"`
}

// --unhealthy-zone-threshold, defaulted
func (e *NodeEviction) ThresholdPercent() int32 {
	if e == nil || e.UnhealthyZoneThresholdPercent == nil {
		return 55
	}
	return *e.UnhealthyZoneThresholdPercent
}

// --large-cluster-size-threshold, defaulted
func (e *NodeEviction) LargeClusterSize() int32 {
	if e == nil || e.LargeClusterSizeThreshold == nil {
		return 50
	}
	return *e.LargeClusterSizeThreshold
}

// status conditions
const (
	// True once a run finishes
	ConditionReady = "Ready"
	// False if nodes lack a zone label (Zone failures count them as surviving capacity)
	ConditionZoneLabelsComplete = "ZoneLabelsComplete"
)

// ZoneCheckStatus holds the results of the most recent run.
type ZoneCheckStatus struct {
	// observedGeneration is the spec generation the last run used.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// lastRunTime is when the last simulation finished.
	// +optional
	LastRunTime *metav1.Time `json:"lastRunTime,omitempty"`

	// results has one entry per simulated failure. A Zone scenario in a
	// cluster with three zones produces three entries.
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	// +optional
	Results []ScenarioResult `json:"results,omitempty"`

	// conditions represent the current state of the ZoneCheck.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ScenarioResult is the outcome of simulating one failure.
type ScenarioResult struct {
	// type of failure that was simulated.
	// +required
	Type ScenarioType `json:"type"`

	// target is what was removed: the zone, node, or pool name.
	// +kubebuilder:validation:MaxLength=253
	// +required
	Target string `json:"target"`

	// lostNodes is how many nodes the failure removed.
	// +required
	LostNodes int32 `json:"lostNodes"`

	// displacedPods is how many pods a controller will recreate elsewhere
	// after a drain. Excludes DaemonSet and static pods.
	// +required
	DisplacedPods int32 `json:"displacedPods"`

	// unschedulablePods is how many displaced pods wouldn't fit on the
	// remaining nodes after a drain. See PodIssue.
	// +required
	UnschedulablePods int32 `json:"unschedulablePods"`

	// drainBlockedPods is how many displaced pods a drain couldn't evict
	// because of a PodDisruptionBudget. See PodIssue.
	// +required
	DrainBlockedPods int32 `json:"drainBlockedPods"`

	// outageStuckPods is how many displaced pods would stay on the dead
	// nodes in an outage. See PodIssue.
	// +required
	OutageStuckPods int32 `json:"outageStuckPods"`

	// lostPods is how many pods nothing will recreate. See PodIssue.
	// +required
	LostPods int32 `json:"lostPods"`

	// pods lists the unschedulable, drain-blocked, lost, and stuck pods, up to 25. The
	// counts above are always complete. A pod can appear once per issue.
	// +kubebuilder:validation:MaxItems=25
	// +listType=atomic
	// +optional
	Pods []AffectedPod `json:"pods,omitempty"`

	// omittedPods is how many affected pods didn't fit in pods.
	// +optional
	OmittedPods int32 `json:"omittedPods,omitempty"`
}

// PodIssue is what goes wrong for a pod when its node is lost.
//
// Unschedulable: after a drain, no remaining node has room for it. The
// reason is kube-scheduler's own message.
//
// DrainBlocked: a drain can't evict it because a PodDisruptionBudget won't
// allow it, so the drain stalls.
//
// OutageStuck: a drain would move it, but an outage leaves it on the dead
// node until someone deletes the node or the pod.
//
// Lost: nothing recreates it, in a drain or an outage.
// +kubebuilder:validation:Enum=Unschedulable;DrainBlocked;OutageStuck;Lost
type PodIssue string

const (
	PodUnschedulable PodIssue = "Unschedulable"
	PodDrainBlocked  PodIssue = "DrainBlocked"
	PodOutageStuck   PodIssue = "OutageStuck"
	PodLost          PodIssue = "Lost"
)

// AffectedPod is a pod the failure causes trouble for.
type AffectedPod struct {
	// +kubebuilder:validation:MaxLength=63
	// +required
	Namespace string `json:"namespace"`

	// +kubebuilder:validation:MaxLength=253
	// +required
	Name string `json:"name"`

	// owner is the controlling workload, like "StatefulSet/db".
	// +kubebuilder:validation:MaxLength=317
	// +optional
	Owner string `json:"owner,omitempty"`

	// +required
	Issue PodIssue `json:"issue"`

	// reason explains the issue.
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Reason string `json:"reason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Interval",type=string,JSONPath=`.spec.interval`
// +kubebuilder:printcolumn:name="Last Run",type=date,JSONPath=`.status.lastRunTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ZoneCheck continuously simulates node, zone, or pool failures and
// reports which workloads couldn't be rescheduled.
type ZoneCheck struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ZoneCheckSpec `json:"spec"`

	// +optional
	Status ZoneCheckStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ZoneCheckList contains a list of ZoneCheck.
type ZoneCheckList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ZoneCheck `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ZoneCheck{}, &ZoneCheckList{})
		return nil
	})
}
