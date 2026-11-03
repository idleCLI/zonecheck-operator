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

package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
)

func zoneA() Scenario {
	return Scenario{
		Type: checksv1alpha1.ScenarioZone, Target: "a", LostNodes: 2, DisplacedPods: 5,
		Affected: map[Workload]int{
			{Namespace: "shop", Name: "Deployment/web", Issue: checksv1alpha1.PodUnschedulable}: 2,
			{Namespace: "shop", Name: "StatefulSet/db", Issue: checksv1alpha1.PodUnschedulable}: 1,
			{Namespace: "shop", Name: "StatefulSet/db", Issue: checksv1alpha1.PodOutageStuck}:   1,
		},
	}
}

func TestCollect(t *testing.T) {
	c := New()
	c.Succeeded("cluster", time.Unix(1700000000, 0), time.Second, []Scenario{
		zoneA(),
		zoneA(), // listed twice in the spec
		{Type: checksv1alpha1.ScenarioZone, Target: "b", LostNodes: 1},
	})

	want := `
# HELP zonecheck_affected_pods Pods the simulated failure causes trouble for, by issue.
# TYPE zonecheck_affected_pods gauge
zonecheck_affected_pods{issue="DrainBlocked",scenario="Zone",target="a",zonecheck="cluster"} 0
zonecheck_affected_pods{issue="Lost",scenario="Zone",target="a",zonecheck="cluster"} 0
zonecheck_affected_pods{issue="OutageStuck",scenario="Zone",target="a",zonecheck="cluster"} 1
zonecheck_affected_pods{issue="Unschedulable",scenario="Zone",target="a",zonecheck="cluster"} 3
zonecheck_affected_pods{issue="DrainBlocked",scenario="Zone",target="b",zonecheck="cluster"} 0
zonecheck_affected_pods{issue="Lost",scenario="Zone",target="b",zonecheck="cluster"} 0
zonecheck_affected_pods{issue="OutageStuck",scenario="Zone",target="b",zonecheck="cluster"} 0
zonecheck_affected_pods{issue="Unschedulable",scenario="Zone",target="b",zonecheck="cluster"} 0
# HELP zonecheck_last_success_timestamp_seconds When the ZoneCheck last finished a run.
# TYPE zonecheck_last_success_timestamp_seconds gauge
zonecheck_last_success_timestamp_seconds{zonecheck="cluster"} 1.7e+09
# HELP zonecheck_lost_nodes Nodes the simulated failure takes away.
# TYPE zonecheck_lost_nodes gauge
zonecheck_lost_nodes{scenario="Zone",target="a",zonecheck="cluster"} 2
zonecheck_lost_nodes{scenario="Zone",target="b",zonecheck="cluster"} 1
# HELP zonecheck_runs_total Runs by result (success or failure).
# TYPE zonecheck_runs_total counter
zonecheck_runs_total{result="success",zonecheck="cluster"} 1
# HELP zonecheck_workload_affected_pods Affected pods per workload. Workload is like Deployment/web, or Pod/<name> for pods with no controller.
# TYPE zonecheck_workload_affected_pods gauge
zonecheck_workload_affected_pods{issue="OutageStuck",namespace="shop",scenario="Zone",target="a",workload="StatefulSet/db",zonecheck="cluster"} 1
zonecheck_workload_affected_pods{issue="Unschedulable",namespace="shop",scenario="Zone",target="a",workload="Deployment/web",zonecheck="cluster"} 2
zonecheck_workload_affected_pods{issue="Unschedulable",namespace="shop",scenario="Zone",target="a",workload="StatefulSet/db",zonecheck="cluster"} 1
`
	// a real registry, so duplicate series would fail here like they would on a scrape
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"zonecheck_affected_pods", "zonecheck_last_success_timestamp_seconds", "zonecheck_lost_nodes",
		"zonecheck_runs_total", "zonecheck_workload_affected_pods"); err != nil {
		t.Fatal(err)
	}
}

func TestFailedRunKeepsLastResults(t *testing.T) {
	c := New()
	c.Succeeded("cluster", time.Unix(1700000000, 0), time.Second, []Scenario{zoneA()})
	c.Failed("cluster", time.Second)

	if n := testutil.ToFloat64(c.runs.WithLabelValues("cluster", "failure")); n != 1 {
		t.Errorf("failures = %v, want 1", n)
	}
	want := `
# HELP zonecheck_lost_nodes Nodes the simulated failure takes away.
# TYPE zonecheck_lost_nodes gauge
zonecheck_lost_nodes{scenario="Zone",target="a",zonecheck="cluster"} 2
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "zonecheck_lost_nodes"); err != nil {
		t.Fatal(err)
	}
}

func TestForget(t *testing.T) {
	c := New()
	c.Succeeded("cluster", time.Now(), time.Second, []Scenario{zoneA()})
	c.Succeeded("other", time.Now(), time.Second, nil)
	c.Failed("cluster", time.Second)
	c.Forget("cluster")
	if c.Has("cluster") || !c.Has("other") {
		t.Errorf("Has after Forget: cluster=%v other=%v", c.Has("cluster"), c.Has("other"))
	}

	// what's left: other's timestamp, run count, and duration histogram
	if n := testutil.CollectAndCount(c); n != 3 {
		t.Errorf("got %d series after Forget, want 3", n)
	}
}
