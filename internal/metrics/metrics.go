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

// Package metrics serves each ZoneCheck's latest run to Prometheus.
package metrics

import (
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	checksv1alpha1 "github.com/idleCLI/zonecheck-operator/api/v1alpha1"
)

// the ZoneCheck's name
const zonecheckLabel = "zonecheck"

var (
	scenarioLabels = []string{zonecheckLabel, "scenario", "target"}

	lastSuccessDesc = prometheus.NewDesc("zonecheck_last_success_timestamp_seconds",
		"When the ZoneCheck last finished a run.", []string{zonecheckLabel}, nil)
	lostNodesDesc = prometheus.NewDesc("zonecheck_lost_nodes",
		"Nodes the simulated failure takes away.", scenarioLabels, nil)
	displacedDesc = prometheus.NewDesc("zonecheck_displaced_pods",
		"Pods a controller would recreate elsewhere after a drain.", scenarioLabels, nil)
	affectedDesc = prometheus.NewDesc("zonecheck_affected_pods",
		"Pods the simulated failure causes trouble for, by issue.",
		slices.Concat(scenarioLabels, []string{"issue"}), nil)
	workloadDesc = prometheus.NewDesc("zonecheck_workload_affected_pods",
		"Affected pods per workload. Workload is like Deployment/web, or Pod/<name> for pods with no controller.",
		slices.Concat(scenarioLabels, []string{"issue", "namespace", "workload"}), nil)

	issues = []checksv1alpha1.PodIssue{
		checksv1alpha1.PodUnschedulable, checksv1alpha1.PodDrainBlocked,
		checksv1alpha1.PodOutageStuck, checksv1alpha1.PodLost,
	}
)

// one simulated failure. Unlike the status, nothing's capped
type Scenario struct {
	Type          checksv1alpha1.ScenarioType
	Target        string
	LostNodes     int
	DisplacedPods int
	Affected      map[Workload]int
}

type Workload struct {
	Namespace string
	Name      string
	Issue     checksv1alpha1.PodIssue
}

type lastRun struct {
	at        time.Time
	scenarios []Scenario
}

// holds the last good run per ZoneCheck. A failed run keeps the old
// numbers -> alert on zonecheck_last_success_timestamp_seconds going stale
type Collector struct {
	mu   sync.Mutex
	last map[string]lastRun

	runs     *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func New() *Collector {
	return &Collector{
		last: map[string]lastRun{},
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "zonecheck_runs_total",
			Help: "Runs by result (success or failure).",
		}, []string{zonecheckLabel, "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "zonecheck_run_duration_seconds",
			Help:    "How long a run takes, failed ones included.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 12),
		}, []string{zonecheckLabel}),
	}
}

func (c *Collector) Succeeded(zonecheck string, at time.Time, took time.Duration, scenarios []Scenario) {
	c.mu.Lock()
	c.last[zonecheck] = lastRun{at: at, scenarios: scenarios}
	c.mu.Unlock()
	c.runs.WithLabelValues(zonecheck, "success").Inc()
	c.duration.WithLabelValues(zonecheck).Observe(took.Seconds())
}

func (c *Collector) Failed(zonecheck string, took time.Duration) {
	c.runs.WithLabelValues(zonecheck, "failure").Inc()
	c.duration.WithLabelValues(zonecheck).Observe(took.Seconds())
}

// false after a restart, till the first run
func (c *Collector) Has(zonecheck string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.last[zonecheck]
	return ok
}

// the ZoneCheck is gone -> so are its series
func (c *Collector) Forget(zonecheck string) {
	c.mu.Lock()
	delete(c.last, zonecheck)
	c.mu.Unlock()
	c.runs.DeletePartialMatch(prometheus.Labels{zonecheckLabel: zonecheck})
	c.duration.DeleteLabelValues(zonecheck)
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{lastSuccessDesc, lostNodesDesc, displacedDesc, affectedDesc, workloadDesc} {
		ch <- d
	}
	c.runs.Describe(ch)
	c.duration.Describe(ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, run := range c.last {
		ch <- prometheus.MustNewConstMetric(lastSuccessDesc, prometheus.GaugeValue, float64(run.at.Unix()), name)
		// same scenario listed twice -> same labels, and Prometheus rejects the whole scrape
		seen := map[[2]string]bool{}
		for _, s := range run.scenarios {
			key := [2]string{string(s.Type), s.Target}
			if seen[key] {
				continue
			}
			seen[key] = true
			labels := []string{name, string(s.Type), s.Target}
			ch <- prometheus.MustNewConstMetric(lostNodesDesc, prometheus.GaugeValue, float64(s.LostNodes), labels...)
			ch <- prometheus.MustNewConstMetric(displacedDesc, prometheus.GaugeValue, float64(s.DisplacedPods), labels...)

			// every issue, even at 0 -> a fixed problem shows as 0, not a gap
			byIssue := make(map[checksv1alpha1.PodIssue]int, len(issues))
			for w, n := range s.Affected {
				byIssue[w.Issue] += n
				ch <- prometheus.MustNewConstMetric(workloadDesc, prometheus.GaugeValue, float64(n),
					name, string(s.Type), s.Target, string(w.Issue), w.Namespace, w.Name)
			}
			for _, issue := range issues {
				ch <- prometheus.MustNewConstMetric(affectedDesc, prometheus.GaugeValue, float64(byIssue[issue]),
					name, string(s.Type), s.Target, string(issue))
			}
		}
	}
	c.runs.Collect(ch)
	c.duration.Collect(ch)
}
