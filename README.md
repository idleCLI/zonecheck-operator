# zonecheck

Kubernetes operator that keeps asking "what if this zone died?" and tells you which workloads would have nowhere to go.

>v1alpha1, no release yet. You build the image yourself, and the API can still change.

![zonecheck finding what breaks when us-east-1a goes](docs/demo.gif)

Snapshot the cluster -> take a zone (or node, or pool) away -> run kube-scheduler's own plugins against what's left. Every few minutes, for every zone. Pods that can't be placed get listed with the same message the scheduler would put on them:

```
0/6 nodes are available: 2 node(s) were unschedulable, 4 node(s) didn't match PersistentVolume's node affinity.
```

It only reads. Nothing gets drained, cordoned, or evicted, so it's safe to leave running in a cluster you're nervous about. The cost is that it's a prediction, and a prediction is only as good as the rules it copies. So it copies the scheduler's, rather than writing its own.

How it all works (and why): [docs/design.md](docs/design.md).

## Doesn't something already do this?

Parts of it. cluster-capacity and kube-scheduler-simulator both run real scheduler code against your cluster. cluster-capacity tells you how many more copies of one pod spec fit. kube-scheduler-simulator mirrors your cluster so you can poke at scheduling decisions in a UI. Neither takes a zone (or node, or pool) away and tells you what's left with nowhere to go.

Gremlin's Detected Risks checks whether you're spread across zones. It doesn't check whether the surviving zones have room for everything that moves, and that gets less likely as a cluster fills up. Its zone redundancy Scenario can show you, but by actually cutting a zone off.

## Usage

You'll need Go 1.26+ and a cluster (kind's fine).

```sh
make install   # CRDs into your current kubectl context
make run       # controller, locally
kubectl apply -f config/samples/checks_v1alpha1_zonecheck.yaml
```

In the cluster instead, push the image somewhere it can pull from:

```sh
make docker-build docker-push IMG=<registry>/zonecheck:<tag>
make deploy IMG=<registry>/zonecheck:<tag>
```

```yaml
apiVersion: checks.zonecheck.dev/v1alpha1
kind: ZoneCheck
metadata:
  name: cluster
spec:
  interval: 5m
  scenarios:
  - type: Zone
  - type: BusiestNode
  - type: NodePool
    nodePool:
      labelKey: <your provider's pool label>
      name: gpu
  # only if your control plane changed these flags (defaults shown)
  nodeEviction:
    unhealthyZoneThresholdPercent: 55  # --unhealthy-zone-threshold
    largeClusterSizeThreshold: 50      # --large-cluster-size-threshold
```

`Zone` loses each zone one at a time. `BusiestNode` loses the node whose pods would need the most room elsewhere. `NodePool` loses every node with that label.

Then:

```sh
kubectl get zonecheck cluster -o yaml
```

One result per scenario, with counts and up to 25 named pods (the metrics below have all of them). Check the `ZoneLabelsComplete` condition first. Nodes without a zone label belong to no zone, so they never get taken away and the results look better than they are. Clusters built by hand (kind included) usually need the labels added.

## Metrics

Same numbers as the status, uncapped, for alerting. Labels: `zonecheck` (the object's name), `scenario` (`Zone`, `BusiestNode`, `NodePool`), `target` (zone, node, or pool name), and `issue` (`Unschedulable`, `DrainBlocked`, `OutageStuck`, `Lost`).

| Metric | What |
|---|---|
| `zonecheck_affected_pods` | pods with trouble, per scenario and issue. Always there, 0 when fine |
| `zonecheck_workload_affected_pods` | same, split by `namespace` + `workload` (`Deployment/web`, or `Pod/<name>` with no controller). Only nonzero ones |
| `zonecheck_displaced_pods` | pods a drain would move |
| `zonecheck_lost_nodes` | nodes the failure takes away |
| `zonecheck_last_success_timestamp_seconds` | last run that finished |
| `zonecheck_runs_total` | runs, by `result` (`success`, `failure`) |
| `zonecheck_run_duration_seconds` | how long runs take |

A failed run keeps the last good numbers. So alert on them going stale too, or a broken zonecheck looks like a healthy cluster:

```yaml
- alert: ZoneLossLeavesPodsStranded
  expr: zonecheck_affected_pods{scenario="Zone", issue="Unschedulable"} > 0
  for: 30m
- alert: ZoneCheckStale
  expr: time() - zonecheck_last_success_timestamp_seconds > 30 * 60 or absent(zonecheck_last_success_timestamp_seconds)
  for: 10m
```

The first `for` is there because a rollout briefly needs room for both old and new pods. Set the stale window to a few of your `interval`s. The `absent` catches a zonecheck that's failed every run since it started (no success -> no timestamp at all).

After a restart it runs right away, whatever the `interval`, so the numbers come back quickly.

Locally: `go run ./cmd/main.go --metrics-bind-address=:8080 --metrics-secure=false`, then `curl localhost:8080/metrics`. In a cluster it's HTTPS on 8443, and only callers bound to the `zonecheck-metrics-reader` ClusterRole get in. There's a ServiceMonitor in `config/prometheus`, off by default (uncomment it in `config/default`). Only the leader has numbers.

## What it checks

kube-scheduler's default rule set: CPU/memory, taints, node selectors + affinity, host ports, volumes (zonal disks, attach limits, ReadWriteOncePod), topology spread, and pod (anti-)affinity. Pods go highest priority first, each one using up room for the next. Pods already shutting down still take room till they're gone.

Lost nodes stay in the picture, cordoned, like they would after a real drain. That's on purpose. With `whenUnsatisfiable: DoNotSchedule`, the scheduler still counts the dead zone (as 0 pods), so it won't put more pods anywhere else, and the replacements sit pending. zonecheck catches that one. The fix is usually `nodeTaintsPolicy: Honor` on the constraint.

A drain also goes through PodDisruptionBudgets. A PDB that allows no disruptions, or one with more homeless pods than budget, stalls the drain, and those pods come back as `DrainBlocked`. Same if more than one PDB covers a pod (the Eviction API refuses those). zonecheck assumes the best eviction order, so treat it as a lower bound.

Pods with ephemeral volumes get a fresh claim, like a real replacement would, so they aren't stuck to the old pod's disk.

## Drain vs outage

They don't play out the same, so each result covers both.

In a drain, everything with a controller gets evicted and recreated somewhere. In an outage, some pods sit on the dead node until someone steps in: StatefulSet pods, Jobs that only replace failed pods, anything tolerating an unreachable node forever. Those come back as `OutageStuck`.

Pods with no controller are gone either way (`Lost`). Same for Job pods when the evictions would push the Job past its `backoffLimit`.

Kubernetes also throttles evictions when lots of nodes go down at once. If most of a small zone dies (or every zone does), it stops evicting entirely, and every pod there is stuck. zonecheck uses the node controller's thresholds for that, which is what `nodeEviction` is for if your platform changed them.

## Limitations

Your scheduler's config isn't read. Custom plugin weights or scoring (GKE's "optimize utilization", say) can put pods somewhere else than zonecheck expects, and pods using a different `schedulerName` get the default rules anyway.

No preemption. A high-priority pod that would bump others shows up as unschedulable.

No DRA (`ResourceClaim`s for GPUs and the like).

Pods already waiting to be scheduled aren't counted as competing for the room.

Pods a PDB blocks are still placed as if they moved. In a real drain they'd stay put, so `Unschedulable` can be a bit high when a PDB blocks pods in the same zone.

Nodes get checked in name order where the scheduler races its workers, and score ties go to the first node by name instead of a random one. Same answer every run, at the cost of sometimes differing from the real pick. Big clusters only check a sample, same size as the scheduler's.

In an outage, pods on dead nodes still count for zone spread till they're evicted (with the default `nodeTaintsPolicy`), so spread can play out a bit differently than the drain-based numbers.

Memory grows with the cluster, since it caches every pod and node (about 80MB on a 6-node test cluster). The manifest's limit is 512Mi, so raise it for big clusters.

Pods owned by other controllers (Argo Rollouts, CloneSets, ...) are assumed to get recreated like a ReplicaSet's. Workloads are named after whatever owns the pod's ReplicaSet or Job when there is one (-> `Deployment/web`, `Rollout/web`, `CronJob/nightly`), otherwise the pod's own owner.

## Development

```sh
make test
make lint
```

Unit tests run the scheduler plugins directly: resources, priority order, spread with and without `Honor`, a volume pinned to a lost zone, two pods fighting over one disk, the PDB and eviction rules, and a 1000-node benchmark. The metrics ones go through a real Prometheus registry, so a duplicate series fails the test instead of your scrape.

The controller tests run against a real API server (envtest), no cluster needed. They cover a full run with a zonal disk, a PDB, an ephemeral volume, and an unlabeled node, plus the CRD's validation. One test runs the controller with only the permissions in `config/rbac/role.yaml`, so a missing RBAC rule fails there and not in your cluster.

The e2e test drains a zone for real and compares:

```sh
make test-e2e   # needs kind + docker. 3-zone cluster from test/e2e/kind-config.yaml
```

One workload per kind of problem zonecheck reports. It asks zonecheck about losing zone-a, then runs `kubectl drain` on zone-a. Pass = the pods left pending, stuck behind the PDB, or gone are exactly the ones zonecheck listed. Empty prediction -> fail, so a broken setup can't pass.

Drains only. An outage means sitting out the 5 min eviction timeout, so `OutageStuck` is unit tests only.

Own cluster w/ 3 zones x 2 nodes? `E2E_SKIP_DEPLOY=true` skips the image build + deploy -> run the manager yourself (`make install run`).

## License

Apache-2.0
