# Design

How zonecheck decides what breaks, and why it's built the way it is. The README covers what it does. This covers how.

## The question

"If zone X went away right now, what would have nowhere to go?"

Being spread across zones doesn't answer it. Three replicas in three zones are fine until the two zones that are left don't have room for the third, or a PDB won't let it move, or its disk can only be mounted in the zone that's gone. Those depend on the whole cluster at that moment, and they change every deploy. So zonecheck asks again every `interval`.

## A run

```
snapshot -> pick failures -> drain each one -> place the displaced pods -> check PDBs -> report
```

1. **Snapshot** (`internal/snapshot`). List nodes, pods, Jobs, PDBs, plus everything the scheduler plugins read: PVCs, PVs, StorageClasses, CSI objects, namespaces, Services, ReplicaSets, StatefulSets. It all comes from the controller's informer cache, so a run doesn't hit the API server.
2. **Failures**. `Zone` -> one failure per zone value. `BusiestNode` -> the node whose movable pods take the biggest share of cluster CPU or memory. `NodePool` -> every node with the label. A pool that matches nothing still gives a result (0 lost nodes), so a typo shows up.
3. **Drain**. Work out what each failure does to every pod on the lost nodes (below).
4. **Place** (`internal/placement`). Run kube-scheduler's plugins on what's left, one displaced pod at a time.
5. **PDBs**. Work out which evictions the Eviction API would refuse.
6. **Report**. Status + metrics.

Each failure starts from the same snapshot. They don't stack.

## Why the scheduler's own plugins

Writing my own "does it fit" check would've been quicker and wrong. The scheduler's rules have a lot of edges: topology spread with `nodeTaintsPolicy`, volume binding for WaitForFirstConsumer classes, attach limits per CSI driver, inter-pod affinity across zones, the order filters run in. A copy drifts the first time any of them changes.

So zonecheck builds a real scheduler framework from `k8s.io/kubernetes` (the in-tree plugin registry + the default profile) and calls it the way kube-scheduler's `schedulePod` does: PreFilter, Filter, PreScore, Score, Reserve. The upside is the error messages come out word for word what the scheduler would put on the pod.

What it takes to make that work outside kube-scheduler:

- **The plugins read informers.** VolumeBinding, NodeVolumeLimits, and friends want listers for PVCs, PVs, StorageClasses, etc. zonecheck feeds them a fake clientset loaded with the snapshot's objects, so they see the same point in time as everything else.
- **One framework, many failures.** Building a framework is slow, so a run builds one and swaps the snapshot under it for each failure.
- **Reserve, then undo it.** VolumeBinding claims a free PV at Reserve. Without that, two pods could both "get" the same disk. Every Reserve is undone at the end of a failure so the next one starts clean.
- **Same answer every run.** The scheduler's workers race each other and it breaks score ties at random. zonecheck walks nodes in name order (still moving the start point along after each pod, like the scheduler) and gives ties to the first name. Big clusters still only get a sample of nodes, the same size the scheduler would use (`numFeasibleNodesToFind` w/ the default `percentageOfNodesToScore`).
- **No batching.** kube-scheduler 1.35+ can reuse the last pod's scores for an identical pod (`OpportunisticBatching`). zonecheck calls the plugins itself, so every pod gets the full pass.
- **Left out.** Preemption (it'd mean evicting pods inside the simulation) and DRA (needs ResourceSlices + a lot of state). Both are listed under limitations.

Pods are placed highest priority first, then oldest, like the scheduler's queue, and each one uses up room for the next.

## The drain model

Lost nodes don't disappear from the simulation. They stay, cordoned (`spec.unschedulable` + the `node.kubernetes.io/unschedulable` taint), because that's what a real drain leaves and the scheduler still sees them. This is the part that catches the zone spread trap: with `whenUnsatisfiable: DoNotSchedule` and the default `nodeTaintsPolicy: Ignore`, the dead zone still counts as a domain with 0 pods. So the skew check won't let more pods into the zones that are left. `Honor` drops tainted nodes from the count and the problem goes away.

What happens to pods on a lost node:

- **DaemonSet + static pods** go down with the node. They stay where they are.
- **Pods with no controller** are `Lost`. Nothing recreates them.
- **Job pods** count each eviction as a failure. If that pushes the Job past `backoffLimit`, they're `Lost` too.
- **Everything else** gets recreated, so it's placed again as a new pod.
- **Pods already terminating** stay on surviving nodes, taking room till they're gone. On lost nodes they're ignored.
- **Ephemeral volumes** get a fresh claim from the template, because a replacement pod gets a new PVC, not the old one (which is often stuck in the old zone). Only for WaitForFirstConsumer classes. An Immediate class provisions wherever, so the volume's left out of the check.

## Drain vs outage

They play out differently, so each result covers both.

A drain evicts. An outage doesn't: the node just stops answering, and some pods wait for a person. These are `OutageStuck`:

- StatefulSet pods (the controller won't make a second `web-0` while the first might still be running)
- Jobs with `podReplacementPolicy: Failed` or a `podFailurePolicy`
- pods that tolerate `node.kubernetes.io/unreachable` with no `tolerationSeconds`

On top of that the node lifecycle controller throttles. A zone with more than 2 nodes down and at least `unhealthyZoneThresholdPercent` of them down is "partially disrupted". If it's no bigger than `largeClusterSizeThreshold`, evictions there stop. If every zone is down, evictions stop everywhere. zonecheck copies those rules (the thresholds are settable under `nodeEviction` for platforms that changed the flags), and pods in a stopped zone are `OutageStuck`.

## PDBs

A drain goes through the Eviction API, and that's where PDBs bite. zonecheck follows `pkg/registry/core/pod/storage/eviction.go`:

- Pending, finished, and already-deleting pods skip PDBs.
- More than one PDB covering a pod -> refused (the API returns a 500).
- Unready pods can go without using the budget: always under `AlwaysAllow`, and under `IfHealthyBudget` (the default) while the app is otherwise healthy.
- A PDB whose status is behind its generation -> refused ("still being processed").
- No disruptions allowed -> refused.

Then it assumes the best order a drain could take: pods that fit somewhere go first, one at a time, each replacement coming up Ready and handing the budget back before the next eviction. Pods with nowhere to go use up the budget for good. Whatever's left over is `DrainBlocked`. A real drain can do worse, never better, so it's a lower bound.

One rough edge: drain-blocked pods are still placed as if they moved. In a real drain they'd stay put and not use any room elsewhere, so `Unschedulable` can be a bit pessimistic when a PDB blocks pods in the same failure.

## Reporting

**Status** has one result per failure, capped at 64 results and 25 named pods each (the counts are always complete). Owners are shown as workloads (`Deployment/web`, not the ReplicaSet), so the names don't change every rollout.

**Metrics** have the same numbers with nothing capped, plus a per-workload series. The collector keeps the last good run per ZoneCheck and serves it on every scrape. A failed run doesn't wipe it, so there's `zonecheck_last_success_timestamp_seconds` to alert on staleness. After a restart the collector's empty, so the first reconcile runs right away instead of waiting out the interval.

Runs are spaced by `interval` using `status.lastRunTime`, so a resync or restart doesn't trigger an early run (unless the metrics are empty). Status-only updates don't trigger runs either.

## Testing

- **Unit tests** call the scheduler plugins directly: resources, priority order, spread with and without `Honor`, a volume pinned to a lost zone, two pods after one PV, the PDB rules, ephemeral claims, eviction throttling, and a 1000-node benchmark. Metrics go through a real Prometheus registry.
- **Controller tests** run against a real API server (envtest), including one that runs the reconciler with only the RBAC in `config/rbac/role.yaml`.
- **The e2e test** predicts a zone loss on a 3-zone kind cluster, drains the zone for real with `kubectl drain`, and checks that the pods left pending, stuck behind a PDB, or gone match the prediction exactly.

## Not done

- Preemption + DRA (see above).
- The scheduler's real config: custom plugin weights, other profiles, `schedulerName`.
- Pods already pending aren't counted as competing for room.
- Drain-blocked pods still take room in placement (see PDBs).
- Outages aren't checked against a real cluster, only drains. A real one means waiting out the 5 min eviction timeout.
