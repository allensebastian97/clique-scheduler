# nvlinkrack: KAI Scheduler plugin

Adds the spares and same-rack replacement rules to
[NVIDIA KAI Scheduler](https://github.com/NVIDIA/KAI-Scheduler).

## What KAI already does, and what this adds

KAI handles most of the policy itself:

* **Segments.** It splits a workload's ranks into segments of
  `kai.scheduler/segment-size` and keeps each segment inside one topology
  domain. Point the topology at `nvidia.com/gpu.clique` and each domain is
  one NVL72 rack.
* **Bin-packing.** It prefers fuller domains.
* **Scheduling basics.** Gang scheduling, queues, fair share, preemption.
* **Consolidation.** It evicts preemptible jobs to make room, but only if
  they can be placed again elsewhere.

`nvlinkrack` adds what's missing:

| Rule | Behaviour |
|---|---|
| **Spares** | A GPU pod may not take one of the last `spares` (default 2) idle nodes of an NVLink clique. Nodes that aren't ready count against the spares, so a rack with two failed nodes still takes a 16-node segment. Applies to every GPU pod, including victims that consolidation, preemption and reclaim re-place. |
| **Same-rack replacement** | A pod of a clique-constrained segment whose other members are still running must go to their clique, and may use its spares. KAI already pins such pods to their domain; this lets them use the capacity kept for exactly that case. |

Without the plugin, KAI's bin-packing does the opposite of what NVL72 needs.
It places small jobs on a full rack's last idle nodes (because that rack is
fullest), and it fills all 18 nodes of a rack with one segment. The tests
below show both.

## How it works

`nvlinkrack` is a predicate. KAI evaluates predicates per pod, after the
pod's earlier tentative placements in the cycle have already updated node
usage. When a rack's nodes are all rejected, KAI rolls back and tries the
next rack its topology plugin offered.

Filtering racks inside the topology step alone wouldn't work, because KAI's
topology plugin counts spares as free capacity.

Only a genuinely new pod gets the replacement exception: `Pending`, and not
tentative (`IsVirtualStatus`). Tentative state doesn't count on either side:

* **Members placed earlier in this cycle.** Otherwise a new segment's first
  pod would make the rest of the segment "replacements" allowed onto the
  spares.
* **Running pods that consolidation, preemption or reclaim are re-placing.**
  The live test found this one. Consolidation evicted a running 16-node
  segment and re-placed it with two pods on the spares, to free an ordinary
  node for a 1-pod job. The spares were used up and a training job was
  disrupted to place one pod.

## Build and deploy

KAI plugins are compiled into the scheduler binary.
`cmd/kai-scheduler/main.go` imports KAI at a pinned version, registers the
plugin, and runs KAI's own entry point. It's the stock scheduler plus one
plugin.

```bash
# from the repository root
docker build -f integrations/kai/Dockerfile -t $KAI_REGISTRY/scheduler:v0.18-nvlinkrack .
docker push $KAI_REGISTRY/scheduler:v0.18-nvlinkrack

kubectl apply -f integrations/kai/deploy/topology.yaml
helm upgrade kai-scheduler <kai-chart> -f your-values.yaml -f integrations/kai/deploy/values.yaml
```

`deploy/values.yaml` points the scheduler at the new image tag and enables
the plugin with priority 450, which is below `topology` (500). Annotate
workloads as KAI's segment docs describe, for example on a PyTorchJob's
worker template:

```yaml
kai.scheduler/topology: nvlink
kai.scheduler/segment-size: "16"
kai.scheduler/segment-topology-required-placement: nvidia.com/gpu.clique
```

## Tests

`nvlinkrack/plugin_test.go` runs KAI's real `allocate` action with its
default plugins on synthetic 18-node racks of 4-GPU nodes:

```bash
cd integrations/kai && go test ./...
```

| Test | Without plugin | With plugin |
|---|---|---|
| 32 ranks, segments of 16 | not checked | one segment per rack, 2 spares left on each |
| 18-node segment on an 18-node rack | fills the rack | waits |
| 16-node segment, rack with 2 failed nodes | not checked | placed, avoiding the failed nodes |
| 2-pod job next to a full 16-node segment | takes that rack's spares | goes to the other rack |
| Lost rank 5, only spares idle in its rack | (KAI pins it to the rack) | lands on a spare |
| Lost rank 5, no spare left | (KAI pins it to the rack) | waits; never leaves the rack |
| allocate + consolidation, a 1-pod job beside a 16-node segment on an 18-node rack | the job takes a spare | the job waits; the segment isn't touched |

Breaking any of the plugin's rules (the predicate, the tentative placement
check, the replacement exception) fails the matching tests.

### Live test

`e2e/live.py` runs the real scheduler binary (with the plugin compiled in)
and KAI's binder as local processes against a
[KWOK](https://kwok.sigs.k8s.io/) cluster: a real API server and controllers,
with 4 racks x 18 fake 4-GPU nodes. Every scenario runs twice, once with KAI's
stock configuration and once with `nvlinkrack` added after `topology`:

| Scenario | Stock KAI | With nvlinkrack |
|---|---|---|
| 18-node segment | all 18 on one rack | waits |
| 2 segments of 16, then a 2-pod job | job on a segment rack's spares (0 left) | job on another rack (2 spares left on each segment rack) |
| Rank 5 lost; a new pod pinned to that rack; then the replacement | new pod takes a spare, replacement the other | new pod waits; replacement takes a spare; consolidation tried 22 times and evicted nothing |

Run it with the commands at the top of `e2e/live.py`. It needs a KAI checkout
for the binder and the CRDs. `--mode`, `--only` and `--verbosity` narrow a run
for debugging.

## Status and limits

* Pinned to KAI commit `0243c4e` (Oct 28, 2026). Bump it in `go.mod` together
  with the `k8s.io` pins copied from KAI's `go.mod`.
* The live test runs KAI's scheduler and binder as processes, not through
  KAI's operator and Helm chart. It doesn't run the pod grouper, so it uses
  hand-written PodGroups rather than PyTorchJob annotations. It hasn't run on
  real GPU nodes.
* KAI only wires up segment annotations for PyTorchJob and LeaderWorkerSet.
  Other workloads need a hand-written PodGroup with subgroups. The spares
  rule applies to every GPU pod regardless.
* Fractional (shared) GPU pods are untested.
* The Dockerfile hasn't been built yet (no Docker in the development
  environment); the binary itself builds and the tests run.
