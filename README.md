# nvlink-rack-scheduler

Open-source placement policy for multi-node GPU jobs on NVLink rack-scale
systems (GB200/GB300 NVL72), with integrations for the schedulers GPU clouds
already run. It is an open take on the topology-aware scheduling, spare-node
recovery and defragmentation that
[SkyPilot describes](https://skypilot.ai/blog/grace-blackwell-needs-a-new-scheduler)
for its closed Platform.

## Why

An NVL72 rack gives every GPU 1.8 TB/s of NVLink; leave the rack and you're on
~50 GB/s of InfiniBand. The default Kubernetes scheduler doesn't know which
nodes share a rack, so it scatters multi-node jobs. Racks also lose nodes
often (about every 5 hours somewhere on a 128-rack cluster), so a job that
needs all 18 nodes of a rack is fragile.

## Policy

A **clique** is NVIDIA's name for a set of GPUs sharing one NVLink domain; on
NVL72 it is one rack. The code and node label (`nvidia.com/gpu.clique`) use
that term, and "rack" means the same thing here.

* **Slices.** A gang is cut into slices of `slice-size` consecutive ranks
  (default 16 nodes = 64 GPUs). Each slice lands entirely inside one NVLink
  clique, identified by the `nvidia.com/gpu.clique` node label that NVIDIA's
  GPU Operator sets. A gang no larger than one slice sits on a single rack.
* **Spares.** New slices can't use a rack's last `reserve` idle nodes
  (default 2). A node that has failed counts against the reserve, so a rack
  with two dead nodes still takes a 16-node slice. This is the 16-of-18
  policy behind the 27% figure in SkyPilot's and NVIDIA's analysis.
* **Best fit.** Each slice goes to the rack it fills most tightly, so emptier
  racks stay whole for bigger slices.
* **Gang placement.** No pod of a new gang is bound until every pod exists
  and the whole gang fits.
* **Same-rack replacement.** When a pod is replaced (node failure, crash),
  the new pod goes back to its slice's rack and may use the spares, so the
  slice stays on NVLink. If the rack has no spare left, `--migrate-slices`
  evicts the rest of the slice so it can be placed whole elsewhere.
* **Defrag.** If a gang is blocked by fragmentation (enough GPUs free, but
  no rack has room), the scheduler finds the cheapest set of restart-safe
  gangs to evict so it fits, but only if every evicted gang can land again
  somewhere else. The freed nodes are then held for the blocked gang, so
  the victims' replacement pods (often created while the old ones are still
  terminating) can't land right back on them. `--defrag=dry-run` (the
  default) only logs the plan.

The scheduler keeps almost no state. A slice's rack is read back from where
its pods run, and a pod's slice from its rank, so restarts are safe. Only
in-flight defrag holds live in memory.

## Layout

| Path | What | Status |
|---|---|---|
| `pkg/policy/` | The policy in Go, no scheduler dependencies. Every integration builds on it. | Done; matches the reference on all shared scenarios |
| `testdata/scenarios/` | Shared scenarios: cluster state + operation + expected result. The contract every implementation must meet. | 18 hand-written, 40 randomized |
| `reference/python/` | Reference implementation, a standalone Kubernetes scheduler, and a KWOK end-to-end suite | Working; see its [README](reference/python/README.md) |
| `integrations/kai/` | KAI Scheduler plugin: spares and same-rack replacement on top of KAI's topology segments and consolidation | Working: KAI's in-process tests, and a live test with KAI's scheduler and binder on KWOK; see its [README](integrations/kai/README.md) |
| Kubernetes scheduler-framework plugin | Same policy for clusters running the stock kube-scheduler | Planned |
| Slurm daemon | Defrag and replacement around Slurm's `topology/block` and `--segment` | Planned |

## Shared scenarios

The Python implementation is the reference. Each file in `testdata/scenarios/`
records a cluster, an operation (`plan_gang`, `plan_defrag`,
`fits_ignoring_topology`) and the reference's exact result, down to which node
each pod lands on. Every implementation runs the same files:

```bash
go test ./pkg/policy/                                   # Go
cd reference/python && python -m pytest tests/test_scenarios.py   # Python
```

After an intentional policy change, regenerate the expectations from the
reference and review the diff:

```bash
cd reference/python && python tools/scenarios.py --update
```

`--random N` regenerates the randomized fragmented-cluster scenarios.

## License

Apache License 2.0. See [LICENSE](LICENSE).
