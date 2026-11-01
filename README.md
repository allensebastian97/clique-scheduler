# clique-scheduler

A Kubernetes secondary scheduler that places multi-node GPU jobs along
NVLink domains (GB200/GB300 NVL72 racks). It is an open-source take on
the topology-aware scheduling, spare-node recovery and defragmentation that
[SkyPilot describes](https://skypilot.ai/blog/grace-blackwell-needs-a-new-scheduler)
for its closed Platform.

## Why

An NVL72 rack gives every GPU 1.8 TB/s of NVLink; leave the rack and you're on
~50 GB/s of InfiniBand. The default Kubernetes scheduler doesn't know which
nodes share a rack, so it scatters multi-node jobs. Racks also lose nodes
often (about every 5 hours somewhere on a 128-rack cluster), so a job that
needs all 18 nodes of a rack is fragile.

## Policy

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

## Usage

Label a workload's pods:

| Field | Required | Meaning |
|---|---|---|
| `spec.schedulerName: clique-scheduler` | yes | Opt in to this scheduler |
| label `cliquesched.io/gang` | yes | Gang name (per namespace) |
| annotation `cliquesched.io/gang-size` | yes | Number of pods in the gang |
| annotation `cliquesched.io/slice-size` | no | Ranks per rack (default `--slice-size`) |
| annotation `cliquesched.io/preemptible: "true"` | no | Restart-safe: defrag may evict it |
| rank | yes | Indexed Job completion index, StatefulSet pod index, or annotation `cliquesched.io/rank` |

See `examples/indexed-job.yaml`. Deploy:

```bash
docker build -t clique-scheduler:0.1.0 .
kubectl apply -f deploy/scheduler.yaml
```

Flags: `--slice-size`, `--reserve`, `--defrag=off|dry-run|on`,
`--migrate-slices`, `--clique-label`, `--cooldown`, `--interval`, `--once`.

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/python -m pytest
```

The planner (`placement.py`, `defrag.py`) works on plain dataclasses and has
no Kubernetes dependency. `k8s.py` translates API JSON into that model, and
`scheduler.py` is the loop. `tests/test_scheduler.py` drives the loop against
an in-memory fake API.

### End-to-end on a laptop, no GPUs

[KWOK](https://kwok.sigs.k8s.io/) runs a real API server, controller manager
and Job controller with fake nodes. `e2e/run.py` builds 4 racks x 18 nodes x
4 GPUs and checks slicing, spare protection, same-rack recovery after a node
failure (with a freshly restarted scheduler), and defrag:

```bash
brew install kwok kubectl
kwokctl create cluster --name clique --runtime binary --config e2e/kwok-stages.yaml
kwokctl get kubeconfig --name clique > /tmp/clique.kubeconfig
KUBECONFIG=/tmp/clique.kubeconfig .venv/bin/python e2e/run.py   # ~5 minutes
kwokctl delete cluster --name clique
```

`e2e/kwok-stages.yaml` is KWOK's default pod lifecycle with two changes. Job
pods keep running instead of completing after a second, and deleted pods go
to `Failed` the way a kubelet would move them.

## Limitations

* **Not yet run on real GPU nodes.** It has been tested against a real
  control plane (KWOK) with fake nodes, which doesn't cover GPU Operator
  labelling, IMEX, or real kubelet timing.
* One replica, no leader election. It polls rather than watches, which is
  fine for thousands of nodes but not tuned beyond that. Planning a 128-node
  gang on a 2,304-node cluster takes about 0.16 s.
* Node health is `Ready` and not cordoned. Faster GPU fault detection (DCGM,
  XID errors) should taint or cordon nodes; this scheduler reacts to that.
* It only does placement. IMEX / ComputeDomain setup for multi-node NVLink
  is still needed (see NVIDIA's DRA driver).
* Fit checks cover resource requests, `nodeSelector` and taints. They don't
  cover affinity, topology spread or volume topology.
* No queueing beyond priority then age, and no capacity reservation for
  blocked gangs. Smaller gangs can backfill past a blocked big one; defrag is
  the remedy.
* Placement and defrag are greedy, not optimal.
* Recovering a replaced pod into a running job (re-forming NCCL groups) is
  up to the training framework (e.g. torchrun elastic, Ray, TorchFT).
