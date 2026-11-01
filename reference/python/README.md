# nvlink-rack-scheduler: Python reference implementation

The reference implementation of the policy described in the
[top-level README](../../README.md), packaged as a standalone Kubernetes
secondary scheduler. Its results define the shared scenarios in
`testdata/scenarios/` that other implementations must match. Commands below
run from this directory.

## Usage

Label a workload's pods:

| Field | Required | Meaning |
|---|---|---|
| `spec.schedulerName: nvlink-rack-scheduler` | yes | Opt in to this scheduler |
| label `nvlink-rack.io/gang` | yes | Gang name (per namespace) |
| annotation `nvlink-rack.io/gang-size` | yes | Number of pods in the gang |
| annotation `nvlink-rack.io/slice-size` | no | Ranks per rack (default `--slice-size`) |
| annotation `nvlink-rack.io/preemptible: "true"` | no | Restart-safe: defrag may evict it |
| rank | yes | Indexed Job completion index, StatefulSet pod index, or annotation `nvlink-rack.io/rank` |

See `examples/indexed-job.yaml`. Deploy:

```bash
docker build -t nvlink-rack-scheduler:0.1.0 .
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
an in-memory fake API, and `tests/test_scenarios.py` checks the shared
scenarios. `tools/scenarios.py` regenerates them.

### End-to-end on a laptop, no GPUs

[KWOK](https://kwok.sigs.k8s.io/) runs a real API server, controller manager
and Job controller with fake nodes. `e2e/run.py` builds 4 racks x 18 nodes x
4 GPUs and checks slicing, spare protection, same-rack recovery after a node
failure (with a freshly restarted scheduler), and defrag:

```bash
brew install kwok kubectl
kwokctl create cluster --name nvlink-rack --runtime binary --config e2e/kwok-stages.yaml
kwokctl get kubeconfig --name nvlink-rack > /tmp/nvlink-rack.kubeconfig
KUBECONFIG=/tmp/nvlink-rack.kubeconfig .venv/bin/python e2e/run.py   # ~5 minutes
kwokctl delete cluster --name nvlink-rack
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

