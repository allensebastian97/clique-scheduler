# nvlink-rack-slurmd: Slurm daemon

Adds spares, same-block replacement and defragmentation to Slurm's
`topology/block` plugin on NVLink rack-scale systems (one block per NVL72
rack).

## What Slurm already does, and what this adds

With `TopologyPlugin=topology/block` and `--segment=16`, Slurm keeps each
16-node segment inside one block. A job that fits in one block stays in one
block. `nvlink-rack-slurmd` adds:

| Rule | How |
|---|---|
| **Spares** | One reservation per block, `nvlr-spares-<block>`, holding up to `--spares` (default 2) idle nodes. Failed nodes count against the spares, so a block with two dead nodes keeps none. Which nodes doesn't matter, only how many: each pass points the reservation at the block's highest-numbered idle nodes. |
| **Same-block replacement** | When a job is requeued because one of its nodes failed, it is limited to the blocks it ran in (`ExcNodeList`). It also gets a reservation over those blocks' spares (`OVERLAP,FLEX`: spares plus ordinary free nodes), and it becomes eligible immediately instead of after Slurm's 120 s requeue delay. |
| **Defrag** | Sometimes a job waits only because no block has room, even though enough nodes are idle. The daemon then requeues the cheapest restart-safe jobs (planned by `pkg/policy`) and holds the freed nodes for the waiting job with a reservation (`IGNORE_JOBS`). Restart-safe means requeueable (`--requeue`) with `nvlink-rack.io/preemptible` in `--comment`. |

Without the daemon, Slurm's bin-packing puts small jobs on a full block's last
nodes, and a fragmented cluster leaves a block-sized job waiting forever. The
live results below show both.

Only a job requeued because one of its nodes actually failed gets the spares
exception. Jobs the daemon requeued for defrag, or that a user requeued, are
new placements. (The KAI plugin taught this lesson: there, consolidation
re-placed a running job onto the spares.)

## Facts verified on Slurm 26.05.5

The design rests on these, each checked against a real cluster:

* `topology/block` **requires `select/cons_tres`**. With `select/linear`,
  Slurm silently ignored block boundaries: `-N32 --segment=16` landed 18 + 14.
* A job requeued after a node failure goes `PENDING` (reason `BeginTime`)
  with `restart_cnt` + 1 and its node list cleared, and becomes eligible after
  120 s. The daemon records running jobs' nodes itself.
* A job may name several reservations, but they are alternatives, not a
  union. A job spanning blocks therefore gets one replacement reservation
  over all its blocks' spares (`OVERLAP` lets it include nodes already in
  the spare reservations).
* `FLEX` lets a job that requests a reservation use reserved and unreserved
  nodes. Users not listed on a reservation are refused.
* A reservation on busy nodes needs `IGNORE_JOBS`.
* A reservation with explicit nodes is not refilled when a node fails, so
  failed nodes really do use up the spares.
* Block membership isn't in the node JSON, so the daemon reads
  `scontrol show topology`.

## Run

```bash
cd integrations/slurm
go build -o nvlink-rack-slurmd ./cmd/nvlink-rack-slurmd
# on a host where scontrol/squeue work with Slurm admin rights:
./nvlink-rack-slurmd --spares=2 --defrag=dry-run
```

Flags: `--spares`, `--defrag=off|dry-run|on` (default `dry-run`, which logs
what it would requeue), `--preemptible` (comment marker), `--hold-for`,
`--state` (job records, default `/var/lib/nvlink-rack-slurmd/state.json`),
`--prefix`, `--interval`, `--once`.

Submit jobs as usual: `sbatch -N32 --segment=16 --requeue ...`. Add
`--comment=nvlink-rack.io/preemptible` to jobs that may be requeued to make
room.

## Tests

```bash
go test ./...
```

* `slurm/`: hostlist expansion and parsers, tested against real Slurm JSON
  in `testdata/`.
* `daemon/`: reconcile logic against an in-memory Slurm. Covers spares,
  failed nodes, spares following idle nodes, replacement steering, user
  requeues, defrag (cheapest victim, hold, dry-run, restart-safe only,
  capacity shortages, node-constrained jobs). Breaking a rule fails its
  test.

### Live test on real Slurm

`e2e/cluster.sh` builds a real Slurm 26.05.5 cluster in a Lima VM: 3
`topology/block` blocks of 18 nodes, one `slurmd` per node (multiple-slurmd
build). `e2e/live.py` runs every scenario with stock Slurm and with the
daemon on. To simulate a node failure it stops the node's `slurmd` and marks
it down.

| Scenario | Stock Slurm | With the daemon |
|---|---|---|
| `-N32 --segment=16`, then a 2-node job | job on `b3`'s last 2 nodes (0 left) | job on `b1`; 2 spares kept in `b2` and `b3` |
| Node failure under the 32-node job | restarts after **125 s** | restarts in the same blocks after **4 s**, using a spare |
| Fragmented: small jobs in every block, then `-N16 --segment=16` | waits indefinitely | `frag-b1` requeued; the job runs in `b1`; spares untouched |

```bash
e2e/cluster.sh vm && e2e/cluster.sh start      # once
GOOS=linux GOARCH=arm64 go build -o /tmp/nvlink-rack-slurmd ./cmd/nvlink-rack-slurmd
limactl copy /tmp/nvlink-rack-slurmd slurm:/tmp/ && limactl copy e2e/live.py slurm:/tmp/
limactl shell slurm -- python3 /tmp/live.py
```

The live test found two bugs before release:

* Defrag tried to "help" a job pinned with `--nodelist` to nodes that were
  spares, planning holds elsewhere. Jobs with node lists, exclusions or their
  own reservation are now left alone.
* The planner re-derived spares and picked different nodes than the
  reservations held, so its holds overlapped them. It now treats the actual
  reservations as the spares.

## Limits

* Whole-node jobs only: the planner models one unit per node. That matches
  NVL72 practice; GRES-level sharing isn't modelled.
* A job larger than one block without `--segment` is left to Slurm and not
  considered for defrag.
* Replacement waits if the job's blocks can't fit it even with their spares.
  It doesn't move the job to other blocks.
* One daemon per cluster, with no leader election. It needs Slurm admin
  rights to create reservations and update and requeue other users' jobs.
* Tested on Slurm 26.05 only. The JSON field names come from its
  `data_parser`.
