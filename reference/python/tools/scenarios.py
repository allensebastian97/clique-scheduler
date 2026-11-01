"""Shared policy scenarios: the contract every implementation must meet.

Each file in testdata/scenarios/ describes a cluster, an operation and the
result the Python reference produces. Other implementations (pkg/policy in
Go) load the same files and must produce identical results, down to which
node each pod lands on.

    python tools/scenarios.py --update            # recompute every "expect"
    python tools/scenarios.py --random 40         # (re)generate random-*.json, then update

Scenario format:

    {
      "description": "...",
      "reserve": 2,                       # spare nodes per clique
      "gpus_per_node": 4,
      "cliques": {"A": 18, "B": 18},      # nodes A00..A17, B00..B17, in this order
      "unhealthy": ["A03"],
      "labels": {"A01": {"pool": "train"}},
      "taints": {"A00": [{"key": "k", "value": "v", "effect": "NoSchedule"}]},
      "gangs": [{
        "name": "job", "size": 16, "slice_size": 16, "gpus": 4,
        "preemptible": false, "priority": 0,
        "nodes": ["A00", null, ...],      # per rank; null = pending; omitted = all pending
        "node_selector": {}, "tolerations": [{"key": "k", "operator": "Exists"}]
      }],
      "op": "plan_gang" | "plan_defrag" | "fits_ignoring_topology",
      "gang": "job",                      # the gang the operation is about
      "expect": ...                       # written by --update
    }

Pods are keyed "ns/<gang>-<rank>". Expected results:

    plan_gang:              {"assignment": {pod: node}, "stuck_slices": [...], "blocked": bool}
    plan_defrag:            null, or {"victims": [gang keys], "gpus": n, "assignment": {pod: node}}
    fits_ignoring_topology: bool
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nvlink_rack_scheduler.defrag import plan_defrag  # noqa: E402
from nvlink_rack_scheduler.model import GPU, Gang, Node, Pod, Taint, Toleration  # noqa: E402
from nvlink_rack_scheduler.placement import (State, fits_ignoring_topology,  # noqa: E402
                                        plan_gang)

SCENARIOS = Path(__file__).resolve().parents[3] / 'testdata' / 'scenarios'


def build(spec: Dict[str, Any]) -> Tuple[State, List[Gang]]:
    gpus = float(spec.get('gpus_per_node', 4))
    unhealthy = set(spec.get('unhealthy', []))
    nodes = []
    for clique, count in spec['cliques'].items():
        for i in range(count):
            name = f'{clique}{i:02d}'
            nodes.append(Node(
                name=name, clique=clique, allocatable={GPU: gpus},
                healthy=name not in unhealthy,
                labels=spec.get('labels', {}).get(name, {}),
                taints=[Taint(t['key'], t.get('value', ''), t['effect'])
                        for t in spec.get('taints', {}).get(name, [])]))
    gangs = []
    for g in spec['gangs']:
        placed = g.get('nodes') or [None] * g['size']
        tolerations = [Toleration(t.get('key', ''), t.get('operator', 'Equal'),
                                  t.get('value', ''), t.get('effect', ''))
                       for t in g.get('tolerations', [])]
        pods = [Pod(key=f"ns/{g['name']}-{r}", gang=f"ns/{g['name']}", rank=r,
                    requests={GPU: float(g.get('gpus', 4))}, node=placed[r],
                    node_selector=dict(g.get('node_selector', {})),
                    tolerations=tolerations)
                for r in range(g['size'])]
        gangs.append(Gang(key=f"ns/{g['name']}", size=g['size'],
                          slice_size=g.get('slice_size', 16), pods=pods,
                          priority=g.get('priority', 0),
                          preemptible=g.get('preemptible', False)))
    state = State(nodes, {}, spec.get('reserve', 2))
    for gang in gangs:
        for pod in gang.pods:
            if pod.node:
                state.assign(pod, pod.node)
    return state, gangs


def run(spec: Dict[str, Any]) -> Any:
    state, gangs = build(spec)
    target = next(g for g in gangs if g.key == f"ns/{spec['gang']}")
    if spec['op'] == 'plan_gang':
        plan = plan_gang(state, target)
        return {'assignment': dict(sorted(plan.assignment.items())),
                'stuck_slices': plan.stuck_slices, 'blocked': plan.blocked}
    if spec['op'] == 'plan_defrag':
        plan = plan_defrag(state, target, gangs)
        if plan is None:
            return None
        return {'victims': [v.key for v in plan.victims], 'gpus': plan.gpus,
                'assignment': dict(sorted(plan.assignment.items()))}
    if spec['op'] == 'fits_ignoring_topology':
        return fits_ignoring_topology(state, [p for p in target.pods if p.node is None])
    raise ValueError(f"unknown op {spec['op']!r}")


def random_scenario(seed: int) -> Dict[str, Any]:
    """A fragmented cluster: random small gangs scattered over the racks, then a target."""
    rng = random.Random(seed)
    cliques = {chr(ord('A') + i): rng.choice([16, 17, 18]) for i in range(rng.randint(2, 6))}
    names = [f'{c}{i:02d}' for c, n in cliques.items() for i in range(n)]
    unhealthy = sorted(rng.sample(names, rng.randint(0, len(cliques))))
    free = [n for n in names if n not in unhealthy]
    rng.shuffle(free)
    gangs = []
    for i in range(rng.randint(0, 2 * len(cliques))):
        size = rng.randint(1, 6)
        if size > len(free):
            break
        clique = rng.choice(list(cliques))
        local = [n for n in free if n.startswith(clique)][:size]
        if len(local) < size:
            continue
        for n in local:
            free.remove(n)
        gangs.append({'name': f'g{i}', 'size': size, 'slice_size': 16,
                      'preemptible': rng.random() < 0.7, 'priority': rng.choice([0, 0, 5]),
                      'nodes': local})
    op = rng.choice(['plan_gang', 'plan_gang', 'plan_defrag', 'fits_ignoring_topology'])
    slice_size = rng.choice([4, 8, 16])
    size = rng.choice([slice_size // 2, slice_size, 2 * slice_size, 3 * slice_size, 20])
    target: Dict[str, Any] = {'name': 'job', 'size': size, 'slice_size': slice_size,
                              'priority': rng.choice([0, 5])}
    if op == 'plan_gang' and rng.random() < 0.4 and size <= len(free):
        # A running gang with a few members lost (repair).
        clique = rng.choice(list(cliques))
        local = sorted(free, key=lambda n: (not n.startswith(clique), n))[:size]
        nodes: List[Any] = list(local)
        for r in rng.sample(range(size), rng.randint(1, max(1, size // 4))):
            nodes[r] = None
        target['nodes'] = nodes
    gangs.append(target)
    return {'description': f'random fragmented cluster, seed {seed}', 'reserve': 2,
            'gpus_per_node': 4, 'cliques': cliques, 'unhealthy': unhealthy,
            'gangs': gangs, 'op': op, 'gang': 'job'}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--update', action='store_true', help='recompute every "expect"')
    parser.add_argument('--random', type=int, metavar='N',
                        help='regenerate random-*.json with N scenarios (implies --update)')
    args = parser.parse_args()
    if args.random is not None:
        for old in SCENARIOS.glob('random-*.json'):
            old.unlink()
        for seed in range(args.random):
            path = SCENARIOS / f'random-{seed:03d}.json'
            path.write_text(json.dumps(random_scenario(seed), indent=2) + '\n')
    mismatches = 0
    for path in sorted(SCENARIOS.glob('*.json')):
        spec = json.loads(path.read_text())
        result = run(spec)
        if args.update or args.random is not None:
            spec['expect'] = result
            path.write_text(json.dumps(spec, indent=2) + '\n')
        elif spec.get('expect') != result:
            mismatches += 1
            print(f'MISMATCH {path.name}')
    if mismatches:
        sys.exit(1)


if __name__ == '__main__':
    main()
