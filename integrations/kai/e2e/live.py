"""Live test of nvlinkrack: the real KAI scheduler (with the plugin compiled in)
and KAI's binder, running as local processes against a KWOK cluster (real
API server and controllers, fake nodes).

Each scenario runs twice: with KAI's stock configuration and with nvlinkrack
added after topology, so the difference the plugin makes is observed live.

    kwokctl create cluster --name nvlink-rack --runtime binary \\
        --config reference/python/e2e/kwok-stages.yaml
    kwokctl get kubeconfig --name nvlink-rack > /tmp/nvlink-rack.kubeconfig
    (cd integrations/kai && go build -o /tmp/kai-scheduler ./cmd/kai-scheduler)
    (cd $KAI_CHECKOUT && go build -o /tmp/kai-binder ./cmd/binder)
    KUBECONFIG=/tmp/nvlink-rack.kubeconfig .venv/bin/python integrations/kai/e2e/live.py \\
        --kai-crds $KAI_CHECKOUT/deployments/kai-scheduler/crds
"""
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, List, Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

HERE = Path(__file__).resolve().parent
NS = 'nvlink-kai-e2e'
LABEL = 'nvidia.com/gpu.clique'
RACKS = [f'rack-{i}' for i in range(4)]
NODES_PER_RACK = 18
NODE_RESOURCES = {'cpu': '64', 'memory': '512Gi', 'pods': '110', 'nvidia.com/gpu': '4'}
QUEUES = '''
apiVersion: scheduling.run.ai/v2
kind: Queue
metadata: {name: default-parent-queue}
spec:
  resources:
    cpu: {limit: -1, overQuotaWeight: 1, quota: 0}
    gpu: {limit: -1, overQuotaWeight: 1, quota: 0}
    memory: {limit: -1, overQuotaWeight: 1, quota: 0}
---
apiVersion: scheduling.run.ai/v2
kind: Queue
metadata: {name: default-queue}
spec:
  parentQueue: default-parent-queue
  resources:
    cpu: {limit: -1, overQuotaWeight: 1, quota: 0}
    gpu: {limit: -1, overQuotaWeight: 1, quota: 0}
    memory: {limit: -1, overQuotaWeight: 1, quota: 0}
'''

log = logging.getLogger('kai-live')
config.load_kube_config()
core, custom = client.CoreV1Api(), client.CustomObjectsApi()


def kubectl(*args: str, stdin: Optional[str] = None) -> None:
    subprocess.run(['kubectl', *args], input=stdin, text=True, check=True,
                   stdout=subprocess.DEVNULL)


def node_name(rack: str, i: int) -> str:
    return f'{rack}-n{i:02d}'


def rack_of(node: str) -> str:
    return node.rsplit('-', 1)[0]


def setup(crds: str) -> None:
    kubectl('apply', '--server-side', '-f', crds)
    kubectl('wait', '--for=condition=established', '--timeout=60s', 'crd', '--all')
    for ns in (NS, 'kai-scheduler'):
        try:
            core.create_namespace({'metadata': {'name': ns}})
        except ApiException as e:
            if e.status != 409:
                raise
    kubectl('apply', '-f', '-', stdin=QUEUES)
    kubectl('apply', '-f', str(HERE.parent / 'deploy' / 'topology.yaml'))
    existing = {n.metadata.name: n for n in core.list_node().items}
    for rack in RACKS:
        for i in range(NODES_PER_RACK):
            name = node_name(rack, i)
            labels = {'type': 'kwok', 'kubernetes.io/hostname': name, LABEL: rack}
            if name in existing:
                core.patch_node(name, {'metadata': {'labels': labels},
                                       'spec': {'unschedulable': False}})
                continue
            core.create_node({
                'apiVersion': 'v1', 'kind': 'Node',
                'metadata': {'name': name, 'labels': labels,
                             'annotations': {'kwok.x-k8s.io/node': 'fake',
                                             'node.alpha.kubernetes.io/ttl': '0'}},
                'status': {'allocatable': NODE_RESOURCES, 'capacity': NODE_RESOURCES},
            })


class Kai:
    """KAI scheduler + binder as local processes."""

    def __init__(self, args: argparse.Namespace, mode: str, logdir: Path) -> None:
        conf = HERE / f'scheduler-{mode}.yaml'
        env = dict(os.environ)
        self.logs = [logdir / f'{mode}-scheduler.log', logdir / f'{mode}-binder.log']
        self.procs = [
            subprocess.Popen([args.scheduler_bin, f'--scheduler-conf={conf}',
                              '--schedule-period=1s', '--listen-address=:18090',
                              f'--v={args.verbosity}'],
                             env=env, stdout=open(self.logs[0], 'w'), stderr=subprocess.STDOUT),
            subprocess.Popen([args.binder_bin, '--metrics-bind-address=:18091',
                              '--health-probe-bind-address=:18092'],
                             env=env, stdout=open(self.logs[1], 'w'), stderr=subprocess.STDOUT),
        ]

    def check_alive(self) -> None:
        for proc, path in zip(self.procs, self.logs):
            if proc.poll() is not None:
                raise RuntimeError(f'{path.name} exited ({proc.returncode}):\n'
                                   + path.read_text()[-3000:])

    def stop(self) -> None:
        for proc in self.procs:
            proc.terminate()
        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def create_pod_group(name: str, size: int, segments: Optional[List[int]] = None) -> None:
    spec: Dict = {'queue': 'default-queue', 'minMember': size}
    if segments:
        spec['subGroups'] = [{'name': 'worker', 'minSubGroup': len(segments)}] + [
            {'name': f'worker-{i}', 'parent': 'worker', 'minMember': n,
             'topologyConstraint': {'topology': 'nvlink', 'requiredTopologyLevel': LABEL}}
            for i, n in enumerate(segments)]
    custom.create_namespaced_custom_object('scheduling.run.ai', 'v2alpha2', NS, 'podgroups', {
        'apiVersion': 'scheduling.run.ai/v2alpha2', 'kind': 'PodGroup',
        'metadata': {'name': name}, 'spec': spec})


def create_pod(group: str, rank: int, subgroup: Optional[str] = None,
               node_selector: Optional[Dict[str, str]] = None) -> None:
    labels = {'kai.scheduler/queue': 'default-queue', 'group': group}
    if subgroup:
        labels['kai.scheduler/subgroup-name'] = subgroup
    core.create_namespaced_pod(NS, {
        'metadata': {'name': f'{group}-{rank}', 'labels': labels,
                     'annotations': {'pod-group-name': group}},
        'spec': {'schedulerName': 'kai-scheduler', 'nodeSelector': node_selector or {},
                 'containers': [{'name': 'main', 'image': 'fake', 'resources': {
                     'limits': {'nvidia.com/gpu': '4'}}}]},
    })


def segmented_job(name: str, segment_sizes: List[int]) -> None:
    create_pod_group(name, sum(segment_sizes), segment_sizes)
    rank = 0
    for s, size in enumerate(segment_sizes):
        for _ in range(size):
            create_pod(name, rank, subgroup=f'worker-{s}')
            rank += 1


def plain_job(name: str, size: int, node_selector: Optional[Dict[str, str]] = None) -> None:
    create_pod_group(name, size)
    for rank in range(size):
        create_pod(name, rank, node_selector=node_selector)


def placement(group: str) -> Dict[int, str]:
    pods = core.list_namespaced_pod(NS, label_selector=f'group={group}').items
    return {int(p.metadata.name.rsplit('-', 1)[1]): p.spec.node_name
            for p in pods if p.spec.node_name and not p.metadata.deletion_timestamp}


def racks(group: str, ranks: Optional[range] = None) -> Counter:
    return Counter(rack_of(n) for r, n in placement(group).items()
                   if ranks is None or r in ranks)


def until(kai: Kai, done: Callable[[], bool], what: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        kai.check_alive()
        if done():
            return
        time.sleep(0.5)
    raise AssertionError(f'timed out waiting for: {what}')


def settle(kai: Kai, seconds: float = 10) -> None:
    """Let the scheduler run several cycles (period 1s) before checking a negative."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        kai.check_alive()
        time.sleep(0.5)


def cleanup(kai: Kai) -> None:
    core.delete_collection_namespaced_pod(NS, grace_period_seconds=0)
    for group in custom.list_namespaced_custom_object(
            'scheduling.run.ai', 'v2alpha2', NS, 'podgroups')['items']:
        custom.delete_namespaced_custom_object('scheduling.run.ai', 'v2alpha2', NS, 'podgroups',
                                               group['metadata']['name'])
    until(kai, lambda: not core.list_namespaced_pod(NS).items, 'cleanup', timeout=90)
    custom.delete_collection_namespaced_custom_object('scheduling.run.ai', 'v1alpha2', NS,
                                                      'bindrequests')


def idle_nodes(rack: str) -> List[str]:
    used = {p.spec.node_name for p in core.list_pod_for_all_namespaces().items
            if p.spec.node_name and p.status.phase not in ('Succeeded', 'Failed')}
    return [node_name(rack, i) for i in range(NODES_PER_RACK)
            if node_name(rack, i) not in used]


def scenario_full_rack_segment(kai: Kai) -> Dict:
    segmented_job('full', [18])
    plain_job('canary', 1)  # proves the scheduler is running either way
    until(kai, lambda: len(placement('canary')) == 1, 'canary bound')
    settle(kai)
    return {'placed': len(placement('full')), 'racks': dict(racks('full'))}


def scenario_small_job_beside_segments(kai: Kai) -> Dict:
    segmented_job('train', [16, 16])
    until(kai, lambda: len(placement('train')) == 32, 'train bound')
    seg0, seg1 = racks('train', range(16)), racks('train', range(16, 32))
    plain_job('small', 2)
    until(kai, lambda: len(placement('small')) == 2, 'small bound')
    train_racks = set(seg0) | set(seg1)
    return {'segment racks': [dict(seg0), dict(seg1)], 'small racks': dict(racks('small')),
            'small on a train rack': bool(set(racks('small')) & train_racks),
            'idle left in train racks': {r: len(idle_nodes(r)) for r in sorted(train_racks)}}


def scenario_replacement_on_spare(kai: Kai) -> Dict:
    segmented_job('train', [16])
    until(kai, lambda: len(placement('train')) == 16, 'train bound')
    (home,) = racks('train')
    freed = placement('train')[5]
    core.delete_namespaced_pod('train-5', NS, grace_period_seconds=0)
    until(kai, lambda: 5 not in placement('train') and not [
        p for p in core.list_namespaced_pod(NS, label_selector='group=train').items
        if p.metadata.name == 'train-5'], 'train-5 gone')
    # Another job takes the freed node, leaving only the rack's two spares idle.
    plain_job('neighbour', 1, node_selector={'kubernetes.io/hostname': freed})
    until(kai, lambda: placement('neighbour').get(0) == freed, 'neighbour on the freed node')
    spares = idle_nodes(home)
    # A new job pinned to the rack would have to take a spare.
    plain_job('probe', 1, node_selector={LABEL: home})
    settle(kai)
    probe = placement('probe').get(0)
    # Now the replacement for rank 5.
    create_pod('train', 5, subgroup='worker-0')
    until(kai, lambda: 5 in placement('train'), 'replacement bound', timeout=30)
    replacement = placement('train')[5]
    return {'home rack': home, 'spares before': spares, 'probe placed on': probe,
            'replacement on': replacement, 'replacement in home rack': rack_of(replacement) == home}


SCENARIOS = [scenario_full_rack_segment, scenario_small_job_beside_segments,
             scenario_replacement_on_spare]


def check(mode: str, name: str, got: Dict) -> List[str]:
    """Expected behaviour per mode; returns failures."""
    fails = []

    def expect(cond: bool, what: str) -> None:
        if not cond:
            fails.append(f'{mode}/{name}: expected {what}; got {got}')

    if name == 'scenario_full_rack_segment':
        if mode == 'stock':
            expect(got['placed'] == 18 and len(got['racks']) == 1, 'all 18 on one rack')
        else:
            expect(got['placed'] == 0, 'the 18-node segment to wait')
    elif name == 'scenario_small_job_beside_segments':
        expect(all(len(s) == 1 for s in got['segment racks'])
               and got['segment racks'][0] != got['segment racks'][1],
               'each segment on its own rack')
        if mode == 'stock':
            expect(got['small on a train rack'], "the small job on a segment rack's spares")
        else:
            expect(not got['small on a train rack'], 'the small job on another rack')
            expect(all(v == 2 for v in got['idle left in train racks'].values()),
                   '2 spares left in each segment rack')
    elif name == 'scenario_replacement_on_spare':
        expect(got['replacement in home rack'], 'the replacement in its home rack')
        if mode == 'stock':
            expect(got['probe placed on'] in got['spares before'], 'the probe to take a spare')
        else:
            expect(got['probe placed on'] is None, 'the probe to be kept off the spares')
            expect(got['replacement on'] in got['spares before'], 'the replacement on a spare')
    return fails


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--kai-crds', required=True)
    parser.add_argument('--scheduler-bin', default='/tmp/kai-scheduler')
    parser.add_argument('--binder-bin', default='/tmp/kai-binder')
    parser.add_argument('--mode', choices=['stock', 'nvlinkrack'], action='append',
                        help='run only this configuration (repeatable)')
    parser.add_argument('--only', help='run only scenarios whose name contains this')
    parser.add_argument('--verbosity', type=int, default=3, help='scheduler log level')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger('kubernetes').setLevel(logging.WARNING)
    logdir = Path(tempfile.mkdtemp(prefix='kai-live-'))
    log.info('process logs in %s', logdir)

    setup(args.kai_crds)
    failures = []
    for mode in args.mode or ['stock', 'nvlinkrack']:
        kai = Kai(args, mode, logdir)
        try:
            cleanup(kai)
            for scenario in [s for s in SCENARIOS if not args.only or args.only in s.__name__]:
                got = scenario(kai)
                log.info('%-10s %-36s %s', mode, scenario.__name__, got)
                failures += check(mode, scenario.__name__, got)
                cleanup(kai)
        finally:
            kai.stop()
    for failure in failures:
        log.error(failure)
    log.info('%s', 'all live checks passed' if not failures else f'{len(failures)} failure(s)')
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
