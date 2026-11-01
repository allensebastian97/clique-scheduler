"""End-to-end scenarios against a KWOK cluster: real API server, controller
manager and Job controller; fake nodes whose pods "run" as soon as they bind.

    kwokctl create cluster --name nvlink-rack --runtime binary --config e2e/kwok-stages.yaml
    kwokctl get kubeconfig --name nvlink-rack > /tmp/nvlink-rack.kubeconfig
    KUBECONFIG=/tmp/nvlink-rack.kubeconfig .venv/bin/python e2e/run.py

Builds 4 racks x 18 nodes x 4 GPUs (288 fake GPUs), then checks slicing,
spare protection, same-rack recovery, scheduler restarts and defrag.
"""
from __future__ import annotations

import logging
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kubernetes import client, config  # noqa: E402

from nvlink_rack_scheduler.k8s import KubeApi  # noqa: E402
from nvlink_rack_scheduler.scheduler import Config, Scheduler  # noqa: E402

NS = 'nvlink-rack-e2e'
RACKS = [f'rack-{i}' for i in range(4)]
NODES_PER_RACK = 18
NODE_RESOURCES = {'cpu': '64', 'memory': '512Gi', 'pods': '110', 'nvidia.com/gpu': '4'}

log = logging.getLogger('e2e')
config.load_kube_config()
core, batch = client.CoreV1Api(), client.BatchV1Api()


def node_name(rack: str, i: int) -> str:
    return f'{rack}-n{i:02d}'


def rack_of(node: str) -> str:
    return node.rsplit('-', 1)[0]


def create_nodes() -> None:
    existing = {n.metadata.name for n in core.list_node().items}
    for rack in RACKS:
        for i in range(NODES_PER_RACK):
            name = node_name(rack, i)
            if name in existing:
                continue
            core.create_node({
                'apiVersion': 'v1', 'kind': 'Node',
                'metadata': {
                    'name': name,
                    'annotations': {'kwok.x-k8s.io/node': 'fake',
                                    'node.alpha.kubernetes.io/ttl': '0'},
                    'labels': {'type': 'kwok', 'kubernetes.io/hostname': name,
                               'nvidia.com/gpu.clique': f'{rack}-uuid.1'},
                },
                'status': {'allocatable': NODE_RESOURCES, 'capacity': NODE_RESOURCES},
            })


def set_cordoned(nodes: List[str], cordoned: bool) -> None:
    for name in nodes:
        core.patch_node(name, {'spec': {'unschedulable': cordoned}})


def create_job(name: str, size: int, preemptible: bool = False) -> None:
    annotations = {'nvlink-rack.io/gang-size': str(size)}
    if preemptible:
        annotations['nvlink-rack.io/preemptible'] = 'true'
    batch.create_namespaced_job(NS, {
        'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name},
        'spec': {
            'completionMode': 'Indexed', 'completions': size, 'parallelism': size,
            'backoffLimit': 1000,
            # Evictions (defrag) aren't failures: recreate without backoff.
            'podFailurePolicy': {'rules': [{
                'action': 'Ignore',
                'onPodConditions': [{'type': 'DisruptionTarget'}]}]},
            'template': {
                'metadata': {'labels': {'nvlink-rack.io/gang': name},
                             'annotations': annotations},
                'spec': {
                    'schedulerName': 'nvlink-rack-scheduler', 'restartPolicy': 'Never',
                    'containers': [{'name': 'trainer', 'image': 'fake',
                                    'resources': {'limits': {'nvidia.com/gpu': '4'},
                                                  'requests': {'cpu': '8',
                                                               'memory': '64Gi'}}}],
                },
            },
        },
    })


def delete_jobs(names: List[str]) -> None:
    for name in names:
        batch.delete_namespaced_job(name, NS, propagation_policy='Background')


def live_pods(job: str) -> List[client.V1Pod]:
    pods = core.list_namespaced_pod(
        NS, label_selector=f'batch.kubernetes.io/job-name={job}').items
    return [p for p in pods if not p.metadata.deletion_timestamp
            and p.status.phase not in ('Succeeded', 'Failed')]


def placement(job: str) -> Dict[int, str]:
    """rank -> node for the job's running pods."""
    return {int(p.metadata.annotations['batch.kubernetes.io/job-completion-index']):
            p.spec.node_name
            for p in live_pods(job) if p.spec.node_name and p.status.phase == 'Running'}


def racks(job: str) -> Counter:
    return Counter(rack_of(n) for n in placement(job).values())


def until(scheduler: Scheduler, done: Callable[[], bool], what: str,
          timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        scheduler.run_once()
        if done():
            log.info('ok: %s', what)
            return
        time.sleep(0.5)
    raise AssertionError(f'timed out waiting for: {what}')


def check(condition: bool, what: str) -> None:
    if not condition:
        raise AssertionError(what)
    log.info('ok: %s', what)


def running(job: str, size: int) -> Callable[[], bool]:
    return lambda: len(placement(job)) == size


def no_pods() -> bool:
    return not core.list_namespaced_pod(NS).items


def scenario_placement_and_recovery() -> None:
    scheduler = Scheduler(KubeApi(), Config(defrag='off'))

    create_job('train', 32)
    until(scheduler, running('train', 32), '32-node job is running')
    where = placement('train')
    first = {rack_of(where[r]) for r in range(16)}
    second = {rack_of(where[r]) for r in range(16, 32)}
    check(len(first) == len(second) == 1 and first != second,
          f'ranks 0-15 on one rack, 16-31 on another: {dict(racks("train"))}')

    create_job('small', 8)
    until(scheduler, running('small', 8), '8-node job is running')
    check(len(racks('small')) == 1 and not set(racks('small')) & set(racks('train')),
          f'8-node job on one rack, not in the 16-node racks\' spares: '
          f'{dict(racks("small"))}')

    # A node fails: the health checker cordons it and its pod dies. The Job
    # controller recreates that index. A fresh scheduler (as after a restart)
    # must put it back in the same rack.
    dead = where[5]
    set_cordoned([dead], True)
    old = next(p for p in live_pods('train') if p.spec.node_name == dead)
    core.delete_namespaced_pod(old.metadata.name, NS, grace_period_seconds=0)
    restarted = Scheduler(KubeApi(), Config(defrag='off'))
    until(restarted, lambda: placement('train').get(5) not in (None, dead),
          'rank 5 replaced after its node failed')
    check(rack_of(placement('train')[5]) == rack_of(dead),
          f'replacement {placement("train")[5]} is in {rack_of(dead)}, the failed '
          'node\'s rack, using a spare')

    delete_jobs(['train', 'small'])
    until(scheduler, no_pods, 'cleanup')
    set_cordoned([dead], False)


def scenario_defrag() -> None:
    # Two racks only: take the others out of service.
    offline = [node_name(r, i) for r in RACKS[2:] for i in range(NODES_PER_RACK)]
    set_cordoned(offline, True)
    scheduler = Scheduler(KubeApi(), Config(defrag='on', cooldown=60))

    # Eight 4-node jobs pack four to a rack; then all but one per rack finish.
    for i in range(8):
        create_job(f'frag-{i}', 4, preemptible=True)
        until(scheduler, running(f'frag-{i}', 4), f'frag-{i} is running')
    keep: Dict[str, str] = {}
    for i in range(8):
        keep.setdefault(next(iter(racks(f'frag-{i}'))), f'frag-{i}')
    check(len(keep) == 2, f'small jobs packed onto both racks: {keep}')
    finished = [f'frag-{i}' for i in range(8) if f'frag-{i}' not in keep.values()]
    delete_jobs(finished)
    until(scheduler, lambda: all(not live_pods(j) for j in finished),
          'all but one small job per rack finished')

    # 16 nodes + 2 spares fit nowhere, though 28 nodes are idle in total.
    create_job('big', 16)
    until(scheduler, running('big', 16), '16-node job placed after defrag',
          timeout=120)
    check(len(racks('big')) == 1, f'16-node job on one rack: {dict(racks("big"))}')
    kept = list(keep.values())
    until(scheduler, lambda: all(len(placement(j)) == 4 for j in kept),
          'evicted job came back')
    where = {j: set(racks(j)) for j in kept}
    check(where[kept[0]] == where[kept[1]] and not where[kept[0]] & set(racks('big')),
          f'both small jobs now share the other rack: {where}')

    delete_jobs(kept + ['big'])
    until(scheduler, no_pods, 'cleanup')
    set_cordoned(offline, False)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s: %(message)s')
    logging.getLogger('kubernetes').setLevel(logging.WARNING)
    create_nodes()
    if NS not in {n.metadata.name for n in core.list_namespace().items}:
        core.create_namespace({'metadata': {'name': NS}})
    until(Scheduler(KubeApi(), Config()), lambda: sum(
        1 for n in core.list_node().items
        if any(c.type == 'Ready' and c.status == 'True' for c in n.status.conditions)
    ) >= len(RACKS) * NODES_PER_RACK, 'fake nodes are Ready')
    scenario_placement_and_recovery()
    scenario_defrag()
    log.info('all scenarios passed')


if __name__ == '__main__':
    main()
