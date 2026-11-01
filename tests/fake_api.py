"""An in-memory stand-in for the Kubernetes API, speaking the same JSON shapes."""
from __future__ import annotations

from typing import Dict, List, Optional


def node_json(name: str, clique: Optional[str], gpus: int = 4, ready: bool = True) -> dict:
    labels = {'kubernetes.io/hostname': name}
    if clique:
        labels['nvidia.com/gpu.clique'] = clique
    return {
        'metadata': {'name': name, 'labels': labels},
        'spec': {},
        'status': {
            'allocatable': {'nvidia.com/gpu': str(gpus), 'cpu': '64', 'memory': '512Gi'},
            'conditions': [{'type': 'Ready', 'status': 'True' if ready else 'False'}],
        },
    }


def job_pod(job: str, index: int, size: int, gpus: int = 4, preemptible: bool = False,
            created: str = '2026-10-31T00:00:00Z', namespace: str = 'ns',
            suffix: str = 'x') -> dict:
    """A pod as an Indexed Job would create it."""
    annotations = {'cliquesched.io/gang-size': str(size),
                   'batch.kubernetes.io/job-completion-index': str(index)}
    if preemptible:
        annotations['cliquesched.io/preemptible'] = 'true'
    return {
        'metadata': {'name': f'{job}-{index}-{suffix}', 'namespace': namespace,
                     'labels': {'cliquesched.io/gang': job},
                     'annotations': annotations, 'creationTimestamp': created},
        'spec': {'schedulerName': 'clique-scheduler',
                 'containers': [{'name': 'main', 'resources': {
                     'requests': {'nvidia.com/gpu': str(gpus), 'cpu': '8',
                                  'memory': '64Gi'}}}]},
        'status': {'phase': 'Pending'},
    }


class FakeApi:

    def __init__(self, nodes: List[dict]) -> None:
        self.nodes: Dict[str, dict] = {n['metadata']['name']: n for n in nodes}
        self.pods: Dict[str, dict] = {}
        self.evicted: List[str] = []

    def add_pod(self, pod: dict) -> None:
        meta = pod['metadata']
        self.pods[f"{meta['namespace']}/{meta['name']}"] = pod

    def set_ready(self, node: str, ready: bool) -> None:
        self.nodes[node]['status']['conditions'][0]['status'] = 'True' if ready else 'False'

    def node_of(self, key: str) -> Optional[str]:
        return self.pods[key]['spec'].get('nodeName')

    def list_nodes(self) -> List[dict]:
        return list(self.nodes.values())

    def list_pods(self) -> List[dict]:
        return list(self.pods.values())

    def bind(self, namespace: str, name: str, node: str) -> None:
        pod = self.pods[f'{namespace}/{name}']
        assert not pod['spec'].get('nodeName'), 'already bound'
        pod['spec']['nodeName'] = node
        pod['status']['phase'] = 'Running'

    def evict(self, namespace: str, name: str) -> None:
        # Like the real API: the pod lingers, terminating, until its grace period ends.
        key = f'{namespace}/{name}'
        self.pods[key]['metadata']['deletionTimestamp'] = '2026-10-31T01:00:00Z'
        self.evicted.append(key)

    def finish_terminating(self) -> None:
        self.pods = {k: p for k, p in self.pods.items()
                     if not p['metadata'].get('deletionTimestamp')}
