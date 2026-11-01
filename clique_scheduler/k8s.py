"""Translates Kubernetes JSON objects into the planner's model, and talks to the API."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from clique_scheduler.model import (Gang, Node, Pod, Resources, Taint,
                                    Toleration, add)
from clique_scheduler.placement import State

log = logging.getLogger(__name__)

GANG_LABEL = 'cliquesched.io/gang'
GANG_SIZE = 'cliquesched.io/gang-size'
SLICE_SIZE = 'cliquesched.io/slice-size'
RANK = 'cliquesched.io/rank'
PREEMPTIBLE = 'cliquesched.io/preemptible'
# Where a pod's rank can come from, in order: explicit, Indexed Job, StatefulSet.
RANK_SOURCES = (('annotations', RANK),
                ('annotations', 'batch.kubernetes.io/job-completion-index'),
                ('labels', 'apps.kubernetes.io/pod-index'))

Json = Dict[str, Any]

_SUFFIX = {'Ki': 2**10, 'Mi': 2**20, 'Gi': 2**30, 'Ti': 2**40, 'Pi': 2**50,
           'Ei': 2**60, 'n': 1e-9, 'u': 1e-6, 'm': 1e-3, '': 1, 'k': 1e3,
           'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18}
_QUANTITY = re.compile(
    r'^([+-]?[0-9.]+(?:[eE][+-]?[0-9]+)?)(Ki|Mi|Gi|Ti|Pi|Ei|n|u|m|k|M|G|T|P|E)?$')


def parse_quantity(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    match = _QUANTITY.match(str(value).strip())
    if not match:
        raise ValueError(f'invalid quantity {value!r}')
    return float(match.group(1)) * _SUFFIX[match.group(2) or '']


def _meta(obj: Json) -> Json:
    return obj.get('metadata') or {}


def _requests(container: Json) -> Resources:
    requests = (container.get('resources') or {}).get('requests') or {}
    return {k: parse_quantity(v) for k, v in requests.items()}


def pod_requests(pod: Json) -> Resources:
    spec = pod.get('spec') or {}
    total: Resources = {}
    for container in spec.get('containers') or []:
        total = add(total, _requests(container))
    for container in spec.get('initContainers') or []:
        for key, value in _requests(container).items():
            total[key] = max(total.get(key, 0.0), value)
    return total


def is_active(pod: Json) -> bool:
    return (pod.get('status') or {}).get('phase') not in ('Succeeded', 'Failed')


def node_from_json(obj: Json, clique_label: str) -> Optional[Node]:
    labels = _meta(obj).get('labels') or {}
    clique = labels.get(clique_label)
    if not clique:
        return None
    spec, status = obj.get('spec') or {}, obj.get('status') or {}
    ready = any(c.get('type') == 'Ready' and c.get('status') == 'True'
                for c in status.get('conditions') or [])
    return Node(
        name=_meta(obj)['name'],
        clique=clique,
        allocatable={k: parse_quantity(v)
                     for k, v in (status.get('allocatable') or {}).items()},
        healthy=ready and not spec.get('unschedulable', False),
        labels=labels,
        taints=[Taint(t.get('key', ''), t.get('value', ''), t.get('effect', ''))
                for t in spec.get('taints') or []],
    )


def build_state(nodes: List[Json], pods: List[Json], clique_label: str,
                reserve: int) -> State:
    clique_nodes = [n for n in (node_from_json(o, clique_label) for o in nodes) if n]
    used: Dict[str, Resources] = defaultdict(dict)
    for pod in pods:
        node = (pod.get('spec') or {}).get('nodeName')
        if node and is_active(pod):
            used[node] = add(used[node], pod_requests(pod))
    return State(clique_nodes, used, reserve)


def _rank(pod: Json) -> Optional[int]:
    meta = _meta(pod)
    for field, key in RANK_SOURCES:
        value = (meta.get(field) or {}).get(key)
        if value is not None:
            return int(value)
    return None


def _pod_from_json(obj: Json, gang_key: str, rank: int) -> Pod:
    spec = obj.get('spec') or {}
    return Pod(
        key=f"{_meta(obj)['namespace']}/{_meta(obj)['name']}",
        gang=gang_key,
        rank=rank,
        requests=pod_requests(obj),
        node=spec.get('nodeName'),
        node_selector=spec.get('nodeSelector') or {},
        tolerations=[Toleration(t.get('key', ''), t.get('operator', 'Equal'),
                                t.get('value', ''), t.get('effect', ''))
                     for t in spec.get('tolerations') or []],
    )


def build_gangs(pods: List[Json], scheduler_name: str,
                default_slice_size: int) -> Tuple[List[Gang], List[str]]:
    """Groups this scheduler's live pods into gangs. Returns (gangs, problems)."""
    grouped: Dict[str, List[Json]] = defaultdict(list)
    for pod in pods:
        meta = _meta(pod)
        gang = (meta.get('labels') or {}).get(GANG_LABEL)
        # Terminating pods still hold resources (counted in build_state) but
        # are no longer members; their replacement may already exist.
        if (gang and (pod.get('spec') or {}).get('schedulerName') == scheduler_name
                and is_active(pod) and not meta.get('deletionTimestamp')):
            grouped[f"{meta['namespace']}/{gang}"].append(pod)

    gangs, problems = [], []
    for key, members in sorted(grouped.items()):
        annotations = _meta(members[0]).get('annotations') or {}
        try:
            size = int(annotations[GANG_SIZE])
            slice_size = int(annotations.get(SLICE_SIZE, default_slice_size))
            ranks = [_rank(p) for p in members]
        except (KeyError, ValueError) as e:
            problems.append(f'gang {key}: bad or missing annotation ({e})')
            continue
        if size < 1 or slice_size < 1:
            problems.append(f'gang {key}: gang-size and slice-size must be positive')
            continue
        if None in ranks:
            problems.append(f'gang {key}: every pod needs a rank ({RANK} annotation, '
                            'an Indexed Job, or a StatefulSet)')
            continue
        if len(set(ranks)) != len(ranks) or not all(0 <= r < size for r in ranks):
            problems.append(f'gang {key}: ranks must be unique and below {size}')
            continue
        gangs.append(Gang(
            key=key,
            size=size,
            slice_size=slice_size,
            pods=[_pod_from_json(p, key, r) for p, r in zip(members, ranks)],
            priority=max((p.get('spec') or {}).get('priority') or 0 for p in members),
            created=min(str(_meta(p).get('creationTimestamp') or '') for p in members),
            preemptible=any((_meta(p).get('annotations') or {}).get(PREEMPTIBLE) == 'true'
                            for p in members),
        ))
    return gangs, problems


class KubeApi:
    """The handful of API calls the scheduler needs."""

    def __init__(self) -> None:
        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self._client = client
        self._core = client.CoreV1Api()
        self._to_json = client.ApiClient().sanitize_for_serialization

    def list_nodes(self) -> List[Json]:
        return [self._to_json(n) for n in self._core.list_node().items]

    def list_pods(self) -> List[Json]:
        pods = self._core.list_pod_for_all_namespaces(
            field_selector='status.phase!=Succeeded,status.phase!=Failed')
        return [self._to_json(p) for p in pods.items]

    def bind(self, namespace: str, name: str, node: str) -> None:
        c = self._client
        body = c.V1Binding(metadata=c.V1ObjectMeta(name=name),
                           target=c.V1ObjectReference(api_version='v1', kind='Node',
                                                      name=node))
        # The client can't deserialize the Binding response; skip decoding it.
        self._core.create_namespaced_binding(namespace, body, _preload_content=False)

    def evict(self, namespace: str, name: str) -> None:
        c = self._client
        body = c.V1Eviction(metadata=c.V1ObjectMeta(name=name, namespace=namespace))
        self._core.create_namespaced_pod_eviction(name, namespace, body)
