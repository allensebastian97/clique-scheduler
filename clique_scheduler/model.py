"""Plain data model shared by the planner and the Kubernetes adapter."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional

GPU = 'nvidia.com/gpu'

Resources = Dict[str, float]


def add(a: Resources, b: Resources) -> Resources:
    out = dict(a)
    for key, value in b.items():
        out[key] = out.get(key, 0.0) + value
    return out


def sub(a: Resources, b: Resources) -> Resources:
    out = dict(a)
    for key, value in b.items():
        out[key] = out.get(key, 0.0) - value
    return out


def fits(request: Resources, free: Resources) -> bool:
    return all(free.get(k, 0.0) >= v for k, v in request.items() if v > 0)


@dataclass
class Taint:
    key: str
    value: str
    effect: str


@dataclass
class Toleration:
    key: str = ''
    operator: str = 'Equal'
    value: str = ''
    effect: str = ''

    def tolerates(self, taint: Taint) -> bool:
        if self.effect and self.effect != taint.effect:
            return False
        if self.operator == 'Exists':
            return not self.key or self.key == taint.key
        return self.key == taint.key and self.value == taint.value


@dataclass
class Node:
    name: str
    clique: str
    allocatable: Resources
    healthy: bool = True
    labels: Dict[str, str] = field(default_factory=dict)
    taints: List[Taint] = field(default_factory=list)


@dataclass
class Pod:
    key: str  # "namespace/name"
    gang: str  # "namespace/gang"
    rank: int
    requests: Resources
    node: Optional[str] = None
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Toleration] = field(default_factory=list)

    def can_run_on(self, node: Node) -> bool:
        if not node.healthy:
            return False
        if any(node.labels.get(k) != v for k, v in self.node_selector.items()):
            return False
        for taint in node.taints:
            if taint.effect not in ('NoSchedule', 'NoExecute'):
                continue
            if not any(t.tolerates(taint) for t in self.tolerations):
                return False
        return True


@dataclass
class Gang:
    key: str
    size: int
    slice_size: int
    pods: List[Pod]
    priority: int = 0
    created: str = ''  # oldest pod's creationTimestamp (RFC 3339)
    preemptible: bool = False

    @property
    def num_slices(self) -> int:
        return math.ceil(self.size / self.slice_size)

    def slice_of(self, pod: Pod) -> int:
        return pod.rank // self.slice_size

    def pods_in_slice(self, index: int) -> List[Pod]:
        return [p for p in self.pods if self.slice_of(p) == index]

    def gpus(self) -> float:
        return sum(p.requests.get(GPU, 0.0) for p in self.pods)
