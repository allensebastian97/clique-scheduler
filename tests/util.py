from __future__ import annotations

from typing import Dict, Iterable, Optional

from clique_scheduler.model import GPU, Gang, Node, Pod
from clique_scheduler.placement import State


def make_state(cliques: Dict[str, int], reserve: int = 2,
               unhealthy: Iterable[str] = ()) -> State:
    """``{'A': 18}`` -> nodes A00..A17 with 4 GPUs each."""
    bad = set(unhealthy)
    nodes = [Node(name=f'{c}{i:02d}', clique=c, allocatable={GPU: 4.0, 'cpu': 64.0},
                  healthy=f'{c}{i:02d}' not in bad)
             for c, count in cliques.items() for i in range(count)]
    return State(nodes, {}, reserve)


def make_gang(name: str, size: int, slice_size: int = 16, gpus: float = 4.0,
              preemptible: bool = False, priority: int = 0) -> Gang:
    pods = [Pod(key=f'ns/{name}-{r}', gang=f'ns/{name}', rank=r, requests={GPU: gpus})
            for r in range(size)]
    return Gang(key=f'ns/{name}', size=size, slice_size=slice_size, pods=pods,
                preemptible=preemptible, priority=priority)


def bind(state: State, gang: Gang, assignment: Dict[str, str]) -> None:
    """Records an assignment on the gang's pods (state already reflects it)."""
    for pod in gang.pods:
        if pod.key in assignment:
            pod.node = assignment[pod.key]


def occupy(state: State, gang: Gang, nodes: Iterable[str]) -> None:
    """Binds ``gang`` to ``nodes`` directly, bypassing the planner."""
    for pod, node in zip(gang.pods, nodes):
        pod.node = node
        state.assign(pod, node)


def cliques_of(state: State, gang: Gang, rank_range: Optional[range] = None):
    pods = gang.pods if rank_range is None else [p for p in gang.pods if p.rank in rank_range]
    return {state.clique_of(p.node) for p in pods}
