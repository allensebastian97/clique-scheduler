"""Clique-aware gang placement.

Policy:

* A gang is cut into slices of ``slice_size`` consecutive ranks
  (``rank // slice_size``). Each slice lands entirely inside one NVLink
  clique, so its collectives never leave NVLink. A gang no bigger than
  ``slice_size`` is a single slice and therefore sits on a single rack.
* New slices may not consume a clique's last ``reserve`` idle nodes. Those
  are the clique's spares. A node that has already failed has used up one
  spare, so a rack with two dead nodes still takes a 16-node slice.
* Each slice goes to the clique it fills most tightly (best fit), keeping
  emptier cliques whole for bigger slices.
* A replacement pod for a slice whose other members are still bound goes
  back to that slice's clique and may use the spares.
* First placement of a gang is all-or-nothing; replacement is per slice.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Iterable, List, Optional, Tuple

from clique_scheduler.model import GPU, Gang, Node, Pod, Resources, add, fits, sub

Assignment = Dict[str, str]  # pod key -> node name


class State:
    """Clique inventory plus per-node usage, cheap to copy for what-if planning."""

    def __init__(self, nodes: Iterable[Node], used: Dict[str, Resources],
                 reserve: int):
        self.nodes = {n.name: n for n in nodes}
        self.used = {name: dict(used.get(name, {})) for name in self.nodes}
        self.reserve = reserve
        # Nodes held for another gang (see Scheduler reservations): not usable
        # and not counted as spares.
        self.blocked: FrozenSet[str] = frozenset()
        self.cliques: Dict[str, List[Node]] = {}
        for node in sorted(self.nodes.values(), key=lambda n: n.name):
            self.cliques.setdefault(node.clique, []).append(node)

    def copy(self) -> State:
        new = State.__new__(State)
        new.nodes, new.cliques, new.reserve = self.nodes, self.cliques, self.reserve
        new.blocked = self.blocked
        # Shallow is enough: per-node usage is replaced on change, never mutated.
        new.used = dict(self.used)
        return new

    def free(self, node: str) -> Resources:
        return sub(self.nodes[node].allocatable, self.used[node])

    def usable(self, node: Node) -> bool:
        return node.healthy and node.name not in self.blocked

    def is_idle(self, node: str) -> bool:
        return self.used[node].get(GPU, 0.0) <= 0

    def idle_count(self, clique: str) -> int:
        return sum(1 for n in self.cliques[clique]
                   if self.usable(n) and self.is_idle(n.name))

    def spares_required(self, clique: str) -> int:
        failed = sum(1 for n in self.cliques[clique] if not n.healthy)
        return max(0, self.reserve - failed)

    def clique_of(self, node: Optional[str]) -> Optional[str]:
        found = self.nodes.get(node) if node else None
        return found.clique if found else None

    def assign(self, pod: Pod, node: str) -> None:
        self.used[node] = add(self.used[node], pod.requests)

    def release(self, pod: Pod) -> None:
        if pod.node in self.used:
            self.used[pod.node] = sub(self.used[pod.node], pod.requests)


def place_in_clique(state: State, clique: str, pods: List[Pod],
                    use_spares: bool) -> Optional[Assignment]:
    """Fits ``pods`` into ``clique``. Mutates ``state`` only on success."""
    nodes = state.cliques[clique]
    free = {n.name: state.free(n.name) for n in nodes if state.usable(n)}
    if sum(f.get(GPU, 0.0) for f in free.values()) < sum(
            p.requests.get(GPU, 0.0) for p in pods):
        return None
    idle = {name for name in free if state.is_idle(name)}
    idle_before = len(idle)
    eligible_by_constraints: Dict[tuple, List[str]] = {}
    out: Assignment = {}
    for pod in sorted(pods, key=lambda p: p.rank):
        constraints = (tuple(sorted(pod.node_selector.items())),
                       tuple((t.key, t.operator, t.value, t.effect) for t in pod.tolerations))
        eligible = eligible_by_constraints.get(constraints)
        if eligible is None:
            eligible = [n.name for n in nodes if n.name in free and pod.can_run_on(n)]
            eligible_by_constraints[constraints] = eligible
        # Fill partly used nodes before touching idle ones.
        best = min(((name in idle, free[name].get(GPU, 0.0), name)
                    for name in eligible if fits(pod.requests, free[name])),
                   default=None)
        if best is None:
            return None
        name = best[2]
        free[name] = sub(free[name], pod.requests)
        if pod.requests.get(GPU, 0.0) > 0:
            idle.discard(name)
        out[pod.key] = name
    if not use_spares:
        if len(idle) < state.spares_required(clique) and len(idle) < idle_before:
            return None
    for pod in pods:
        state.assign(pod, out[pod.key])
    return out


def best_clique(state: State, pods: List[Pod],
                use_spares: bool) -> Optional[Tuple[str, Assignment]]:
    """Places ``pods`` in the clique they fill most tightly. Mutates ``state`` on success."""
    best = None
    for clique in sorted(state.cliques):
        trial = state.copy()
        placed = place_in_clique(trial, clique, pods, use_spares)
        if placed is None:
            continue
        leftover_gpus = sum(trial.free(n.name).get(GPU, 0.0)
                            for n in state.cliques[clique] if state.usable(n))
        score = (trial.idle_count(clique), leftover_gpus, clique)
        if best is None or score < best[0]:
            best = (score, clique, placed, trial)
    if best is None:
        return None
    state.used = best[3].used
    return best[1], best[2]


def home_clique(state: State, members: List[Pod]) -> Optional[str]:
    """The clique a slice already occupies, judged by its bound members."""
    cliques = Counter(c for c in (state.clique_of(p.node) for p in members) if c)
    return cliques.most_common(1)[0][0] if cliques else None


@dataclass
class GangPlan:
    assignment: Assignment = field(default_factory=dict)
    # Slices whose replacement pods don't fit in the clique they occupy.
    stuck_slices: List[int] = field(default_factory=list)
    # First placement found no room; nothing was assigned.
    blocked: bool = False


def plan_gang(state: State, gang: Gang) -> GangPlan:
    """Plans the gang's unbound pods. Mutates ``state`` to reflect the plan."""
    pending = [p for p in gang.pods if p.node is None]
    if not pending:
        return GangPlan()
    initial = len(pending) == len(gang.pods)
    waiting_by_slice: Dict[int, List[Pod]] = {}
    for pod in pending:
        waiting_by_slice.setdefault(gang.slice_of(pod), []).append(pod)

    plan = GangPlan()
    trial = state.copy()
    for index in sorted(waiting_by_slice, key=lambda i: (-len(waiting_by_slice[i]), i)):
        waiting = waiting_by_slice[index]
        home = home_clique(trial, gang.pods_in_slice(index))
        if home is not None:
            placed = place_in_clique(trial, home, waiting, use_spares=True)
            if placed is None:
                plan.stuck_slices.append(index)
                continue
        else:
            found = best_clique(trial, waiting, use_spares=False)
            if found is None:
                if initial:
                    return GangPlan(blocked=True)
                continue  # A whole slice is waiting for room; nothing to repair yet.
            placed = found[1]
        plan.assignment.update(placed)
    state.used = trial.used
    return plan


def fits_ignoring_topology(state: State, pods: List[Pod]) -> bool:
    """Whether ``pods`` would fit if NVLink domains didn't matter."""
    trial = state.copy()
    for pod in pods:
        node = next((n for n in trial.nodes.values()
                     if trial.usable(n) and pod.can_run_on(n)
                     and fits(pod.requests, trial.free(n.name))),
                    None)
        if node is None:
            return False
        trial.assign(pod, node.name)
    return True
