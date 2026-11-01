"""Topology defragmentation: free a clique for a gang blocked by fragmentation.

When a gang can't be placed even though enough GPUs are free in total, look
for the cheapest set of restart-safe (``preemptible``) gangs to evict so the
blocked gang fits. A plan is only accepted if every evicted gang can land
again elsewhere afterwards. Otherwise evicting it would just move the
blockage, and two gangs could keep evicting each other.

Cost is GPUs evicted. The per-clique search is greedy (smallest gangs first,
then drop any victim that turns out unnecessary), not an exact optimum.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List, Optional, Tuple

from clique_scheduler.model import Gang, Pod
from clique_scheduler.placement import (Assignment, State, best_clique,
                                        place_in_clique, plan_gang)


@dataclass
class DefragPlan:
    victims: List[Gang]
    gpus: float  # GPUs held by the victims
    assignment: Assignment  # Where the blocked gang lands once they're gone.


def plan_defrag(state: State, blocked: Gang,
                gangs: List[Gang]) -> Optional[DefragPlan]:
    candidates = [g for g in gangs
                  if g.preemptible and g.key != blocked.key
                  and g.priority <= blocked.priority
                  and len(g.pods) == g.size and all(p.node for p in g.pods)]
    trial = state.copy()
    victims: List[Gang] = []
    assignment: Assignment = {}
    pending = [p for p in blocked.pods if p.node is None]
    slices = sorted({blocked.slice_of(p) for p in pending},
                    key=lambda i: (-len(blocked.pods_in_slice(i)), i))
    for index in slices:
        pods = [p for p in blocked.pods_in_slice(index) if p.node is None]
        found = best_clique(trial, pods, use_spares=False)
        if found is not None:
            assignment.update(found[1])
            continue
        remaining = [g for g in candidates if all(g is not v for v in victims)]
        option = _cheapest_clearing(trial, pods, remaining)
        if option is None:
            return None
        evicted, trial, placed = option
        victims.extend(evicted)
        assignment.update(placed)

    check = trial.copy()
    for victim in victims:
        fresh = replace(victim, pods=[replace(p, node=None) for p in victim.pods])
        if plan_gang(check, fresh).blocked:
            return None
    return DefragPlan(victims=victims, gpus=sum(v.gpus() for v in victims),
                      assignment=assignment)


def _without(state: State, gangs: List[Gang]) -> State:
    trial = state.copy()
    for gang in gangs:
        for pod in gang.pods:
            trial.release(pod)
    return trial


def _try_place(state: State, clique: str,
               pods: List[Pod]) -> Optional[Tuple[State, Assignment]]:
    trial = state.copy()
    placed = place_in_clique(trial, clique, pods, use_spares=False)
    return None if placed is None else (trial, placed)


def _cheapest_clearing(state: State, pods: List[Pod], candidates: List[Gang]
                       ) -> Optional[Tuple[List[Gang], State, Assignment]]:
    best = None
    for clique in sorted(state.cliques):
        local = sorted((g for g in candidates
                        if any(state.clique_of(p.node) == clique for p in g.pods)),
                       key=lambda g: (g.gpus(), g.key))
        evicted: List[Gang] = []
        result = None
        for gang in local:
            evicted.append(gang)
            result = _try_place(_without(state, evicted), clique, pods)
            if result is not None:
                break
        if result is None:
            continue
        # Drop victims the placement turned out not to need.
        for gang in list(reversed(evicted)):
            fewer = [g for g in evicted if g is not gang]
            smaller = _try_place(_without(state, fewer), clique, pods)
            if smaller is not None:
                evicted, result = fewer, smaller
        cost = sum(g.gpus() for g in evicted)
        if best is None or cost < best[0]:
            best = (cost, evicted, result[0], result[1])
    return None if best is None else (best[1], best[2], best[3])
