"""The scheduling loop: list, plan, bind, and (optionally) evict.

Stateless by design: every pass rebuilds its view from the API server. A
slice's clique is read back from where its bound pods run, and its slice
from each pod's rank, so a restarted scheduler picks up exactly where the
last one left off. The only in-memory state is a cooldown that keeps
defrag and slice migration from firing again before their evictions land,
and the nodes a defrag freed, held for the gang it freed them for. Without
that hold, the evicted gang's replacement pods, created while the old ones
are still terminating, could land right back where they were.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, FrozenSet, List, Tuple

from clique_scheduler.defrag import plan_defrag
from clique_scheduler.k8s import build_gangs, build_state
from clique_scheduler.model import Gang
from clique_scheduler.placement import State, fits_ignoring_topology, plan_gang

log = logging.getLogger(__name__)


@dataclass
class Config:
    scheduler_name: str = 'clique-scheduler'
    clique_label: str = 'nvidia.com/gpu.clique'
    slice_size: int = 16
    reserve: int = 2
    defrag: str = 'dry-run'  # off | dry-run | on
    migrate_slices: bool = False
    cooldown: float = 300.0


class Scheduler:

    def __init__(self, api, config: Config,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.api = api
        self.config = config
        self.clock = clock
        self._quiet_until: Dict[str, float] = {}
        # gang key -> (nodes held for it, expiry)
        self._reservations: Dict[str, Tuple[FrozenSet[str], float]] = {}

    def run_once(self) -> None:
        cfg = self.config
        nodes, pods = self.api.list_nodes(), self.api.list_pods()
        state = build_state(nodes, pods, cfg.clique_label, cfg.reserve)
        gangs, problems = build_gangs(pods, cfg.scheduler_name, cfg.slice_size)
        for problem in problems:
            if self._due(problem):
                log.warning(problem)

        now = self.clock()
        live = {g.key for g in gangs if any(p.node is None for p in g.pods)}
        self._reservations = {k: v for k, v in self._reservations.items()
                              if v[1] > now and k in live}

        for gang in sorted(gangs, key=lambda g: (-g.priority, g.created, g.key)):
            pending = [p for p in gang.pods if p.node is None]
            if not pending:
                continue
            if len(pending) == len(gang.pods) and len(gang.pods) < gang.size:
                continue  # Wait until every member exists before placing any.
            state.blocked = frozenset().union(
                *(nodes for key, (nodes, _) in self._reservations.items()
                  if key != gang.key))
            plan = plan_gang(state, gang)
            for pod_key, node in sorted(plan.assignment.items()):
                self._bind(pod_key, node)
            if plan.assignment:
                log.info('gang %s: bound %d pod(s)', gang.key, len(plan.assignment))
            if plan.blocked:
                self._blocked(state, gang, gangs)
            for index in plan.stuck_slices:
                self._stuck(gang, index)

    def _due(self, key: str) -> bool:
        """True at most once per cooldown for ``key``."""
        now = self.clock()
        if now < self._quiet_until.get(key, float('-inf')):
            return False
        self._quiet_until[key] = now + self.config.cooldown
        return True

    def _bind(self, pod_key: str, node: str) -> None:
        namespace, name = pod_key.split('/', 1)
        try:
            self.api.bind(namespace, name, node)
        except Exception as e:  # pylint: disable=broad-except
            # The next pass rebuilds from the API server, so just report it.
            log.warning('bind %s -> %s failed: %s', pod_key, node, e)

    def _evict(self, pod_key: str) -> None:
        namespace, name = pod_key.split('/', 1)
        try:
            self.api.evict(namespace, name)
        except Exception as e:  # pylint: disable=broad-except
            log.warning('evict %s failed: %s', pod_key, e)

    def _blocked(self, state: State, gang: Gang, gangs: List[Gang]) -> None:
        pending = [p for p in gang.pods if p.node is None]
        if self.config.defrag == 'off' or not fits_ignoring_topology(state, pending):
            return  # Short on capacity, not fragmented: just wait.
        if gang.key in self._reservations:
            return  # A defrag for this gang is already in flight.
        # Replan every pass (a safe plan can appear as soon as terminating
        # pods go away); only the logging is throttled.
        plan = plan_defrag(state, gang, gangs)
        if plan is None:
            if self._due(f'no-defrag:{gang.key}'):
                log.info('gang %s: blocked by fragmentation; no safe defrag plan yet',
                         gang.key)
            return
        names = ', '.join(v.key for v in plan.victims)
        if self.config.defrag != 'on':
            if self._due(f'defrag:{gang.key}'):
                log.info('gang %s: defrag would evict %s (%g GPUs) [dry-run]',
                         gang.key, names, plan.gpus)
            return
        log.info('gang %s: defrag evicting %s (%g GPUs)', gang.key, names, plan.gpus)
        self._reservations[gang.key] = (frozenset(plan.assignment.values()),
                                        self.clock() + self.config.cooldown)
        for victim in plan.victims:
            for pod in victim.pods:
                self._evict(pod.key)

    def _stuck(self, gang: Gang, index: int) -> None:
        if not self._due(f'stuck:{gang.key}#{index}'):
            return
        if not self.config.migrate_slices:
            log.warning('gang %s: slice %d has no spare left in its clique; waiting '
                        '(--migrate-slices would move the slice)', gang.key, index)
            return
        bound = [p for p in gang.pods_in_slice(index) if p.node]
        log.warning('gang %s: slice %d has no spare left; evicting its %d bound pod(s) '
                    'so it can be placed whole in another clique',
                    gang.key, index, len(bound))
        for pod in bound:
            self._evict(pod.key)
