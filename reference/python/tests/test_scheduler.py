"""End-to-end passes of the scheduler loop against the fake API."""
from collections import Counter

from nvlink_rack_scheduler.scheduler import Config, Scheduler

from tests.fake_api import FakeApi, job_pod, node_json

CLIQUE = {'A': 'uuid-a.1', 'B': 'uuid-b.1'}


def cluster():
    nodes = [node_json(f'{c}{i:02d}', CLIQUE[c]) for c in 'AB' for i in range(18)]
    return FakeApi(nodes + [node_json('cpu-0', None)])


def clique_counts(api, job):
    return Counter(api.node_of(k)[0] for k, pod in api.pods.items()
                   if k.startswith(f'ns/{job}-') and api.node_of(k)
                   and not pod['metadata'].get('deletionTimestamp'))


def fragment(api):
    """Leaves a 4-node restart-safe gang on each clique, as finished neighbors would.

    Neither clique then has room for 16 nodes plus 2 spares.
    """
    for job, clique in (('small-a', 'A'), ('small-b', 'B')):
        for i in range(4):
            pod = job_pod(job, i, 4, preemptible=True)
            pod['spec']['nodeName'] = f'{clique}{i:02d}'
            pod['status']['phase'] = 'Running'
            api.add_pod(pod)


def test_waits_for_the_whole_gang_then_places_one_slice_per_clique():
    api = cluster()
    scheduler = Scheduler(api, Config())
    for i in range(31):
        api.add_pod(job_pod('train', i, 32))
    scheduler.run_once()
    assert not any(api.node_of(k) for k in api.pods)

    api.add_pod(job_pod('train', 31, 32))
    scheduler.run_once()
    assert clique_counts(api, 'train') == {'A': 16, 'B': 16}
    for start in (0, 16):
        assert len({api.node_of(f'ns/train-{i}-x')[0] for i in range(start, start + 16)}) == 1


def test_failed_node_is_replaced_from_the_same_clique():
    api = cluster()
    scheduler = Scheduler(api, Config())
    for i in range(16):
        api.add_pod(job_pod('train', i, 16))
    scheduler.run_once()
    home = api.node_of('ns/train-5-x')[0]

    # The node dies; the Job controller replaces the pod with the same index.
    dead = api.node_of('ns/train-5-x')
    api.set_ready(dead, False)
    del api.pods['ns/train-5-x']
    api.add_pod(job_pod('train', 5, 16))
    scheduler.run_once()

    new = api.node_of('ns/train-5-x')
    assert new != dead and new[0] == home


def test_defrag_evicts_a_restart_safe_gang_and_the_blocked_gang_then_lands():
    api = cluster()
    scheduler = Scheduler(api, Config(defrag='on'))
    fragment(api)

    for i in range(16):
        api.add_pod(job_pod('train', i, 16, created='2026-10-31T00:00:02Z'))
    scheduler.run_once()
    assert len(api.evicted) == 4
    victim = api.evicted[0].split('/')[1].rsplit('-', 2)[0]
    victim_rack = 'A' if victim == 'small-a' else 'B'

    # The victim's Job recreates its pods while the old ones are still
    # terminating. They must not land back in the rack being freed.
    for i in range(4):
        api.add_pod(job_pod(victim, i, 4, preemptible=True,
                            created='2026-10-31T00:00:03Z', suffix='y'))
    scheduler.run_once()
    assert set(clique_counts(api, victim)) == {'AB'.replace(victim_rack, '')}
    assert not clique_counts(api, 'train')

    api.finish_terminating()
    scheduler.run_once()
    assert clique_counts(api, 'train') == {victim_rack: 16}


def test_dry_run_defrag_evicts_nothing():
    api = cluster()
    scheduler = Scheduler(api, Config(defrag='dry-run'))
    fragment(api)
    for i in range(16):
        api.add_pod(job_pod('train', i, 16, created='2026-10-31T00:00:02Z'))
    scheduler.run_once()
    assert not api.evicted
    assert not any(api.node_of(k) for k in api.pods if k.startswith('ns/train-'))


def test_stuck_slice_is_migrated_only_when_enabled():
    for migrate in (False, True):
        # A has no room for 16 + 2 spares, so the gang lands on B.
        api = FakeApi([node_json(f'A{i:02d}', 'uuid-a.1') for i in range(16)] +
                      [node_json(f'B{i:02d}', 'uuid-b.1') for i in range(18)])
        scheduler = Scheduler(api, Config(migrate_slices=migrate))
        for i in range(16):
            api.add_pod(job_pod('train', i, 16))
        scheduler.run_once()
        assert clique_counts(api, 'train') == {'B': 16}

        # Something else takes B's two spares, then a training node dies.
        for i, node in enumerate(('B16', 'B17')):
            pod = job_pod('other', i, 2)
            pod['spec'].update(schedulerName='default-scheduler', nodeName=node)
            api.add_pod(pod)
        api.set_ready(api.node_of('ns/train-3-x'), False)
        del api.pods['ns/train-3-x']
        api.add_pod(job_pod('train', 3, 16))
        scheduler.run_once()

        assert api.node_of('ns/train-3-x') is None
        assert len(api.evicted) == (15 if migrate else 0)
