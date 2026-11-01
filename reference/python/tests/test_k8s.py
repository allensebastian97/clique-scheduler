import pytest

from nvlink_rack_scheduler.k8s import (build_gangs, build_state, node_from_json,
                                  parse_quantity, pod_requests)

from tests.fake_api import job_pod, node_json


@pytest.mark.parametrize('text,value', [
    ('4', 4.0), ('500m', 0.5), ('64Gi', 64 * 2**30), ('1.5k', 1500.0),
    ('1e3', 1000.0), ('2E', 2e18), (8, 8.0),
])
def test_parse_quantity(text, value):
    assert parse_quantity(text) == value


def test_pod_requests_take_the_larger_of_containers_and_init_containers():
    pod = {'spec': {
        'containers': [{'resources': {'requests': {'cpu': '1'}}},
                       {'resources': {'requests': {'cpu': '2', 'nvidia.com/gpu': '4'}}}],
        'initContainers': [{'resources': {'requests': {'cpu': '6'}}}],
    }}
    assert pod_requests(pod) == {'cpu': 6.0, 'nvidia.com/gpu': 4.0}


def test_nodes_without_a_clique_are_ignored_and_cordoned_nodes_are_unhealthy():
    assert node_from_json(node_json('cpu-0', None), 'nvidia.com/gpu.clique') is None
    cordoned = node_json('gpu-0', 'uuid.1')
    cordoned['spec']['unschedulable'] = True
    assert not node_from_json(cordoned, 'nvidia.com/gpu.clique').healthy
    assert not node_from_json(node_json('gpu-1', 'uuid.1', ready=False),
                              'nvidia.com/gpu.clique').healthy


def test_usage_counts_every_active_pod_including_other_schedulers():
    other = job_pod('x', 0, 1)
    other['spec'].update(schedulerName='default-scheduler', nodeName='gpu-0')
    done = job_pod('y', 0, 1)
    done['spec']['nodeName'] = 'gpu-0'
    done['status']['phase'] = 'Succeeded'
    state = build_state([node_json('gpu-0', 'uuid.1')], [other, done],
                        'nvidia.com/gpu.clique', reserve=2)
    assert state.free('gpu-0')['nvidia.com/gpu'] == 0


def test_gangs_take_rank_from_indexed_jobs_and_skip_terminating_pods():
    pods = [job_pod('train', i, 3, preemptible=True) for i in range(3)]
    old = job_pod('train', 1, 3)
    old['metadata']['name'] = 'train-1-old'
    old['metadata']['deletionTimestamp'] = '2026-10-31T00:01:00Z'
    gangs, problems = build_gangs(pods + [old], 'nvlink-rack-scheduler', 16)
    assert not problems
    (gang,) = gangs
    assert gang.key == 'ns/train' and gang.size == 3 and gang.preemptible
    assert sorted(p.rank for p in gang.pods) == [0, 1, 2]


def test_gangs_without_ranks_or_with_duplicates_are_reported():
    no_rank = job_pod('a', 0, 2)
    del no_rank['metadata']['annotations']['batch.kubernetes.io/job-completion-index']
    dup = [job_pod('b', 0, 2), job_pod('b', 0, 2)]
    dup[1]['metadata']['name'] = 'b-dup'
    gangs, problems = build_gangs([no_rank] + dup, 'nvlink-rack-scheduler', 16)
    assert not gangs and len(problems) == 2
