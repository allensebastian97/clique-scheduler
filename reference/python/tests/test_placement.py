from nvlink_rack_scheduler.model import Taint, Toleration
from nvlink_rack_scheduler.placement import fits_ignoring_topology, plan_gang

from tests.util import bind, cliques_of, make_gang, make_state, occupy


def test_small_gang_stays_on_one_clique_and_prefers_the_fuller_one():
    state = make_state({'A': 18, 'B': 18})
    occupy(state, make_gang('busy', 6), [f'A{i:02d}' for i in range(6)])
    gang = make_gang('job', 8)
    plan = plan_gang(state, gang)
    bind(state, gang, plan.assignment)
    assert len(plan.assignment) == 8
    assert cliques_of(state, gang) == {'A'}


def test_large_gang_is_cut_into_one_slice_per_clique_leaving_spares():
    state = make_state({'A': 18, 'B': 18})
    gang = make_gang('job', 32)
    plan = plan_gang(state, gang)
    bind(state, gang, plan.assignment)
    assert len(plan.assignment) == 32
    first, second = cliques_of(state, gang, range(16)), cliques_of(state, gang, range(16, 32))
    assert len(first) == len(second) == 1 and first != second
    assert state.idle_count('A') == state.idle_count('B') == 2


def test_full_rack_slice_cannot_eat_the_spares():
    gang = make_gang('job', 18, slice_size=18)
    assert plan_gang(make_state({'A': 18}), gang).blocked
    assert len(plan_gang(make_state({'A': 18}, reserve=0), gang).assignment) == 18


def test_failed_nodes_use_up_the_reserve_so_the_rack_stays_usable():
    state = make_state({'A': 18}, unhealthy=['A03', 'A11'])
    plan = plan_gang(state, make_gang('job', 16))
    assert len(plan.assignment) == 16
    assert not {'A03', 'A11'} & set(plan.assignment.values())


def test_other_gangs_cannot_take_a_racks_spares():
    state = make_state({'A': 18, 'B': 18})
    big = make_gang('big', 16)
    bind(state, big, plan_gang(state, big).assignment)
    small = make_gang('small', 2)
    bind(state, small, plan_gang(state, small).assignment)
    assert cliques_of(state, small) != cliques_of(state, big)


def test_replacement_goes_back_to_its_clique_and_may_use_a_spare():
    state = make_state({'A': 18, 'B': 18})
    occupy(state, make_gang('other', 16), [f'B{i:02d}' for i in range(16)])
    gang = make_gang('job', 16)
    bind(state, gang, plan_gang(state, gang).assignment)
    home = cliques_of(state, gang)

    # Rank 5's node dies; its pod is gone and the workload recreates it unbound.
    victim = gang.pods[5]
    state.release(victim)
    state.nodes[victim.node].healthy = False
    old_node, victim.node = victim.node, None

    plan = plan_gang(state, gang)
    new_node = plan.assignment[victim.key]
    assert new_node != old_node
    assert state.clique_of(new_node) in home
    assert not plan.stuck_slices


def test_replacement_with_no_spare_left_reports_the_slice_stuck():
    state = make_state({'A': 16, 'B': 18})
    occupy(state, make_gang('other', 18, slice_size=18), [f'B{i:02d}' for i in range(18)])
    gang = make_gang('job', 16)
    occupy(state, gang, [f'A{i:02d}' for i in range(16)])
    state.release(gang.pods[0])
    state.nodes[gang.pods[0].node].healthy = False
    gang.pods[0].node = None
    plan = plan_gang(state, gang)
    assert plan.stuck_slices == [0] and not plan.assignment


def test_first_placement_is_all_or_nothing():
    state = make_state({'A': 18, 'B': 18})
    before = {k: dict(v) for k, v in state.used.items()}
    plan = plan_gang(state, make_gang('job', 48))  # 3 slices, 2 cliques
    assert plan.blocked and not plan.assignment
    assert state.used == before


def test_node_selector_and_taints_are_respected():
    state = make_state({'A': 18}, reserve=0)
    state.nodes['A00'].taints = [Taint('maintenance', 'yes', 'NoSchedule')]
    for name in ('A01', 'A02'):
        state.nodes[name].labels = {'pool': 'train'}
    gang = make_gang('job', 2)
    for pod in gang.pods:
        pod.node_selector = {'pool': 'train'}
    assert set(plan_gang(state, gang).assignment.values()) == {'A01', 'A02'}

    tolerant = make_gang('fixer', 1)
    tolerant.pods[0].tolerations = [Toleration(key='maintenance', operator='Exists')]
    tolerant.pods[0].node_selector = {}
    assert plan_gang(state.copy(), tolerant).assignment  # A00 is usable to it


def test_fragmentation_is_distinguished_from_lack_of_capacity():
    state = make_state({'A': 18, 'B': 18})
    occupy(state, make_gang('a', 4), [f'A{i:02d}' for i in range(4)])
    occupy(state, make_gang('b', 4), [f'B{i:02d}' for i in range(4)])
    gang = make_gang('job', 16)
    assert plan_gang(state.copy(), gang).blocked
    assert fits_ignoring_topology(state, gang.pods)
    assert not fits_ignoring_topology(state, make_gang('huge', 29).pods)
