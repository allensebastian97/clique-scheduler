from nvlink_rack_scheduler.defrag import plan_defrag

from tests.util import make_gang, make_state, occupy


def fragmented(preemptible=True, priority=0):
    """Two 18-node cliques, each with a 4-node gang, so no clique fits 16 + 2 spares."""
    state = make_state({'A': 18, 'B': 18})
    small_a = make_gang('small-a', 4, preemptible=preemptible, priority=priority)
    small_b = make_gang('small-b', 2, preemptible=preemptible, priority=priority)
    occupy(state, small_a, [f'A{i:02d}' for i in range(4)])
    occupy(state, small_b, ['B00', 'B01', 'B02', 'B03'][:2])
    occupy(state, make_gang('pinned', 2), ['B02', 'B03'])  # not restart-safe
    return state, [small_a, small_b]


def test_evicts_the_cheapest_gang_that_frees_a_clique():
    state, gangs = fragmented()
    blocked = make_gang('job', 16)
    plan = plan_defrag(state, blocked, gangs + [blocked])
    assert plan is not None
    assert [v.key for v in plan.victims] == ['ns/small-a']
    assert plan.gpus == 16
    assert {state.clique_of(n) for n in plan.assignment.values()} == {'A'}


def test_never_evicts_workloads_that_are_not_restart_safe():
    state, gangs = fragmented(preemptible=False)
    assert plan_defrag(state, make_gang('job', 16), gangs) is None


def test_never_evicts_higher_priority_work():
    state, gangs = fragmented(priority=10)
    assert plan_defrag(state, make_gang('job', 16, priority=5), gangs) is None


def test_refuses_when_victims_could_not_land_again():
    state = make_state({'A': 18})
    small = make_gang('small', 4, preemptible=True)
    occupy(state, small, [f'A{i:02d}' for i in range(4)])
    # Evicting `small` frees A for the job, but then `small` has nowhere to go.
    assert plan_defrag(state, make_gang('job', 16), [small]) is None


def test_drops_victims_the_placement_does_not_need():
    state = make_state({'A': 18, 'B': 18})
    tiny = make_gang('tiny', 1, preemptible=True)
    big = make_gang('big', 3, preemptible=True)
    occupy(state, tiny, ['A00'])
    occupy(state, big, ['A01', 'A02', 'A03'])
    occupy(state, make_gang('pinned', 3), ['B00', 'B01', 'B02'])
    # A has 14 idle and the job needs 15 + 2 spares. Greedy evicts tiny (15 idle, not
    # enough) then big (18); pruning then finds big alone (17) suffices.
    plan = plan_defrag(state, make_gang('job', 15), [tiny, big])
    assert plan is not None
    assert [v.key for v in plan.victims] == ['ns/big']
