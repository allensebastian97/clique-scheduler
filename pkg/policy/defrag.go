package policy

import "sort"

// DefragPlan is the result of PlanDefrag.
type DefragPlan struct {
	Victims []*Gang
	// GPUs held by the victims.
	GPUs float64
	// Assignment is where the blocked gang lands once the victims are gone.
	Assignment Assignment
}

// PlanDefrag finds the cheapest set of restart-safe gangs whose eviction lets
// blocked place, or nil. Victims must be preemptible, fully running, of no
// higher priority, and able to land again elsewhere afterwards. Otherwise
// evicting them would just move the blockage.
//
// Cost is GPUs evicted. The per-clique search is greedy (smallest gangs
// first, then drop victims that turn out unnecessary), not an exact optimum.
func PlanDefrag(s *State, blocked *Gang, gangs []*Gang) *DefragPlan {
	var candidates []*Gang
	for _, g := range gangs {
		if g.Preemptible && g.Key != blocked.Key && g.Priority <= blocked.Priority &&
			len(g.Pods) == g.Size && allBound(g) {
			candidates = append(candidates, g)
		}
	}
	trial := s.Copy()
	var victims []*Gang
	assignment := Assignment{}

	indexSet := map[int]bool{}
	for _, p := range blocked.Pods {
		if p.Node == "" {
			indexSet[blocked.SliceOf(p)] = true
		}
	}
	slices := make([]int, 0, len(indexSet))
	for index := range indexSet {
		slices = append(slices, index)
	}
	sort.Slice(slices, func(i, j int) bool {
		a, b := len(blocked.PodsInSlice(slices[i])), len(blocked.PodsInSlice(slices[j]))
		if a != b {
			return a > b
		}
		return slices[i] < slices[j]
	})

	for _, index := range slices {
		var pods []*Pod
		for _, p := range blocked.PodsInSlice(index) {
			if p.Node == "" {
				pods = append(pods, p)
			}
		}
		if _, placed, ok := BestClique(trial, pods, false); ok {
			merge(assignment, placed)
			continue
		}
		var remaining []*Gang
		for _, g := range candidates {
			if !contains(victims, g) {
				remaining = append(remaining, g)
			}
		}
		evicted, after, placed, ok := cheapestClearing(trial, pods, remaining)
		if !ok {
			return nil
		}
		victims = append(victims, evicted...)
		trial = after
		merge(assignment, placed)
	}

	check := trial.Copy()
	gpus := 0.0
	for _, v := range victims {
		if PlanGang(check, v.Unbound()).Blocked {
			return nil
		}
		gpus += v.GPUs()
	}
	return &DefragPlan{Victims: victims, GPUs: gpus, Assignment: assignment}
}

func without(s *State, gangs []*Gang) *State {
	trial := s.Copy()
	for _, g := range gangs {
		for _, p := range g.Pods {
			trial.Release(p)
		}
	}
	return trial
}

func tryPlace(s *State, clique string, pods []*Pod) (*State, Assignment, bool) {
	trial := s.Copy()
	placed, ok := PlaceInClique(trial, clique, pods, false)
	return trial, placed, ok
}

func cheapestClearing(s *State, pods []*Pod, candidates []*Gang) ([]*Gang, *State, Assignment, bool) {
	var (
		bestCost    float64
		bestEvicted []*Gang
		bestState   *State
		bestPlaced  Assignment
		found       bool
	)
	for _, clique := range s.cliqueNames {
		var local []*Gang
		for _, g := range candidates {
			for _, p := range g.Pods {
				if s.CliqueOf(p.Node) == clique {
					local = append(local, g)
					break
				}
			}
		}
		sort.SliceStable(local, func(i, j int) bool {
			if local[i].GPUs() != local[j].GPUs() {
				return local[i].GPUs() < local[j].GPUs()
			}
			return local[i].Key < local[j].Key
		})

		var evicted []*Gang
		var state *State
		var placed Assignment
		ok := false
		for _, g := range local {
			evicted = append(evicted, g)
			if state, placed, ok = tryPlace(without(s, evicted), clique, pods); ok {
				break
			}
		}
		if !ok {
			continue
		}
		// Drop victims the placement turned out not to need.
		order := append([]*Gang(nil), evicted...)
		for i := len(order) - 1; i >= 0; i-- {
			var fewer []*Gang
			for _, g := range evicted {
				if g != order[i] {
					fewer = append(fewer, g)
				}
			}
			if st, pl, ok := tryPlace(without(s, fewer), clique, pods); ok {
				evicted, state, placed = fewer, st, pl
			}
		}
		cost := 0.0
		for _, g := range evicted {
			cost += g.GPUs()
		}
		if !found || cost < bestCost {
			bestCost, bestEvicted, bestState, bestPlaced, found = cost, evicted, state, placed, true
		}
	}
	return bestEvicted, bestState, bestPlaced, found
}

func allBound(g *Gang) bool {
	for _, p := range g.Pods {
		if p.Node == "" {
			return false
		}
	}
	return true
}

func contains(gangs []*Gang, g *Gang) bool {
	for _, x := range gangs {
		if x == g {
			return true
		}
	}
	return false
}

func merge(into, from Assignment) {
	for k, v := range from {
		into[k] = v
	}
}
