package policy

import "sort"

// Assignment maps pod key to node name.
type Assignment map[string]string

// PlaceInClique fits pods into clique, filling partly used nodes before idle
// ones. Unless useSpares is set, it may not take the clique's last spares.
// It mutates s only on success.
func PlaceInClique(s *State, clique string, pods []*Pod, useSpares bool) (Assignment, bool) {
	nodes := s.cliques[clique]
	free := map[string]Resources{}
	freeGPUs, wantGPUs := 0.0, 0.0
	for _, n := range nodes {
		if s.Usable(n) {
			free[n.Name] = s.Free(n.Name)
			freeGPUs += free[n.Name][GPU]
		}
	}
	for _, p := range pods {
		wantGPUs += p.Requests[GPU]
	}
	if freeGPUs < wantGPUs {
		return nil, false
	}
	idle := map[string]bool{}
	for name := range free {
		if s.IsIdle(name) {
			idle[name] = true
		}
	}
	idleBefore := len(idle)

	byRank := append([]*Pod(nil), pods...)
	sort.SliceStable(byRank, func(i, j int) bool { return byRank[i].Rank < byRank[j].Rank })
	out := Assignment{}
	for _, p := range byRank {
		best := ""
		for _, n := range nodes {
			f, ok := free[n.Name]
			if !ok || !p.CanRunOn(n) || !Fits(p.Requests, f) {
				continue
			}
			if best == "" || packsBefore(n.Name, best, idle, free) {
				best = n.Name
			}
		}
		if best == "" {
			return nil, false
		}
		free[best] = free[best].Sub(p.Requests)
		if p.Requests[GPU] > 0 {
			delete(idle, best)
		}
		out[p.Key] = best
	}
	if !useSpares && len(idle) < s.SparesRequired(clique) && len(idle) < idleBefore {
		return nil, false
	}
	for _, p := range pods {
		s.Assign(p, out[p.Key])
	}
	return out, true
}

// packsBefore orders candidate nodes: partly used before idle, then fewest
// free GPUs, then name.
func packsBefore(a, b string, idle map[string]bool, free map[string]Resources) bool {
	if idle[a] != idle[b] {
		return !idle[a]
	}
	if free[a][GPU] != free[b][GPU] {
		return free[a][GPU] < free[b][GPU]
	}
	return a < b
}

// BestClique places pods in the clique they fill most tightly. It mutates s
// on success.
func BestClique(s *State, pods []*Pod, useSpares bool) (string, Assignment, bool) {
	var (
		bestClique string
		bestPlaced Assignment
		bestTrial  *State
		bestIdle   int
		bestGPUs   float64
	)
	for _, clique := range s.cliqueNames {
		trial := s.Copy()
		placed, ok := PlaceInClique(trial, clique, pods, useSpares)
		if !ok {
			continue
		}
		leftover := 0.0
		for _, n := range s.cliques[clique] {
			if trial.Usable(n) {
				leftover += trial.Free(n.Name)[GPU]
			}
		}
		idle := trial.IdleCount(clique)
		// Cliques are visited in name order, so ties keep the first.
		if bestTrial == nil || idle < bestIdle || (idle == bestIdle && leftover < bestGPUs) {
			bestClique, bestPlaced, bestTrial, bestIdle, bestGPUs = clique, placed, trial, idle, leftover
		}
	}
	if bestTrial == nil {
		return "", nil, false
	}
	s.used = bestTrial.used
	return bestClique, bestPlaced, true
}

// HomeClique returns the clique a slice already occupies, judged by its bound
// members; ties go to the clique seen first. "" if none are bound.
func HomeClique(s *State, members []*Pod) string {
	counts := map[string]int{}
	var seen []string
	for _, p := range members {
		c := s.CliqueOf(p.Node)
		if c == "" {
			continue
		}
		if counts[c] == 0 {
			seen = append(seen, c)
		}
		counts[c]++
	}
	home := ""
	for _, c := range seen {
		if home == "" || counts[c] > counts[home] {
			home = c
		}
	}
	return home
}

// GangPlan is the result of PlanGang.
type GangPlan struct {
	Assignment Assignment
	// StuckSlices are slices whose replacement pods don't fit in the clique
	// they occupy.
	StuckSlices []int
	// Blocked means first placement found no room; nothing was assigned.
	Blocked bool
}

// PlanGang plans the gang's pending pods and records the plan in s.
//
// First placement is all-or-nothing: each slice goes to its best-fit clique
// without touching spares. Replacement is per slice: pods rejoin the clique
// their slice occupies and may use its spares.
func PlanGang(s *State, g *Gang) GangPlan {
	plan := GangPlan{Assignment: Assignment{}, StuckSlices: []int{}}
	waiting := map[int][]*Pod{}
	pending := 0
	for _, p := range g.Pods {
		if p.Node == "" {
			waiting[g.SliceOf(p)] = append(waiting[g.SliceOf(p)], p)
			pending++
		}
	}
	if pending == 0 {
		return plan
	}
	initial := pending == len(g.Pods)
	slices := make([]int, 0, len(waiting))
	for index := range waiting {
		slices = append(slices, index)
	}
	sort.Slice(slices, func(i, j int) bool {
		a, b := slices[i], slices[j]
		if len(waiting[a]) != len(waiting[b]) {
			return len(waiting[a]) > len(waiting[b])
		}
		return a < b
	})

	trial := s.Copy()
	for _, index := range slices {
		var placed Assignment
		if home := HomeClique(trial, g.PodsInSlice(index)); home != "" {
			var ok bool
			if placed, ok = PlaceInClique(trial, home, waiting[index], true); !ok {
				plan.StuckSlices = append(plan.StuckSlices, index)
				continue
			}
		} else {
			var ok bool
			if _, placed, ok = BestClique(trial, waiting[index], false); !ok {
				if initial {
					return GangPlan{Assignment: Assignment{}, StuckSlices: []int{}, Blocked: true}
				}
				continue // A whole slice is waiting for room; nothing to repair yet.
			}
		}
		for k, v := range placed {
			plan.Assignment[k] = v
		}
	}
	s.used = trial.used
	return plan
}

// FitsIgnoringTopology reports whether pods would fit if NVLink domains
// didn't matter: the test for "blocked by fragmentation".
func FitsIgnoringTopology(s *State, pods []*Pod) bool {
	trial := s.Copy()
	for _, p := range pods {
		placed := false
		for _, name := range trial.order {
			n := trial.nodes[name]
			if trial.Usable(n) && p.CanRunOn(n) && Fits(p.Requests, trial.Free(name)) {
				trial.Assign(p, name)
				placed = true
				break
			}
		}
		if !placed {
			return false
		}
	}
	return true
}
