package policy

import "sort"

// State is the clique inventory plus per-node usage. Copy is cheap, so
// planners take what-if copies freely.
type State struct {
	nodes       map[string]*Node
	order       []string // node names in the order given to NewState
	cliques     map[string][]*Node
	cliqueNames []string
	used        map[string]Resources
	// Reserve is the number of idle spare nodes each clique keeps for
	// replacements.
	Reserve int
	// Blocked nodes are held for another gang: not usable, and not counted
	// as spares. Shared between copies; replace it rather than mutating it.
	Blocked map[string]bool
}

// NewState builds a state. used maps node name to resources in use; entries
// for unknown nodes are ignored.
func NewState(nodes []*Node, used map[string]Resources, reserve int) *State {
	s := &State{
		nodes:   make(map[string]*Node, len(nodes)),
		cliques: map[string][]*Node{},
		used:    make(map[string]Resources, len(nodes)),
		Reserve: reserve,
	}
	for _, n := range nodes {
		s.nodes[n.Name] = n
		s.order = append(s.order, n.Name)
		s.used[n.Name] = Resources{}.Add(used[n.Name])
	}
	sorted := append([]*Node(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, n := range sorted {
		if _, ok := s.cliques[n.Clique]; !ok {
			s.cliqueNames = append(s.cliqueNames, n.Clique)
		}
		s.cliques[n.Clique] = append(s.cliques[n.Clique], n)
	}
	sort.Strings(s.cliqueNames)
	return s
}

// Copy returns a copy whose usage can change independently.
func (s *State) Copy() *State {
	c := *s
	// Shallow is enough: per-node usage is replaced on change, never mutated.
	c.used = make(map[string]Resources, len(s.used))
	for k, v := range s.used {
		c.used[k] = v
	}
	return &c
}

// Cliques returns the clique names, sorted.
func (s *State) Cliques() []string { return s.cliqueNames }

// CliqueNodes returns a clique's nodes, sorted by name.
func (s *State) CliqueNodes(clique string) []*Node { return s.cliques[clique] }

// Node returns a node by name, or nil.
func (s *State) Node(name string) *Node { return s.nodes[name] }

// Free returns a node's allocatable minus what is in use.
func (s *State) Free(node string) Resources {
	return s.nodes[node].Allocatable.Sub(s.used[node])
}

// Usable reports whether a node is healthy and not held for another gang.
func (s *State) Usable(n *Node) bool {
	return n.Healthy && !s.Blocked[n.Name]
}

// IsIdle reports whether no GPU on the node is in use.
func (s *State) IsIdle(node string) bool {
	return s.used[node][GPU] <= 0
}

// IdleCount returns the number of usable, idle nodes in a clique.
func (s *State) IdleCount(clique string) int {
	count := 0
	for _, n := range s.cliques[clique] {
		if s.Usable(n) && s.IsIdle(n.Name) {
			count++
		}
	}
	return count
}

// SparesRequired is the number of idle nodes new slices must leave in a
// clique: the reserve, minus nodes that have already failed.
func (s *State) SparesRequired(clique string) int {
	failed := 0
	for _, n := range s.cliques[clique] {
		if !n.Healthy {
			failed++
		}
	}
	return max(0, s.Reserve-failed)
}

// CliqueOf returns a node's clique, or "" for unknown nodes.
func (s *State) CliqueOf(node string) string {
	if n, ok := s.nodes[node]; ok {
		return n.Clique
	}
	return ""
}

// Assign records a pod's requests against a node.
func (s *State) Assign(p *Pod, node string) {
	s.used[node] = s.used[node].Add(p.Requests)
}

// Release removes a bound pod's requests from its node.
func (s *State) Release(p *Pod) {
	if _, ok := s.used[p.Node]; ok {
		s.used[p.Node] = s.used[p.Node].Sub(p.Requests)
	}
}
