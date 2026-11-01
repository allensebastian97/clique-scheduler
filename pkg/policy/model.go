// Package policy is the clique-aware placement policy shared by every
// scheduler integration: rank slices pinned to one NVLink clique, spare nodes
// per clique, same-clique replacement and topology defragmentation.
//
// It has no Kubernetes or Slurm dependencies. Its behaviour is pinned to the
// Python reference implementation by the shared scenarios in
// testdata/scenarios.
package policy

// GPU is the resource name GPUs are counted under.
const GPU = "nvidia.com/gpu"

// Resources maps a resource name to an amount. Values are never mutated in
// place; Add and Sub return new maps.
type Resources map[string]float64

// Add returns r + o.
func (r Resources) Add(o Resources) Resources {
	out := make(Resources, len(r)+len(o))
	for k, v := range r {
		out[k] = v
	}
	for k, v := range o {
		out[k] += v
	}
	return out
}

// Sub returns r - o.
func (r Resources) Sub(o Resources) Resources {
	out := make(Resources, len(r)+len(o))
	for k, v := range r {
		out[k] = v
	}
	for k, v := range o {
		out[k] -= v
	}
	return out
}

// Fits reports whether request fits in free.
func Fits(request, free Resources) bool {
	for k, v := range request {
		if v > 0 && free[k] < v {
			return false
		}
	}
	return true
}

// Taint is a node taint.
type Taint struct {
	Key, Value, Effect string
}

// Toleration is a pod toleration. An empty Operator means "Equal".
type Toleration struct {
	Key, Operator, Value, Effect string
}

// Tolerates reports whether t tolerates taint.
func (t Toleration) Tolerates(taint Taint) bool {
	if t.Effect != "" && t.Effect != taint.Effect {
		return false
	}
	if t.Operator == "Exists" {
		return t.Key == "" || t.Key == taint.Key
	}
	return t.Key == taint.Key && t.Value == taint.Value
}

// Node is a schedulable node in an NVLink clique.
type Node struct {
	Name        string
	Clique      string
	Allocatable Resources
	Healthy     bool
	Labels      map[string]string
	Taints      []Taint
}

// Pod is one rank of a gang.
type Pod struct {
	Key          string // "namespace/name"
	Gang         string // "namespace/gang"
	Rank         int
	Requests     Resources
	Node         string // "" while pending
	NodeSelector map[string]string
	Tolerations  []Toleration
}

// CanRunOn reports whether node is healthy and satisfies the pod's node
// selector and NoSchedule/NoExecute taints.
func (p *Pod) CanRunOn(node *Node) bool {
	if !node.Healthy {
		return false
	}
	for k, v := range p.NodeSelector {
		if got, ok := node.Labels[k]; !ok || got != v {
			return false
		}
	}
	for _, taint := range node.Taints {
		if taint.Effect != "NoSchedule" && taint.Effect != "NoExecute" {
			continue
		}
		tolerated := false
		for _, t := range p.Tolerations {
			if t.Tolerates(taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

// Gang is a set of pods scheduled together, cut into slices of SliceSize
// consecutive ranks.
type Gang struct {
	Key         string
	Size        int
	SliceSize   int
	Pods        []*Pod
	Priority    int
	Preemptible bool
}

// SliceOf returns the slice a pod belongs to.
func (g *Gang) SliceOf(p *Pod) int {
	return p.Rank / g.SliceSize
}

// PodsInSlice returns the gang's pods in slice index, in gang order.
func (g *Gang) PodsInSlice(index int) []*Pod {
	var out []*Pod
	for _, p := range g.Pods {
		if g.SliceOf(p) == index {
			out = append(out, p)
		}
	}
	return out
}

// GPUs returns the GPUs the gang's pods request.
func (g *Gang) GPUs() float64 {
	total := 0.0
	for _, p := range g.Pods {
		total += p.Requests[GPU]
	}
	return total
}

// Unbound returns a copy of the gang with every pod pending.
func (g *Gang) Unbound() *Gang {
	fresh := *g
	fresh.Pods = make([]*Pod, len(g.Pods))
	for i, p := range g.Pods {
		copied := *p
		copied.Node = ""
		fresh.Pods[i] = &copied
	}
	return &fresh
}
