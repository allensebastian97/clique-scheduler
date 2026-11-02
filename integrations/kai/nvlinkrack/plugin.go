// Package nvlinkrack is a KAI Scheduler plugin that adds two rules on top of
// KAI's topology-aware segment placement on NVLink racks:
//
//   - Spares. A GPU pod may not take one of the last `spares` idle nodes of an
//     NVLink clique, so a rack keeps headroom for replacements. Nodes that are
//     not ready count against the spares: a rack with two failed nodes still
//     takes a 16-node segment.
//   - Same-rack replacement. A pod of a segment constrained to the clique
//     level, whose other members are still running, must go to the clique
//     those members occupy, and may use its spares.
//
// It works as a predicate. KAI evaluates predicates per task with earlier
// tentative placements already applied to node usage, and when every node of
// a rack is rejected it rolls back and tries the next rack the topology
// plugin offered. KAI's topology plugin counts spares as free capacity, so
// filtering inside it would not be enough.
package nvlinkrack

import (
	"fmt"
	"sort"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/resource_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/log"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/scheduler_util"
)

// Name is the plugin name to enable in the scheduler configuration.
const Name = "nvlinkrack"

const (
	defaultSpares      = 2
	defaultCliqueLabel = "nvidia.com/gpu.clique"
)

type plugin struct {
	spares      int
	cliqueLabel string

	// Rebuilt every session.
	cliqueOf map[string]string
	cliques  map[string][]*node_info.NodeInfo
	ready    map[string]bool
}

// New builds the plugin. Arguments: "spares" (default 2) and "cliqueLabel"
// (default nvidia.com/gpu.clique).
func New(args framework.PluginArguments) framework.Plugin {
	spares, err := args.GetInt("spares", defaultSpares)
	if err != nil || spares < 0 {
		log.InfraLogger.Errorf("%s: invalid spares argument (%v); using %d", Name, err, defaultSpares)
		spares = defaultSpares
	}
	return &plugin{spares: spares, cliqueLabel: args.GetString("cliqueLabel", defaultCliqueLabel)}
}

func (p *plugin) Name() string { return Name }

func (p *plugin) OnSessionOpen(ssn *framework.Session) {
	p.cliqueOf = map[string]string{}
	p.cliques = map[string][]*node_info.NodeInfo{}
	p.ready = map[string]bool{}
	for name, node := range ssn.ClusterInfo.Nodes {
		if node.Node == nil {
			continue
		}
		clique := node.Node.Labels[p.cliqueLabel]
		if clique == "" {
			continue
		}
		p.cliqueOf[name] = clique
		p.cliques[clique] = append(p.cliques[clique], node)
		p.ready[name] = scheduler_util.ValidateIsNodeReady(node.Node)
	}
	ssn.AddPredicateFn(p.predicate)
}

func (p *plugin) OnSessionClose(*framework.Session) {}

func (p *plugin) predicate(task *pod_info.PodInfo, job *podgroup_info.PodGroupInfo,
	node *node_info.NodeInfo) error {
	clique := p.cliqueOf[node.Name]
	if clique == "" || !task.IsRequireAnyKindOfGPU() {
		return nil
	}
	if home := p.homeClique(task, job); home != "" {
		if clique != home {
			return fmt.Errorf("%s: segment %q of %s/%s runs in NVLink clique %s, not %s",
				Name, task.SubGroupName, job.Namespace, job.Name, home, clique)
		}
		return nil // Replacements may use the spares.
	}
	if !isIdle(node) {
		return nil // Packing onto a node already in use leaves the spares alone.
	}
	if idle, keep := p.idleCount(clique), p.sparesRequired(clique); idle <= keep {
		return fmt.Errorf("%s: node %s is one of the last %d idle nodes in NVLink clique %s, "+
			"kept as spares", Name, node.Name, keep, clique)
	}
	return nil
}

// homeClique returns the clique occupied by the other running members of the
// task's segment, if the task is a replacement: a genuinely pending pod of a
// segment constrained to the clique level.
//
// Tentative (virtual) state doesn't count, on either side. Members placed
// earlier in this cycle would make a new segment's later pods look like
// replacements. And a running pod that consolidation, preemption or reclaim
// is re-placing is not a replacement: letting victims onto the spares lets
// consolidation shuffle a segment onto them to free an ordinary node.
func (p *plugin) homeClique(task *pod_info.PodInfo, job *podgroup_info.PodGroupInfo) string {
	if job == nil || task.IsVirtualStatus || task.Status != pod_status.Pending {
		return ""
	}
	name := task.SubGroupName
	if name == "" {
		name = podgroup_info.DefaultSubGroup
	}
	podSet := job.GetAllPodSets()[name]
	if podSet == nil {
		return ""
	}
	constraint := podSet.GetTopologyConstraint()
	if constraint == nil ||
		(constraint.RequiredLevel != p.cliqueLabel && constraint.PreferredLevel != p.cliqueLabel) {
		return ""
	}
	counts := map[string]int{}
	for _, member := range podSet.GetPodInfos() {
		if member.UID == task.UID || member.IsVirtualStatus ||
			!pod_status.IsActiveAllocatedStatus(member.Status) {
			continue
		}
		if clique := p.cliqueOf[member.NodeName]; clique != "" {
			counts[clique]++
		}
	}
	home := ""
	for _, clique := range sortedKeys(counts) {
		if home == "" || counts[clique] > counts[home] {
			home = clique
		}
	}
	return home
}

func (p *plugin) idleCount(clique string) int {
	count := 0
	for _, node := range p.cliques[clique] {
		if p.ready[node.Name] && isIdle(node) {
			count++
		}
	}
	return count
}

func (p *plugin) sparesRequired(clique string) int {
	failed := 0
	for _, node := range p.cliques[clique] {
		if !p.ready[node.Name] {
			failed++
		}
	}
	return max(0, p.spares-failed)
}

// isIdle reports whether no GPU on the node is in use, counting pods still
// releasing and tentative placements from this cycle.
func isIdle(node *node_info.NodeInfo) bool {
	return node.UsedVector.Get(resource_info.GPUIndex) <= 0
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
