package nvlinkrack_test

import (
	"fmt"
	"sort"
	"testing"

	kaiv1alpha1 "github.com/kai-scheduler/api/kai/v1alpha1"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/allocate"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/actions/consolidation"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/pod_status"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/api/topology_info"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/constants"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/framework"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/jobs_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/nodes_fake"
	"github.com/kai-scheduler/KAI-scheduler/pkg/scheduler/test_utils/tasks_fake"

	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/kai/nvlinkrack"
)

// These tests run KAI's real allocate action, with its default plugins and
// topology-aware segment placement, on synthetic NVL72 racks of 4-GPU nodes.
// The key ones also run without nvlinkrack to show what it changes.

const (
	cliqueLabel  = "nvidia.com/gpu.clique"
	topologyName = "nvlink"
)

type cluster struct {
	racks    map[string]int // rack -> node count; nodes are named A00, A01, ...
	notReady []string
	jobs     []*jobs_fake.TestJobBasic
}

func node(rack string, i int) string { return fmt.Sprintf("%s%02d", rack, i) }

func nodes(rack string, from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, node(rack, i))
	}
	return out
}

// segmentedJob is a training job whose ranks are cut into segments, each
// required to stay inside one clique: what KAI's podgrouper builds for a
// PyTorchJob with kai.scheduler/segment-size. placed[r] is rank r's node, or
// "" if pending.
func segmentedJob(name string, size, segment int, placed []string) *jobs_fake.TestJobBasic {
	segments := (size + segment - 1) / segment
	root := subgroup_info.NewSubGroupSet(subgroup_info.RootSubGroupSetName, nil)
	workers := subgroup_info.NewSubGroupSet("worker", nil)
	minSegments := int32(segments)
	workers.SetMinSubGroup(&minSegments)
	for s := 0; s < segments; s++ {
		members := min(segment, size-s*segment)
		workers.AddPodSet(subgroup_info.NewPodSet(fmt.Sprintf("worker-%d", s), int32(members),
			&topology_info.TopologyConstraintInfo{Topology: topologyName, RequiredLevel: cliqueLabel}))
	}
	root.AddSubGroup(workers)
	job := &jobs_fake.TestJobBasic{Name: name, RequiredGPUsPerTask: 4, QueueName: "queue0",
		Priority: constants.PriorityTrainNumber, RootSubGroupSet: root}
	for r := 0; r < size; r++ {
		task := &tasks_fake.TestTaskBasic{SubGroupName: fmt.Sprintf("worker-%d", r/segment),
			State: pod_status.Pending}
		if r < len(placed) && placed[r] != "" {
			task.State, task.NodeName = pod_status.Running, placed[r]
		}
		job.Tasks = append(job.Tasks, task)
	}
	return job
}

// plainJob has no topology constraint. placed works as in segmentedJob.
func plainJob(name string, size int, placed []string) *jobs_fake.TestJobBasic {
	job := &jobs_fake.TestJobBasic{Name: name, RequiredGPUsPerTask: 4, QueueName: "queue0",
		Priority: constants.PriorityTrainNumber}
	for r := 0; r < size; r++ {
		task := &tasks_fake.TestTaskBasic{State: pod_status.Pending}
		if r < len(placed) && placed[r] != "" {
			task.State, task.NodeName = pod_status.Running, placed[r]
		}
		job.Tasks = append(job.Tasks, task)
	}
	return job
}

func (c cluster) topology() test_utils.TestTopologyBasic {
	bad := map[string]bool{}
	for _, n := range c.notReady {
		bad[n] = true
	}
	nodeMap := map[string]nodes_fake.TestNodeBasic{}
	for rack, count := range c.racks {
		for i := 0; i < count; i++ {
			meta := nodes_fake.TestNodeBasic{GPUs: 4, Labels: map[string]string{cliqueLabel: rack}}
			if bad[node(rack, i)] {
				meta.Conditions = &[]v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}}
			}
			nodeMap[node(rack, i)] = meta
		}
	}
	return test_utils.TestTopologyBasic{
		Topologies: []*kaiv1alpha1.Topology{{
			ObjectMeta: metav1.ObjectMeta{Name: topologyName},
			Spec: kaiv1alpha1.TopologySpec{Levels: []kaiv1alpha1.TopologyLevel{
				{NodeLabel: cliqueLabel}}},
		}},
		Jobs:  c.jobs,
		Nodes: nodeMap,
		Queues: []test_utils.TestQueueBasic{{Name: "queue0", ParentQueue: "department-a",
			DeservedGPUs: 1000, MaxAllowedGPUs: 1000, GPUOverQuotaWeight: 1}},
		Departments: []test_utils.TestDepartmentBasic{{Name: "department-a", DeservedGPUs: 1000}},
		Mocks: &test_utils.TestMock{
			CacheRequirements: &test_utils.CacheMocking{NumberOfCacheBinds: 1000,
				NumberOfCacheEvictions: 1000, NumberOfPipelineActions: 1000}},
	}
}

type action interface{ Execute(*framework.Session) }

// session runs the given actions in one scheduling session and returns it.
func (c cluster) session(t *testing.T, withPlugin bool, actions ...action) *framework.Session {
	t.Helper()
	test_utils.InitTestingInfrastructure()
	controller := gomock.NewController(t)
	t.Cleanup(controller.Finish)
	ssn := test_utils.BuildSession(c.topology(), controller)
	if withPlugin {
		nvlinkrack.New(framework.PluginArguments{"spares": "2"}).OnSessionOpen(ssn)
	}
	for _, a := range actions {
		a.Execute(ssn)
	}
	return ssn
}

// allocate runs one allocate pass and returns, per job, rank -> node for
// pods placed in this pass (rank = task order in the job).
func (c cluster) allocate(t *testing.T, withPlugin bool) map[string]map[int]string {
	t.Helper()
	ssn := c.session(t, withPlugin, allocate.New())

	placed := map[string]map[int]string{}
	for _, job := range c.jobs {
		info := ssn.ClusterInfo.PodGroupInfos[common_info.PodGroupID(job.Name)]
		placed[job.Name] = map[int]string{}
		for _, task := range info.GetAllPodsMap() {
			if task.Status == pod_status.Binding || task.Status == pod_status.Allocated {
				var rank int
				fmt.Sscanf(string(task.UID), job.Name+"-%d", &rank)
				placed[job.Name][rank] = task.NodeName
			}
		}
	}
	return placed
}

func racksOf(placed map[int]string) []string {
	seen := map[string]bool{}
	for _, n := range placed {
		seen[n[:1]] = true
	}
	var out []string
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func idleLeft(c cluster, placed map[string]map[int]string, rack string) int {
	used := map[string]bool{}
	for _, job := range c.jobs {
		for _, task := range job.Tasks {
			if task.NodeName != "" {
				used[task.NodeName] = true
			}
		}
	}
	for _, job := range placed {
		for _, n := range job {
			used[n] = true
		}
	}
	bad := map[string]bool{}
	for _, n := range c.notReady {
		bad[n] = true
	}
	idle := 0
	for i := 0; i < c.racks[rack]; i++ {
		if n := node(rack, i); !used[n] && !bad[n] {
			idle++
		}
	}
	return idle
}

func TestTwoSegmentsLandOnTwoRacksKeepingSpares(t *testing.T) {
	c := cluster{racks: map[string]int{"A": 18, "B": 18},
		jobs: []*jobs_fake.TestJobBasic{segmentedJob("train", 32, 16, nil)}}
	placed := c.allocate(t, true)["train"]
	if len(placed) != 32 {
		t.Fatalf("placed %d of 32 pods", len(placed))
	}
	first, second := map[int]string{}, map[int]string{}
	for r, n := range placed {
		if r < 16 {
			first[r] = n
		} else {
			second[r] = n
		}
	}
	if a, b := racksOf(first), racksOf(second); len(a) != 1 || len(b) != 1 || a[0] == b[0] {
		t.Fatalf("segments should each fill one distinct rack: %v and %v", a, b)
	}
}

func TestFullRackSegmentCannotTakeTheSpares(t *testing.T) {
	// Also the regression test for tentative placements: if the segment's
	// own earlier pods counted as running members, later pods would be
	// treated as replacements and allowed onto the spares.
	c := cluster{racks: map[string]int{"A": 18},
		jobs: []*jobs_fake.TestJobBasic{segmentedJob("train", 18, 18, nil)}}
	if got := len(c.allocate(t, false)["train"]); got != 18 {
		t.Fatalf("without the plugin KAI should fill the whole rack; placed %d", got)
	}
	if got := len(c.allocate(t, true)["train"]); got != 0 {
		t.Fatalf("with the plugin the segment must wait; placed %d", got)
	}
}

func TestFailedNodesUseUpTheSpares(t *testing.T) {
	c := cluster{racks: map[string]int{"A": 18}, notReady: []string{"A03", "A11"},
		jobs: []*jobs_fake.TestJobBasic{segmentedJob("train", 16, 16, nil)}}
	placed := c.allocate(t, true)["train"]
	if len(placed) != 16 {
		t.Fatalf("a rack with two failed nodes should still take 16; placed %d", len(placed))
	}
	for _, n := range placed {
		if n == "A03" || n == "A11" {
			t.Fatalf("placed on failed node %s", n)
		}
	}
}

func TestOtherJobsCannotTakeARacksSpares(t *testing.T) {
	c := cluster{racks: map[string]int{"A": 18, "B": 18}, jobs: []*jobs_fake.TestJobBasic{
		segmentedJob("big", 16, 16, nodes("A", 0, 16)),
		plainJob("small", 2, nil),
	}}
	if got := racksOf(c.allocate(t, false)["small"]); len(got) != 1 || got[0] != "A" {
		t.Fatalf("without the plugin KAI's bin-packing should put it on A's spares; got %v", got)
	}
	placed := c.allocate(t, true)
	if got := racksOf(placed["small"]); len(got) != 1 || got[0] != "B" {
		t.Fatalf("with the plugin the small job should go to B; got %v", got)
	}
	if left := idleLeft(c, placed, "A"); left != 2 {
		t.Fatalf("rack A should keep 2 spares, has %d", left)
	}
}

func TestReplacementRejoinsItsRackOnASpare(t *testing.T) {
	// Rank 5's pod is gone. A15 is busy, so A's only idle nodes are its two
	// spares: a new segment couldn't use them, a replacement can.
	placedBefore := nodes("A", 0, 16)
	placedBefore[5] = ""
	c := cluster{racks: map[string]int{"A": 18, "B": 18}, jobs: []*jobs_fake.TestJobBasic{
		segmentedJob("train", 16, 16, placedBefore),
		plainJob("neighbour", 1, []string{"A05"}),
	}}
	placed := c.allocate(t, true)["train"]
	if n, ok := placed[5]; !ok || (n != "A16" && n != "A17") {
		t.Fatalf("rank 5 should land on one of A's spares; got %q (placed=%v)", n, placed)
	}
}

func TestReplacementWaitsWhenItsRackHasNoSpare(t *testing.T) {
	placedBefore := nodes("A", 0, 16)
	placedBefore[5] = ""
	c := cluster{racks: map[string]int{"A": 18, "B": 18}, jobs: []*jobs_fake.TestJobBasic{
		segmentedJob("train", 16, 16, placedBefore),
		plainJob("neighbours", 3, []string{"A05", "A16", "A17"}),
	}}
	if placed := c.allocate(t, true)["train"]; len(placed) != 0 {
		t.Fatalf("rank 5 must not leave its rack's NVLink domain; placed %v", placed)
	}
}

func TestConsolidationCannotMoveASegmentOntoTheSpares(t *testing.T) {
	// Found in the live test: with only the spares idle, consolidation evicted
	// a running segment and re-placed it with two pods on the spares (they
	// looked like replacements, because one member had been put back first),
	// freeing an ordinary node for a 1-pod job. The spares were used up and a
	// training job disrupted. Re-placed victims must count as new placements.
	c := cluster{racks: map[string]int{"A": 18}, jobs: []*jobs_fake.TestJobBasic{
		segmentedJob("train", 16, 16, nodes("A", 0, 16)),
		plainJob("probe", 1, nil),
	}}
	status := func(ssn *framework.Session, job string) map[pod_status.PodStatus]int {
		counts := map[pod_status.PodStatus]int{}
		for _, task := range ssn.ClusterInfo.PodGroupInfos[common_info.PodGroupID(job)].GetAllPodsMap() {
			counts[task.Status]++
		}
		return counts
	}

	stock := c.session(t, false, allocate.New(), consolidation.New())
	if got := status(stock, "probe"); got[pod_status.Binding] != 1 {
		t.Fatalf("without the plugin the probe should simply take a spare; got %v", got)
	}
	ssn := c.session(t, true, allocate.New(), consolidation.New())
	if got := status(ssn, "probe"); got[pod_status.Pending] != 1 {
		t.Errorf("the probe must stay pending; got %v", got)
	}
	if got := status(ssn, "train"); got[pod_status.Running] != 16 {
		t.Errorf("the segment must not be disturbed; got %v", got)
	}
}
