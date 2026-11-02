package daemon

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/slurm/slurm"
)

// fake is a minimal in-memory Slurm: tests place and requeue jobs by hand;
// the daemon's reservations and job updates are recorded.
type fake struct {
	blocks   []slurm.Block
	nodes    map[string]*slurm.Node
	jobs     map[int]*slurm.Job
	resvs    map[string]*slurm.Reservation
	updates  map[int]map[string]string
	requeued []int
}

func newFake(blocks map[string]int) *fake {
	f := &fake{nodes: map[string]*slurm.Node{}, jobs: map[int]*slurm.Job{},
		resvs: map[string]*slurm.Reservation{}, updates: map[int]map[string]string{}}
	names := slices.Sorted(func(yield func(string) bool) {
		for b := range blocks {
			if !yield(b) {
				return
			}
		}
	})
	for _, b := range names {
		block := slurm.Block{Name: b}
		for i := 1; i <= blocks[b]; i++ {
			n := fmt.Sprintf("%sn%02d", b, i)
			block.Nodes = append(block.Nodes, n)
			f.nodes[n] = &slurm.Node{Name: n, State: []string{"IDLE"}}
		}
		f.blocks = append(f.blocks, block)
	}
	return f
}

func nodeRange(block string, from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("%sn%02d", block, i))
	}
	return out
}

// run places a running job on nodes.
func (f *fake) run(id int, user string, nodes []string, segment int, comment string) {
	f.jobs[id] = &slurm.Job{ID: id, User: user, State: "RUNNING", Nodes: nodes,
		NodeCount: len(nodes), Segment: segment, Requeue: true, Comment: comment}
	for _, n := range nodes {
		f.nodes[n].State = []string{"ALLOCATED"}
	}
}

func (f *fake) pend(id int, user string, count, segment int, reason string) {
	f.jobs[id] = &slurm.Job{ID: id, User: user, State: "PENDING", NodeCount: count,
		Segment: segment, Requeue: true, Reason: reason}
}

// requeue does what Slurm does: free the nodes, clear them, bump restarts.
func (f *fake) requeue(id int) {
	j := f.jobs[id]
	for _, n := range j.Nodes {
		if f.nodes[n].Healthy() {
			f.nodes[n].State = []string{"IDLE"}
		}
	}
	j.State, j.Nodes, j.Reason = "PENDING", nil, "BeginTime"
	j.Restarts++
}

func (f *fake) fail(node string) { f.nodes[node].State = []string{"DOWN"} }

func (f *fake) Topology() ([]slurm.Block, error) { return f.blocks, nil }

func (f *fake) Nodes() ([]slurm.Node, error) {
	inResv := map[string]string{}
	for _, r := range f.resvs {
		for _, n := range r.Nodes {
			inResv[n] = r.Name
		}
	}
	var out []slurm.Node
	for _, n := range f.nodes {
		node := *n
		node.Reservation = inResv[n.Name]
		out = append(out, node)
	}
	return out, nil
}

func (f *fake) Jobs() ([]slurm.Job, error) {
	var out []slurm.Job
	for _, j := range f.jobs {
		out = append(out, *j)
	}
	return out, nil
}

func (f *fake) Reservations() ([]slurm.Reservation, error) {
	var out []slurm.Reservation
	for _, r := range f.resvs {
		out = append(out, *r)
	}
	return out, nil
}

func (f *fake) CreateReservation(r slurm.Reservation) error {
	if _, ok := f.resvs[r.Name]; ok {
		return fmt.Errorf("reservation %s exists", r.Name)
	}
	f.resvs[r.Name] = &r
	return nil
}

func (f *fake) UpdateReservation(r slurm.Reservation) error {
	cur, ok := f.resvs[r.Name]
	if !ok {
		return fmt.Errorf("no reservation %s", r.Name)
	}
	cur.Nodes, cur.Users = r.Nodes, r.Users
	return nil
}

func (f *fake) DeleteReservation(name string) error {
	delete(f.resvs, name)
	return nil
}

func (f *fake) UpdateJob(id int, fields map[string]string) error {
	if f.updates[id] == nil {
		f.updates[id] = map[string]string{}
	}
	for k, v := range fields {
		f.updates[id][k] = v
	}
	if r, ok := fields["Reservation"]; ok {
		f.jobs[id].Reservation = r
	}
	return nil
}

func (f *fake) Requeue(id int) error {
	f.requeued = append(f.requeued, id)
	f.requeue(id)
	return nil
}

func (f *fake) spares(block string) []string {
	r := f.resvs["nvlr-spares-"+block]
	if r == nil {
		return nil
	}
	nodes := slices.Clone(r.Nodes)
	sort.Strings(nodes)
	return nodes
}

func newDaemon(t *testing.T, f *fake, defrag string) *Daemon {
	t.Helper()
	d, err := New(f, Config{Spares: 2, Preemptible: "nvlink-rack.io/preemptible", Defrag: defrag,
		HoldFor: 10 * time.Minute}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func reconcile(t *testing.T, d *Daemon) {
	t.Helper()
	if err := d.Reconcile(); err != nil {
		t.Fatal(err)
	}
}

func TestEachBlockKeepsTwoIdleSpares(t *testing.T) {
	f := newFake(map[string]int{"b1": 18, "b2": 18})
	f.run(1, "alice", nodeRange("b1", 1, 16), 16, "")
	d := newDaemon(t, f, "off")
	reconcile(t, d)
	if got := f.spares("b1"); !slices.Equal(got, nodeRange("b1", 17, 18)) {
		t.Errorf("b1 spares = %v", got)
	}
	if got := f.spares("b2"); len(got) != 2 {
		t.Errorf("b2 spares = %v", got)
	}
	if r := f.resvs["nvlr-spares-b1"]; !slices.Contains(r.Flags, "FLEX") ||
		!slices.Equal(r.Users, []string{"root"}) {
		t.Errorf("spare reservation = %+v", r)
	}
}

func TestFailedNodesUseUpTheSpares(t *testing.T) {
	f := newFake(map[string]int{"b1": 18, "b2": 18})
	f.fail("b1n03")
	f.fail("b1n11")
	f.fail("b2n07")
	d := newDaemon(t, f, "off")
	reconcile(t, d)
	if got := f.spares("b1"); got != nil {
		t.Errorf("two failed nodes leave no spares to keep; got %v", got)
	}
	if got := f.spares("b2"); len(got) != 1 {
		t.Errorf("one failed node leaves one spare to keep; got %v", got)
	}
}

func TestSparesFollowTheIdleNodes(t *testing.T) {
	f := newFake(map[string]int{"b1": 18})
	f.run(1, "alice", nodeRange("b1", 1, 16), 16, "")
	d := newDaemon(t, f, "off")
	reconcile(t, d)
	// A replacement takes both spares; two other nodes free up.
	f.nodes["b1n17"].State = []string{"ALLOCATED"}
	f.nodes["b1n18"].State = []string{"ALLOCATED"}
	f.nodes["b1n03"].State = []string{"IDLE"}
	f.nodes["b1n04"].State = []string{"IDLE"}
	reconcile(t, d)
	if got := f.spares("b1"); !slices.Equal(got, []string{"b1n03", "b1n04"}) {
		t.Errorf("spares should move to the idle nodes; got %v", got)
	}
}

func TestJobRequeuedByANodeFailureIsKeptToItsBlocksWithTheirSpares(t *testing.T) {
	f := newFake(map[string]int{"b1": 18, "b2": 18, "b3": 18})
	f.run(7, "alice", append(nodeRange("b1", 1, 16), nodeRange("b2", 1, 16)...), 16,
		"nvlink-rack.io/preemptible")
	d := newDaemon(t, f, "on")
	reconcile(t, d)

	f.fail("b1n05")
	f.requeue(7)
	reconcile(t, d)
	repl := f.resvs["nvlr-repl-7"]
	if repl == nil {
		t.Fatal("no replacement reservation")
	}
	nodes := slices.Clone(repl.Nodes)
	sort.Strings(nodes)
	if !slices.Equal(nodes, []string{"b1n17", "b1n18", "b2n17", "b2n18"}) ||
		!slices.Equal(repl.Users, []string{"alice"}) || !slices.Contains(repl.Flags, "OVERLAP") ||
		!slices.Contains(repl.Flags, "FLEX") {
		t.Errorf("replacement reservation = %+v", repl)
	}
	u := f.updates[7]
	if u["Reservation"] != "nvlr-repl-7" || u["StartTime"] != "now" ||
		u["ExcNodeList"] != strings.Join(nodeRange("b3", 1, 18), ",") {
		t.Errorf("job update = %v", u)
	}
	if len(f.requeued) != 0 {
		t.Errorf("a replacement must not trigger defrag; requeued %v", f.requeued)
	}

	// Slurm restarts it in its blocks; the reservation goes away.
	f.run(7, "alice", append(append(nodeRange("b1", 1, 4), nodeRange("b1", 6, 17)...),
		nodeRange("b2", 1, 16)...), 16, "")
	f.jobs[7].Restarts = 1
	reconcile(t, d)
	if f.resvs["nvlr-repl-7"] != nil {
		t.Error("replacement reservation should be released once the job runs")
	}
}

func TestRequeueWithoutAFailedNodeIsANewPlacement(t *testing.T) {
	f := newFake(map[string]int{"b1": 18, "b2": 18})
	f.run(7, "alice", nodeRange("b1", 1, 16), 16, "")
	d := newDaemon(t, f, "off")
	reconcile(t, d)
	f.requeue(7) // e.g. `scontrol requeue` by the user
	reconcile(t, d)
	if f.resvs["nvlr-repl-7"] != nil || f.updates[7] != nil {
		t.Errorf("not a replacement: reservations %v, updates %v", f.resvs, f.updates[7])
	}
}

// fragmented: each block has a small restart-safe job, so no block fits 16
// nodes plus 2 spares, though 28 nodes are idle.
func fragmented() *fake {
	f := newFake(map[string]int{"b1": 18, "b2": 18})
	f.run(1, "bob", nodeRange("b1", 1, 4), 0, "nvlink-rack.io/preemptible")
	f.run(2, "bob", nodeRange("b2", 1, 2), 0, "nvlink-rack.io/preemptible")
	f.run(3, "carol", nodeRange("b2", 3, 4), 0, "") // not restart-safe
	f.pend(9, "alice", 16, 16, "Resources")
	return f
}

func TestDefragRequeuesTheCheapestRestartSafeJobAndHoldsItsBlock(t *testing.T) {
	f := fragmented()
	d := newDaemon(t, f, "on")
	reconcile(t, d)
	if !slices.Equal(f.requeued, []int{1}) {
		t.Fatalf("should requeue job 1 (frees b1; job 2 alone wouldn't); requeued %v", f.requeued)
	}
	hold := f.resvs["nvlr-hold-9"]
	if hold == nil || len(hold.Nodes) != 16 || !slices.Equal(hold.Users, []string{"alice"}) ||
		!slices.Contains(hold.Flags, "IGNORE_JOBS") {
		t.Fatalf("hold = %+v", hold)
	}
	for _, n := range hold.Nodes {
		if !strings.HasPrefix(n, "b1") {
			t.Fatalf("hold should be in b1: %v", hold.Nodes)
		}
	}
	if f.updates[9]["Reservation"] != "nvlr-hold-9" {
		t.Errorf("job 9 update = %v", f.updates[9])
	}
	if !d.state.Jobs[1].Evicted {
		t.Error("job 1 should be marked as evicted, not a replacement candidate")
	}

	// The victim's requeue isn't a node failure: no replacement steering.
	reconcile(t, d)
	if f.resvs["nvlr-repl-1"] != nil {
		t.Error("a defrag victim must not get the spares exception")
	}

	// Job 9 starts in its hold; the hold is released.
	f.run(9, "alice", nodeRange("b1", 1, 16), 16, "")
	reconcile(t, d)
	if f.resvs["nvlr-hold-9"] != nil {
		t.Error("hold should be released once the job runs")
	}
}

func TestDefragDryRunAndNonRestartSafeJobsChangeNothing(t *testing.T) {
	f := fragmented()
	reconcile(t, newDaemon(t, f, "dry-run"))
	if len(f.requeued) != 0 || f.resvs["nvlr-hold-9"] != nil {
		t.Fatalf("dry-run changed the cluster: requeued %v", f.requeued)
	}

	f = fragmented()
	f.jobs[1].Comment, f.jobs[2].Comment = "", ""
	reconcile(t, newDaemon(t, f, "on"))
	if len(f.requeued) != 0 {
		t.Fatalf("only restart-safe jobs may be requeued; requeued %v", f.requeued)
	}
}

func TestDefragLeavesCapacityShortagesAlone(t *testing.T) {
	f := fragmented()
	f.pend(9, "alice", 32, 16, "Resources") // 28 idle nodes: not fragmentation
	reconcile(t, newDaemon(t, f, "on"))
	if len(f.requeued) != 0 {
		t.Fatalf("requeued %v for a job that can't fit anyway", f.requeued)
	}
}

func TestDefragIgnoresJobsWithNodeConstraints(t *testing.T) {
	// Found in the live test: a job pinned with --nodelist to nodes that are
	// spares waits with ReqNodeNotAvail. Defrag must not "help" it by
	// freeing other nodes.
	f := fragmented()
	f.jobs[9].Required = "b1n[01-16]"
	reconcile(t, newDaemon(t, f, "on"))
	if len(f.requeued) != 0 || f.resvs["nvlr-hold-9"] != nil {
		t.Fatalf("defrag acted on a node-constrained job: requeued %v", f.requeued)
	}
}

func TestDefragHoldAvoidsTheSpareNodes(t *testing.T) {
	f := fragmented()
	reconcile(t, newDaemon(t, f, "on"))
	spares := f.spares("b1")
	for _, n := range f.resvs["nvlr-hold-9"].Nodes {
		if slices.Contains(spares, n) {
			t.Fatalf("hold %v overlaps spares %v", f.resvs["nvlr-hold-9"].Nodes, spares)
		}
	}
}
