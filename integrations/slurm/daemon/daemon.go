// Package daemon keeps NVLink-rack spares, same-block replacement and
// defragmentation around Slurm's topology/block plugin. Slurm places jobs
// (and --segment keeps each segment inside one block); the daemon only
// adjusts reservations and pending jobs:
//
//   - Spares: one reservation per block, <prefix>-spares-<block>, holding up
//     to `spares` idle nodes, minus the block's failed nodes. Which nodes
//     doesn't matter, only how many: every pass re-points the reservation at
//     whatever is idle.
//   - Replacement: a job requeued because one of its nodes failed is limited
//     to the blocks it ran in, given a reservation over those blocks' spares
//     (OVERLAP,FLEX: spares plus ordinary free nodes), and made eligible
//     immediately instead of after Slurm's requeue delay.
//   - Defrag: a job waiting only because of fragmentation gets the cheapest
//     restart-safe jobs requeued (pkg/policy.PlanDefrag) and a reservation
//     holding the freed nodes for it.
//
// Only jobs requeued by a node failure get the spares exception. Jobs the
// daemon requeued for defrag, or a user requeued, are new placements.
package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/allensebastian97/nvlink-rack-scheduler/integrations/slurm/slurm"
	"github.com/allensebastian97/nvlink-rack-scheduler/pkg/policy"
)

// Config tunes the daemon.
type Config struct {
	Spares int
	// Prefix names the daemon's reservations: <prefix>-spares-<block>,
	// <prefix>-repl-<job>, <prefix>-hold-<job>.
	Prefix string
	// Preemptible marks restart-safe jobs: requeueable jobs whose comment
	// contains it may be requeued for defrag.
	Preemptible string
	Defrag      string // off | dry-run | on
	// HoldFor bounds how long a defrag hold waits for its job.
	HoldFor time.Duration
	// StatePath persists job records across restarts ("" keeps them in memory).
	StatePath string
	Admin     string // user listed on spare reservations; default root
}

// Record is what the daemon remembers about a job.
type Record struct {
	Nodes    []string `json:"nodes"`
	Blocks   []string `json:"blocks"`
	Restarts int      `json:"restarts"`
	// Evicted marks a job the daemon requeued for defrag.
	Evicted bool `json:"evicted,omitempty"`
	// Steered marks a replacement given its blocks and spares.
	Steered bool `json:"steered,omitempty"`
}

// Hold is a defrag reservation waiting for its job.
type Hold struct {
	Reservation string    `json:"reservation"`
	Until       time.Time `json:"until"`
}

type persisted struct {
	Jobs  map[int]*Record `json:"jobs"`
	Holds map[int]*Hold   `json:"holds"`
}

// Daemon reconciles one cluster.
type Daemon struct {
	client  slurm.Client
	cfg     Config
	now     func() time.Time
	state   persisted
	blocks  []slurm.Block
	quiet   map[string]time.Time
	retryAt map[int]time.Time
}

// New builds a daemon, loading state from cfg.StatePath if it exists.
func New(client slurm.Client, cfg Config, now func() time.Time) (*Daemon, error) {
	if cfg.Prefix == "" {
		cfg.Prefix = "nvlr"
	}
	if cfg.Admin == "" {
		cfg.Admin = "root"
	}
	d := &Daemon{client: client, cfg: cfg, now: now, quiet: map[string]time.Time{},
		retryAt: map[int]time.Time{},
		state:   persisted{Jobs: map[int]*Record{}, Holds: map[int]*Hold{}}}
	if cfg.StatePath != "" {
		data, err := os.ReadFile(cfg.StatePath)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return nil, err
		default:
			if err := json.Unmarshal(data, &d.state); err != nil {
				return nil, fmt.Errorf("%s: %w", cfg.StatePath, err)
			}
		}
	}
	return d, nil
}

type view struct {
	nodes   map[string]slurm.Node
	blockOf map[string]string
	jobs    map[int]slurm.Job
	resvs   map[string]slurm.Reservation
}

// Reconcile runs one pass.
func (d *Daemon) Reconcile() error {
	if d.blocks == nil {
		blocks, err := d.client.Topology()
		if err != nil {
			return err
		}
		d.blocks = blocks
	}
	v, err := d.read()
	if err != nil {
		return err
	}
	d.track(v)
	d.steerReplacements(v)
	d.expireHolds(v)
	if v, err = d.read(); err != nil { // replacements took reservations
		return err
	}
	d.keepSpares(v)
	if d.cfg.Defrag != "off" {
		if v, err = d.read(); err != nil {
			return err
		}
		d.defrag(v)
	}
	return d.save()
}

func (d *Daemon) read() (view, error) {
	nodes, err := d.client.Nodes()
	if err != nil {
		return view{}, err
	}
	jobs, err := d.client.Jobs()
	if err != nil {
		return view{}, err
	}
	resvs, err := d.client.Reservations()
	if err != nil {
		return view{}, err
	}
	v := view{nodes: map[string]slurm.Node{}, blockOf: map[string]string{},
		jobs: map[int]slurm.Job{}, resvs: map[string]slurm.Reservation{}}
	for _, n := range nodes {
		v.nodes[n.Name] = n
	}
	for _, b := range d.blocks {
		for _, n := range b.Nodes {
			v.blockOf[n] = b.Name
		}
	}
	for _, j := range jobs {
		v.jobs[j.ID] = j
	}
	for _, r := range resvs {
		v.resvs[r.Name] = r
	}
	return v, nil
}

func (d *Daemon) spareName(block string) string { return d.cfg.Prefix + "-spares-" + block }
func (d *Daemon) replName(job int) string       { return d.cfg.Prefix + "-repl-" + strconv.Itoa(job) }
func (d *Daemon) holdName(job int) string       { return d.cfg.Prefix + "-hold-" + strconv.Itoa(job) }

// track records where running jobs are; Slurm clears a job's nodes when it
// is requeued.
func (d *Daemon) track(v view) {
	for id := range d.state.Jobs {
		if _, ok := v.jobs[id]; !ok {
			delete(d.state.Jobs, id)
			d.deleteReservation(v, d.replName(id))
		}
	}
	for _, j := range v.jobs {
		if j.State != "RUNNING" || len(j.Nodes) == 0 {
			continue
		}
		rec := d.state.Jobs[j.ID]
		if rec != nil && rec.Steered {
			log.Printf("job %d: replacement running on %s", j.ID, strings.Join(blocksOf(v, j.Nodes), ","))
			d.deleteReservation(v, d.replName(j.ID))
		}
		d.state.Jobs[j.ID] = &Record{Nodes: j.Nodes, Blocks: blocksOf(v, j.Nodes), Restarts: j.Restarts}
	}
}

func blocksOf(v view, nodes []string) []string {
	var blocks []string
	for _, n := range nodes {
		if b := v.blockOf[n]; b != "" && !slices.Contains(blocks, b) {
			blocks = append(blocks, b)
		}
	}
	sort.Strings(blocks)
	return blocks
}

// steerReplacements sends jobs requeued by a node failure back to their
// blocks, with their blocks' spares.
func (d *Daemon) steerReplacements(v view) {
	for id, rec := range d.state.Jobs {
		j, ok := v.jobs[id]
		if !ok || j.State != "PENDING" || rec.Steered || rec.Evicted || j.Restarts <= rec.Restarts {
			continue
		}
		failed := ""
		for _, n := range rec.Nodes {
			if node, ok := v.nodes[n]; ok && !node.Healthy() {
				failed = n
				break
			}
		}
		if failed == "" {
			rec.Restarts = j.Restarts // Requeued for another reason: a new placement.
			continue
		}
		var spares, outside []string
		for _, b := range rec.Blocks {
			spares = append(spares, v.resvs[d.spareName(b)].Nodes...)
		}
		for _, b := range d.blocks {
			if !slices.Contains(rec.Blocks, b.Name) {
				outside = append(outside, b.Nodes...)
			}
		}
		fields := map[string]string{"StartTime": "now", "ExcNodeList": strings.Join(outside, ",")}
		if len(spares) > 0 {
			if err := d.client.CreateReservation(slurm.Reservation{Name: d.replName(id),
				Nodes: spares, Users: []string{j.User},
				Flags: []string{"OVERLAP", "FLEX", "NO_HOLD_JOBS_AFTER"}}); err != nil {
				log.Printf("job %d: replacement reservation: %v", id, err)
				continue
			}
			fields["Reservation"] = d.replName(id)
		}
		if err := d.client.UpdateJob(id, fields); err != nil {
			log.Printf("job %d: steering replacement: %v", id, err)
			continue
		}
		rec.Steered = true
		log.Printf("job %d: requeued after %s failed; kept to blocks %s with %d spare node(s)",
			id, failed, strings.Join(rec.Blocks, ","), len(spares))
	}
}

func (d *Daemon) expireHolds(v view) {
	for id, h := range d.state.Holds {
		j, ok := v.jobs[id]
		switch {
		case !ok || j.State != "PENDING":
			if ok {
				log.Printf("job %d: started; releasing defrag hold", id)
			}
		case d.now().After(h.Until):
			log.Printf("job %d: defrag hold expired before the job started", id)
		default:
			continue
		}
		d.deleteReservation(v, h.Reservation)
		delete(d.state.Holds, id)
	}
}

func (d *Daemon) deleteReservation(v view, name string) {
	if _, ok := v.resvs[name]; !ok {
		return
	}
	if err := d.client.DeleteReservation(name); err != nil {
		log.Printf("delete reservation %s: %v", name, err)
	}
}

// keepSpares points each block's spare reservation at up to
// max(0, spares - failed) idle nodes not held by any other reservation.
func (d *Daemon) keepSpares(v view) {
	for _, b := range d.blocks {
		name := d.spareName(b.Name)
		current := v.resvs[name]
		failed := 0
		var candidates []string
		for _, n := range b.Nodes {
			node, ok := v.nodes[n]
			if !ok || !node.Healthy() {
				failed++
				continue
			}
			if node.Idle() && (node.Reservation == "" || node.Reservation == name) {
				candidates = append(candidates, n)
			}
		}
		want := max(0, d.cfg.Spares-failed)
		// Keep current spares first so reservations don't churn, then take
		// from the high end of the block: Slurm allocates low-numbered nodes
		// first, and so do users pinning nodes with --nodelist.
		slices.Reverse(candidates)
		sort.SliceStable(candidates, func(i, k int) bool {
			return slices.Contains(current.Nodes, candidates[i]) &&
				!slices.Contains(current.Nodes, candidates[k])
		})
		chosen := candidates[:min(want, len(candidates))]
		sort.Strings(chosen)
		have := slices.Clone(current.Nodes)
		sort.Strings(have)
		switch {
		case slices.Equal(chosen, have):
		case len(chosen) == 0:
			d.deleteReservation(v, name)
		case current.Name == "":
			if err := d.client.CreateReservation(slurm.Reservation{Name: name, Nodes: chosen,
				Users: []string{d.cfg.Admin}, Flags: []string{"FLEX"}}); err != nil {
				log.Printf("block %s: create spares: %v", b.Name, err)
			}
		default:
			if err := d.client.UpdateReservation(slurm.Reservation{Name: name, Nodes: chosen,
				Users: current.Users}); err != nil {
				log.Printf("block %s: update spares: %v", b.Name, err)
			}
		}
	}
}

// defrag frees a block for jobs blocked by fragmentation.
func (d *Daemon) defrag(v view) {
	state, gangs := d.policyState(v)
	pending := make([]slurm.Job, 0)
	for _, j := range v.jobs {
		// "ReqNodeNotAvail" covers jobs whose only free nodes are reserved
		// (for example, spares); the fit checks below filter out real
		// capacity shortages.
		waiting := j.Reason == "Resources" || strings.HasPrefix(j.Reason, "ReqNodeNotAvail")
		// Node lists and reservations are constraints the planner doesn't
		// model; it would free the wrong nodes.
		constrained := j.Required != "" || j.Excluded != "" || j.Reservation != ""
		if j.State == "PENDING" && waiting && !constrained && d.state.Holds[j.ID] == nil &&
			!d.steered(j.ID) {
			pending = append(pending, j)
		}
	}
	sort.Slice(pending, func(i, k int) bool {
		if pending[i].Priority != pending[k].Priority {
			return pending[i].Priority > pending[k].Priority
		}
		return pending[i].ID < pending[k].ID
	})
	for _, j := range pending {
		if d.now().Before(d.retryAt[j.ID]) {
			continue // A defrag action for this job failed recently.
		}
		blocked, ok := d.gang(j, v)
		if !ok || !policy.FitsIgnoringTopology(state, blocked.Pods) {
			continue
		}
		plan := policy.PlanDefrag(state, blocked, gangs)
		if plan == nil {
			continue
		}
		var victims []string
		for _, g := range plan.Victims {
			victims = append(victims, g.Key)
		}
		targets := slices.Sorted(maps(plan.Assignment))
		if d.cfg.Defrag != "on" {
			if d.due(fmt.Sprintf("dry-run %d", j.ID)) {
				log.Printf("job %d: defrag would requeue %s and hold %d node(s) [dry-run]",
					j.ID, strings.Join(victims, ","), len(targets))
			}
			continue
		}
		if err := d.client.CreateReservation(slurm.Reservation{Name: d.holdName(j.ID),
			Nodes: targets, Users: []string{j.User},
			Flags: []string{"IGNORE_JOBS", "NO_HOLD_JOBS_AFTER"}}); err != nil {
			log.Printf("job %d: defrag hold: %v (retrying in a minute)", j.ID, err)
			d.retryAt[j.ID] = d.now().Add(time.Minute)
			continue
		}
		for _, g := range plan.Victims {
			id, _ := strconv.Atoi(g.Key)
			if rec := d.state.Jobs[id]; rec != nil {
				rec.Evicted = true
			}
			if err := d.client.Requeue(id); err != nil {
				log.Printf("job %d: requeue for defrag: %v", id, err)
			}
		}
		if err := d.client.UpdateJob(j.ID, map[string]string{"Reservation": d.holdName(j.ID)}); err != nil {
			log.Printf("job %d: attach defrag hold: %v", j.ID, err)
		}
		d.state.Holds[j.ID] = &Hold{Reservation: d.holdName(j.ID), Until: d.now().Add(d.cfg.HoldFor)}
		log.Printf("job %d: defrag requeued %s (%g nodes) and holds %s", j.ID,
			strings.Join(victims, ","), plan.GPUs, strings.Join(targets, ","))
		return // One at a time: the next pass sees the new cluster state.
	}
}

func (d *Daemon) steered(id int) bool {
	rec := d.state.Jobs[id]
	return rec != nil && rec.Steered
}

func maps(a policy.Assignment) func(func(string) bool) {
	return func(yield func(string) bool) {
		for _, n := range a {
			if !yield(n) {
				return
			}
		}
	}
}

// policyState models the cluster for pkg/policy: one clique per block, one
// unit of "GPU" per node (jobs get whole nodes). The spare reservations
// already are the spares policy (failed nodes included), so the planner
// keeps no reserve of its own and treats every reserved node, spare or not,
// as off limits. Re-deriving spares would pick different nodes than the
// reservations hold.
func (d *Daemon) policyState(v view) (*policy.State, []*policy.Gang) {
	var nodes []*policy.Node
	used := map[string]policy.Resources{}
	blocked := map[string]bool{}
	for _, b := range d.blocks {
		for _, n := range b.Nodes {
			node, ok := v.nodes[n]
			nodes = append(nodes, &policy.Node{Name: n, Clique: b.Name,
				Allocatable: policy.Resources{policy.GPU: 1}, Healthy: ok && node.Healthy()})
			if ok && node.Healthy() && !node.Idle() {
				used[n] = policy.Resources{policy.GPU: 1}
			}
			if ok && node.Reservation != "" {
				blocked[n] = true
			}
		}
	}
	state := policy.NewState(nodes, used, 0)
	state.Blocked = blocked
	var gangs []*policy.Gang
	for _, j := range v.jobs {
		if j.State != "RUNNING" {
			continue
		}
		if g, ok := d.gang(j, v); ok {
			gangs = append(gangs, g)
		}
	}
	sort.Slice(gangs, func(i, k int) bool { return gangs[i].Key < gangs[k].Key })
	return state, gangs
}

// gang turns a job into a policy gang: one pod per node, cut into slices of
// its --segment (or one slice if it fits a block).
func (d *Daemon) gang(j slurm.Job, v view) (*policy.Gang, bool) {
	size := j.NodeCount
	if size == 0 {
		return nil, false
	}
	slice := size
	if j.Segment > 0 && j.Segment < size {
		slice = j.Segment
	} else if size > d.largestBlock() {
		return nil, false // Slurm spreads it over blocks its own way; not modelled.
	}
	g := &policy.Gang{Key: strconv.Itoa(j.ID), Size: size, SliceSize: slice,
		Priority: j.Priority, Preemptible: j.Requeue && d.cfg.Preemptible != "" &&
			strings.Contains(j.Comment, d.cfg.Preemptible)}
	for r := 0; r < size; r++ {
		p := &policy.Pod{Key: fmt.Sprintf("%d-%d", j.ID, r), Gang: g.Key, Rank: r,
			Requests: policy.Resources{policy.GPU: 1}}
		if r < len(j.Nodes) {
			p.Node = j.Nodes[r]
		}
		g.Pods = append(g.Pods, p)
	}
	return g, true
}

func (d *Daemon) largestBlock() int {
	n := 0
	for _, b := range d.blocks {
		n = max(n, len(b.Nodes))
	}
	return n
}

// due is true at most once a minute per key, to keep logs readable.
func (d *Daemon) due(key string) bool {
	if d.now().Before(d.quiet[key]) {
		return false
	}
	d.quiet[key] = d.now().Add(time.Minute)
	return true
}

func (d *Daemon) save() error {
	if d.cfg.StatePath == "" {
		return nil
	}
	data, err := json.MarshalIndent(d.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := d.cfg.StatePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(d.cfg.StatePath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, d.cfg.StatePath)
}
