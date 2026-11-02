// Package slurm is the slice of Slurm the daemon needs: topology blocks,
// nodes, jobs and reservations, read with --json and changed with scontrol.
package slurm

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Block is a topology/block block: one NVLink domain.
type Block struct {
	Name  string
	Nodes []string
}

// Node is a compute node.
type Node struct {
	Name        string
	State       []string // e.g. IDLE, ALLOCATED, DOWN, DRAIN, RESERVED
	Reason      string
	Reservation string
}

func (n Node) has(state string) bool { return slices.Contains(n.State, state) }

// Healthy reports whether the node can run jobs.
func (n Node) Healthy() bool {
	for _, bad := range []string{"DOWN", "DRAIN", "FAIL", "NOT_RESPONDING", "INVALID",
		"FUTURE", "POWERED_DOWN", "MAINTENANCE"} {
		if n.has(bad) {
			return false
		}
	}
	return true
}

// Idle reports whether the node is healthy and has no job on it.
func (n Node) Idle() bool {
	return n.Healthy() && n.has("IDLE") && !n.has("COMPLETING")
}

// Job is a Slurm job.
type Job struct {
	ID          int
	Name        string
	User        string
	State       string // first job_state entry, e.g. PENDING, RUNNING
	Reason      string
	Nodes       []string
	NodeCount   int
	Segment     int // --segment, 0 if unset
	Requeue     bool
	Restarts    int
	Comment     string
	Priority    int
	Reservation string
	Excluded    string
	Required    string // --nodelist
}

// Reservation is an advanced reservation.
type Reservation struct {
	Name  string
	Nodes []string
	Flags []string
	Users []string
}

// Client reads and changes cluster state.
type Client interface {
	Topology() ([]Block, error)
	Nodes() ([]Node, error)
	Jobs() ([]Job, error)
	Reservations() ([]Reservation, error)
	CreateReservation(r Reservation) error
	UpdateReservation(r Reservation) error
	DeleteReservation(name string) error
	// UpdateJob sets job fields such as Reservation, ExcNodeList, StartTime.
	UpdateJob(id int, fields map[string]string) error
	Requeue(id int) error
}

// ExpandHostlist expands a Slurm hostlist such as "b1n[01-03,07],x5" into
// node names.
func ExpandHostlist(list string) ([]string, error) {
	var out []string
	for _, part := range splitTopLevel(list) {
		names, err := expandOne(part)
		if err != nil {
			return nil, fmt.Errorf("hostlist %q: %w", list, err)
		}
		out = append(out, names...)
	}
	return out, nil
}

func splitTopLevel(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func expandOne(p string) ([]string, error) {
	open := strings.IndexByte(p, '[')
	if open < 0 {
		return []string{p}, nil
	}
	end := strings.IndexByte(p[open:], ']')
	if end < 0 {
		return nil, fmt.Errorf("unbalanced brackets")
	}
	end += open
	prefix, inner := p[:open], p[open+1:end]
	suffixes, err := expandOne(p[end+1:])
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range strings.Split(inner, ",") {
		lo, hi, isRange := strings.Cut(r, "-")
		if !isRange {
			hi = lo
		}
		a, err1 := strconv.Atoi(lo)
		b, err2 := strconv.Atoi(hi)
		if err1 != nil || err2 != nil || b < a {
			return nil, fmt.Errorf("bad range %q", r)
		}
		for n := a; n <= b; n++ {
			for _, s := range suffixes {
				out = append(out, fmt.Sprintf("%s%0*d%s", prefix, len(lo), n, s))
			}
		}
	}
	return out, nil
}
