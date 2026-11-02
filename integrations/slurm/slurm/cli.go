package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// CLI implements Client with scontrol and squeue, run on a host with Slurm
// admin rights (the daemon creates reservations and updates other users'
// jobs).
type CLI struct {
	Scontrol string // default "scontrol"
	Squeue   string // default "squeue"
}

func (c CLI) run(bin string, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s %s: %v: %s", bin, strings.Join(args, " "), err,
			strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (c CLI) scontrol(args ...string) ([]byte, error) {
	return c.run(or(c.Scontrol, "scontrol"), args...)
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// Topology parses `scontrol show topology` (topology/block); the node JSON
// doesn't carry block membership.
func (c CLI) Topology() ([]Block, error) {
	out, err := c.scontrol("show", "topology")
	if err != nil {
		return nil, err
	}
	return ParseTopology(string(out))
}

// ParseTopology reads the BlockName=... Nodes=... lines of
// `scontrol show topology`.
func ParseTopology(text string) ([]Block, error) {
	var blocks []Block
	for _, line := range strings.Split(text, "\n") {
		fields := map[string]string{}
		for _, f := range strings.Fields(line) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		if fields["BlockName"] == "" || fields["Nodes"] == "" {
			continue
		}
		nodes, err := ExpandHostlist(fields["Nodes"])
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, Block{Name: fields["BlockName"], Nodes: nodes})
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("no blocks in `scontrol show topology`; is TopologyPlugin=topology/block?")
	}
	return blocks, nil
}

type number struct {
	Number int `json:"number"`
}

func (c CLI) Nodes() ([]Node, error) {
	out, err := c.scontrol("show", "nodes", "--json")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Nodes []struct {
			Name        string   `json:"name"`
			State       []string `json:"state"`
			Reason      string   `json:"reason"`
			Reservation string   `json:"reservation"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return nil, fmt.Errorf("scontrol show nodes --json: %w", err)
	}
	nodes := make([]Node, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		nodes = append(nodes, Node{Name: n.Name, State: n.State, Reason: n.Reason,
			Reservation: n.Reservation})
	}
	return nodes, nil
}

func (c CLI) Jobs() ([]Job, error) {
	out, err := c.run(or(c.Squeue, "squeue"), "--json")
	if err != nil {
		return nil, err
	}
	return ParseJobs(out)
}

// ParseJobs reads `squeue --json`.
func ParseJobs(data []byte) ([]Job, error) {
	var doc struct {
		Jobs []struct {
			JobID       int      `json:"job_id"`
			Name        string   `json:"name"`
			UserName    string   `json:"user_name"`
			JobState    []string `json:"job_state"`
			StateReason string   `json:"state_reason"`
			Nodes       string   `json:"nodes"`
			NodeCount   number   `json:"node_count"`
			SegmentSize int      `json:"segment_size"`
			Requeue     bool     `json:"requeue"`
			RestartCnt  int      `json:"restart_cnt"`
			Comment     string   `json:"comment"`
			Priority    number   `json:"priority"`
			ResvName    string   `json:"resv_name"`
			Excluded    string   `json:"excluded_nodes"`
			Required    string   `json:"required_nodes"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("squeue --json: %w", err)
	}
	jobs := make([]Job, 0, len(doc.Jobs))
	for _, j := range doc.Jobs {
		nodes, err := ExpandHostlist(j.Nodes)
		if err != nil {
			return nil, err
		}
		state := ""
		if len(j.JobState) > 0 {
			state = j.JobState[0]
		}
		jobs = append(jobs, Job{
			ID: j.JobID, Name: j.Name, User: j.UserName, State: state, Reason: j.StateReason,
			Nodes: nodes, NodeCount: j.NodeCount.Number, Segment: j.SegmentSize,
			Requeue: j.Requeue, Restarts: j.RestartCnt, Comment: j.Comment,
			Priority: j.Priority.Number, Reservation: j.ResvName, Excluded: j.Excluded,
			Required: j.Required,
		})
	}
	return jobs, nil
}

func (c CLI) Reservations() ([]Reservation, error) {
	out, err := c.scontrol("show", "reservation", "--json")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Reservations []struct {
			Name     string   `json:"name"`
			NodeList string   `json:"node_list"`
			Flags    []string `json:"flags"`
			Users    string   `json:"users"`
		} `json:"reservations"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return nil, fmt.Errorf("scontrol show reservation --json: %w", err)
	}
	var resvs []Reservation
	for _, r := range doc.Reservations {
		nodes, err := ExpandHostlist(r.NodeList)
		if err != nil {
			return nil, err
		}
		var users []string
		if r.Users != "" {
			users = strings.Split(r.Users, ",")
		}
		resvs = append(resvs, Reservation{Name: r.Name, Nodes: nodes, Flags: r.Flags, Users: users})
	}
	return resvs, nil
}

func (c CLI) CreateReservation(r Reservation) error {
	args := []string{"create", "reservation", "ReservationName=" + r.Name, "StartTime=now",
		"Duration=infinite", "Nodes=" + strings.Join(r.Nodes, ","),
		"Users=" + strings.Join(r.Users, ",")}
	if len(r.Flags) > 0 {
		args = append(args, "Flags="+strings.Join(r.Flags, ","))
	}
	_, err := c.scontrol(args...)
	return err
}

func (c CLI) UpdateReservation(r Reservation) error {
	_, err := c.scontrol("update", "reservation", "ReservationName="+r.Name,
		"Nodes="+strings.Join(r.Nodes, ","), "Users="+strings.Join(r.Users, ","))
	return err
}

func (c CLI) DeleteReservation(name string) error {
	_, err := c.scontrol("delete", "ReservationName="+name)
	return err
}

func (c CLI) UpdateJob(id int, fields map[string]string) error {
	args := []string{"update", "JobId=" + strconv.Itoa(id)}
	for _, k := range []string{"Reservation", "ExcNodeList", "StartTime"} {
		if v, ok := fields[k]; ok {
			args = append(args, k+"="+v)
		}
	}
	_, err := c.scontrol(args...)
	return err
}

func (c CLI) Requeue(id int) error {
	_, err := c.scontrol("requeue", strconv.Itoa(id))
	return err
}
