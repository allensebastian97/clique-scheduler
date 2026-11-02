package slurm

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// testdata/ holds real output from Slurm 26.05.5 (trimmed to the fields the
// daemon reads), captured from a 3-block topology/block cluster.

func TestExpandHostlist(t *testing.T) {
	for in, want := range map[string][]string{
		"":                       nil,
		"b1n05":                  {"b1n05"},
		"b1n[01-03]":             {"b1n01", "b1n02", "b1n03"},
		"b2n[01-02,17-18],b3n09": {"b2n01", "b2n02", "b2n17", "b2n18", "b3n09"},
		"r[1-2]n[08-09]":         {"r1n08", "r1n09", "r2n08", "r2n09"},
		"gpu[9-11]":              {"gpu9", "gpu10", "gpu11"},
	} {
		got, err := ExpandHostlist(in)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("ExpandHostlist(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"b1n[01-", "b1n[05-01]", "b1n[x]"} {
		if _, err := ExpandHostlist(bad); err == nil {
			t.Errorf("ExpandHostlist(%q) should fail", bad)
		}
	}
}

func TestParseTopology(t *testing.T) {
	data, _ := os.ReadFile("testdata/topology.txt")
	blocks, err := ParseTopology(string(data))
	if err != nil || len(blocks) != 3 || blocks[1].Name != "b2" || len(blocks[1].Nodes) != 18 ||
		blocks[1].Nodes[0] != "b2n01" {
		t.Fatalf("blocks = %+v, %v", blocks, err)
	}
	if _, err := ParseTopology("SwitchName=s1 Nodes=n[1-4]\n"); err == nil {
		t.Fatal("a tree topology should be rejected")
	}
}

func TestParseJobs(t *testing.T) {
	data, _ := os.ReadFile("testdata/squeue.json")
	jobs, err := ParseJobs(data)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs = %+v, %v", jobs, err)
	}
	j := jobs[0]
	if j.ID != 5 || j.State != "RUNNING" || j.NodeCount != 32 || j.Segment != 16 ||
		!j.Requeue || j.Restarts != 1 || j.User == "" || j.Reservation != "nvlr-repl-5" ||
		len(j.Nodes) != 32 || j.Nodes[14] != "b2n17" || j.Excluded != "b1n[01-18]" {
		t.Fatalf("job 5 = %+v", j)
	}
}

func TestParseNodesAndReservations(t *testing.T) {
	var nodes struct {
		Nodes []struct {
			Name        string   `json:"name"`
			State       []string `json:"state"`
			Reservation string   `json:"reservation"`
		} `json:"nodes"`
	}
	data, _ := os.ReadFile("testdata/nodes.json")
	if err := json.Unmarshal(data, &nodes); err != nil {
		t.Fatal(err)
	}
	idle := map[string]bool{}
	for _, n := range nodes.Nodes {
		idle[n.Name] = Node{Name: n.Name, State: n.State}.Idle()
	}
	want := map[string]bool{"b1n01": false, "b1n03": true, "b2n05": false, "b2n15": true,
		"b2n17": false, "b3n15": true}
	if !reflect.DeepEqual(idle, want) {
		t.Fatalf("idle = %v, want %v", idle, want)
	}
	if (Node{State: []string{"IDLE", "DRAIN"}}).Healthy() || (Node{State: []string{"DOWN"}}).Idle() {
		t.Fatal("drained and down nodes are not healthy")
	}

	data, _ = os.ReadFile("testdata/reservations.json")
	if !strings.Contains(string(data), `"nvlr-repl-5"`) || !strings.Contains(string(data), "OVERLAP") {
		t.Fatal("testdata should include the replacement reservation")
	}
}
