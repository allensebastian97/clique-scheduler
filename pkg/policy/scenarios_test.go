package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The shared scenarios pin this package to the Python reference
// implementation; see reference/python/tools/scenarios.py for the format.

type scenario struct {
	Description string                       `json:"description"`
	Reserve     int                          `json:"reserve"`
	GPUsPerNode float64                      `json:"gpus_per_node"`
	Cliques     orderedCounts                `json:"cliques"`
	Unhealthy   []string                     `json:"unhealthy"`
	Labels      map[string]map[string]string `json:"labels"`
	Taints      map[string][]scenarioTaint   `json:"taints"`
	Gangs       []scenarioGang               `json:"gangs"`
	Op          string                       `json:"op"`
	Gang        string                       `json:"gang"`
	Expect      json.RawMessage              `json:"expect"`
}

type scenarioTaint struct {
	Key, Value, Effect string
}

type scenarioToleration struct {
	Key, Operator, Value, Effect string
}

type scenarioGang struct {
	Name         string               `json:"name"`
	Size         int                  `json:"size"`
	SliceSize    *int                 `json:"slice_size"`
	GPUs         *float64             `json:"gpus"`
	Preemptible  bool                 `json:"preemptible"`
	Priority     int                  `json:"priority"`
	Nodes        []*string            `json:"nodes"`
	NodeSelector map[string]string    `json:"node_selector"`
	Tolerations  []scenarioToleration `json:"tolerations"`
}

// orderedCounts keeps the JSON key order: node order matters to
// FitsIgnoringTopology, exactly as dict order does in Python.
type orderedCounts struct {
	keys   []string
	counts map[string]int
}

func (o *orderedCounts) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if _, err := dec.Token(); err != nil { // {
		return err
	}
	o.counts = map[string]int{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		key := tok.(string)
		var n int
		if err := dec.Decode(&n); err != nil {
			return err
		}
		o.keys = append(o.keys, key)
		o.counts[key] = n
	}
	return nil
}

func (sc *scenario) build() (*State, []*Gang) {
	unhealthy := map[string]bool{}
	for _, n := range sc.Unhealthy {
		unhealthy[n] = true
	}
	gpus := sc.GPUsPerNode
	if gpus == 0 {
		gpus = 4
	}
	var nodes []*Node
	for _, clique := range sc.Cliques.keys {
		for i := 0; i < sc.Cliques.counts[clique]; i++ {
			name := fmt.Sprintf("%s%02d", clique, i)
			var taints []Taint
			for _, t := range sc.Taints[name] {
				taints = append(taints, Taint(t))
			}
			nodes = append(nodes, &Node{
				Name: name, Clique: clique, Allocatable: Resources{GPU: gpus},
				Healthy: !unhealthy[name], Labels: sc.Labels[name], Taints: taints,
			})
		}
	}
	var gangs []*Gang
	for _, sg := range sc.Gangs {
		sliceSize, podGPUs := 16, 4.0
		if sg.SliceSize != nil {
			sliceSize = *sg.SliceSize
		}
		if sg.GPUs != nil {
			podGPUs = *sg.GPUs
		}
		var tolerations []Toleration
		for _, t := range sg.Tolerations {
			tolerations = append(tolerations, Toleration(t))
		}
		g := &Gang{Key: "ns/" + sg.Name, Size: sg.Size, SliceSize: sliceSize,
			Priority: sg.Priority, Preemptible: sg.Preemptible}
		for r := 0; r < sg.Size; r++ {
			node := ""
			if sg.Nodes != nil && sg.Nodes[r] != nil {
				node = *sg.Nodes[r]
			}
			g.Pods = append(g.Pods, &Pod{
				Key: fmt.Sprintf("ns/%s-%d", sg.Name, r), Gang: g.Key, Rank: r,
				Requests: Resources{GPU: podGPUs}, Node: node,
				NodeSelector: sg.NodeSelector, Tolerations: tolerations,
			})
		}
		gangs = append(gangs, g)
	}
	state := NewState(nodes, nil, sc.Reserve)
	for _, g := range gangs {
		for _, p := range g.Pods {
			if p.Node != "" {
				state.Assign(p, p.Node)
			}
		}
	}
	return state, gangs
}

type gangResult struct {
	Assignment  map[string]string `json:"assignment"`
	StuckSlices []int             `json:"stuck_slices"`
	Blocked     bool              `json:"blocked"`
}

type defragResult struct {
	Victims    []string          `json:"victims"`
	GPUs       float64           `json:"gpus"`
	Assignment map[string]string `json:"assignment"`
}

func (sc *scenario) run(t *testing.T) any {
	state, gangs := sc.build()
	var target *Gang
	for _, g := range gangs {
		if g.Key == "ns/"+sc.Gang {
			target = g
		}
	}
	if target == nil {
		t.Fatalf("no gang %q", sc.Gang)
	}
	switch sc.Op {
	case "plan_gang":
		plan := PlanGang(state, target)
		return gangResult{Assignment: plan.Assignment, StuckSlices: plan.StuckSlices,
			Blocked: plan.Blocked}
	case "plan_defrag":
		plan := PlanDefrag(state, target, gangs)
		if plan == nil {
			return (*defragResult)(nil)
		}
		res := &defragResult{GPUs: plan.GPUs, Assignment: plan.Assignment}
		for _, v := range plan.Victims {
			res.Victims = append(res.Victims, v.Key)
		}
		return res
	case "fits_ignoring_topology":
		var pending []*Pod
		for _, p := range target.Pods {
			if p.Node == "" {
				pending = append(pending, p)
			}
		}
		return FitsIgnoringTopology(state, pending)
	}
	t.Fatalf("unknown op %q", sc.Op)
	return nil
}

func TestScenarios(t *testing.T) {
	files, err := filepath.Glob("../../testdata/scenarios/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no scenarios found (err=%v)", err)
	}
	sort.Strings(files)
	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".json")
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var sc scenario
			if err := json.Unmarshal(data, &sc); err != nil {
				t.Fatal(err)
			}
			got := sc.run(t)
			want := reflect.New(reflect.TypeOf(got)).Interface()
			if err := json.Unmarshal(sc.Expect, want); err != nil {
				t.Fatal(err)
			}
			wantVal := reflect.ValueOf(want).Elem().Interface()
			if !reflect.DeepEqual(normalize(got), normalize(wantVal)) {
				gotJSON, _ := json.Marshal(got)
				t.Errorf("%s\n got: %s\nwant: %s", sc.Description, gotJSON, sc.Expect)
			}
		})
	}
}

// normalize treats nil and empty maps/slices alike.
func normalize(v any) any {
	data, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(data, &out)
	return dropEmpty(out)
}

func dropEmpty(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			val = dropEmpty(val)
			if val == nil {
				continue
			}
			if m, ok := val.(map[string]any); ok && len(m) == 0 {
				continue
			}
			if s, ok := val.([]any); ok && len(s) == 0 {
				continue
			}
			out[k] = val
		}
		return out
	case []any:
		for i := range x {
			x[i] = dropEmpty(x[i])
		}
		return x
	}
	return v
}
