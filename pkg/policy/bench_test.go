package policy

import (
	"fmt"
	"testing"
)

// BenchmarkFill128Racks places sixteen 128-node gangs (8 slices each) onto
// 128 empty 18-node cliques: 2,048 pods on 2,304 nodes.
func BenchmarkFill128Racks(b *testing.B) {
	var nodes []*Node
	for r := 0; r < 128; r++ {
		for i := 0; i < 18; i++ {
			nodes = append(nodes, &Node{Name: fmt.Sprintf("R%03d-%02d", r, i),
				Clique: fmt.Sprintf("R%03d", r), Allocatable: Resources{GPU: 4}, Healthy: true})
		}
	}
	for b.Loop() {
		state := NewState(nodes, nil, 2)
		for j := 0; j < 16; j++ {
			g := &Gang{Key: fmt.Sprintf("ns/job%d", j), Size: 128, SliceSize: 16}
			for r := 0; r < 128; r++ {
				g.Pods = append(g.Pods, &Pod{Key: fmt.Sprintf("ns/job%d-%d", j, r),
					Gang: g.Key, Rank: r, Requests: Resources{GPU: 4}})
			}
			if plan := PlanGang(state, g); len(plan.Assignment) != 128 {
				b.Fatalf("job%d: placed %d of 128", j, len(plan.Assignment))
			}
		}
	}
}
