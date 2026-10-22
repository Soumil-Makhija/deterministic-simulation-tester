package sim_test

import (
	"fmt"
	"github.com/Soumil-Makhija/deterministic-simulation-tester/sim"
)

type echo struct{ Received int }

func (n *echo) Snapshot() any { return n.Received }
func (n *echo) Handle(c *sim.Context, e sim.Event) error {
	n.Received++
	if e.Kind == "input" {
		c.Send("b", "hello")
	}
	return nil
}
func Example() {
	a, b := &echo{}, &echo{}
	s, _ := sim.New(sim.Config{Seed: 7, MinDelay: 1, MaxDelay: 5, Until: 20, MaxEvents: 100}, map[string]sim.Node{"a": a, "b": b})
	s.Inject(0, "a", "start")
	tr, err := s.Run()
	fmt.Println(len(tr.Steps), a.Received, b.Received, err)
	// Output: 2 1 1 <nil>
}
