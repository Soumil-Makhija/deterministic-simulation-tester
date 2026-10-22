// Package counter models two clients incrementing a remote counter. The broken
// protocol performs read/modify/write over a network; the fixed protocol sends
// an atomic, idempotent increment keyed by client ID.
package counter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Soumil-Makhija/deterministic-simulation-tester/sim"
)

type Scenario struct {
	Model          string     `json:"model"`
	Config         sim.Config `json:"config"`
	PartitionStart int64      `json:"partition_start"`
	PartitionEnd   int64      `json:"partition_end"`
}
type Recording struct {
	Scenario Scenario  `json:"scenario"`
	Trace    sim.Trace `json:"trace"`
}
type server struct {
	Value   int             `json:"value"`
	Applied map[string]bool `json:"applied"`
}

func (n *server) Snapshot() any { return n }
func (n *server) Handle(c *sim.Context, e sim.Event) error {
	switch {
	case e.Data == "read":
		c.Send(e.From, "value:"+strconv.Itoa(n.Value))
	case strings.HasPrefix(e.Data, "write:"):
		v, err := strconv.Atoi(strings.TrimPrefix(e.Data, "write:"))
		if err != nil {
			return err
		}
		n.Value = v
		c.Send(e.From, "ack")
	case e.Data == "inc":
		if !n.Applied[e.From] {
			n.Value++
			n.Applied[e.From] = true
		}
		c.Send(e.From, "ack")
	default:
		return fmt.Errorf("unexpected server message %q", e.Data)
	}
	return nil
}

type client struct {
	Model string `json:"model"`
	Phase string `json:"phase"`
	Write int    `json:"write"`
	Done  bool   `json:"done"`
}

func (n *client) Snapshot() any { return n }
func (n *client) Handle(c *sim.Context, e sim.Event) error {
	if n.Done {
		return nil
	}
	switch {
	case e.Kind == "input" || e.Kind == "timer":
		if n.Model == "fixed" {
			c.Send("server", "inc")
		} else if n.Phase == "write" {
			c.Send("server", "write:"+strconv.Itoa(n.Write))
		} else {
			c.Send("server", "read")
		}
		c.After(12, "retry")
	case e.Data == "ack":
		n.Done = true
	case strings.HasPrefix(e.Data, "value:"):
		if n.Phase == "write" {
			return nil
		}
		v, err := strconv.Atoi(strings.TrimPrefix(e.Data, "value:"))
		if err != nil {
			return err
		}
		n.Phase = "write"
		n.Write = v + 1
		c.Send("server", "write:"+strconv.Itoa(n.Write))
	default:
		return fmt.Errorf("unexpected client message %q", e.Data)
	}
	return nil
}

type audit struct {
	Server *server
	A, B   *client
}

func (n *audit) Snapshot() any { return struct{}{} }
func (n *audit) Handle(c *sim.Context, e sim.Event) error {
	if !n.A.Done || !n.B.Done {
		return fmt.Errorf("incomplete: clients did not both receive acknowledgements by horizon")
	}
	return nil
}
func Default(seed uint64, model string) Scenario {
	return Scenario{Model: model, Config: sim.Config{Seed: seed, MinDelay: 1, MaxDelay: 9, Until: 100, MaxEvents: 10000}}
}
func Build(sc Scenario, replay *sim.Trace) (*sim.Simulator, error) {
	if sc.Model != "broken" && sc.Model != "fixed" {
		return nil, fmt.Errorf("model must be broken or fixed")
	}
	if sc.Config.Until < 1 || sc.PartitionStart < 0 || sc.PartitionEnd < 0 || (sc.PartitionEnd != 0 && sc.PartitionEnd <= sc.PartitionStart) {
		return nil, fmt.Errorf("invalid horizon or partition window")
	}
	srv := &server{Applied: map[string]bool{}}
	a := &client{Model: sc.Model}
	b := &client{Model: sc.Model}
	nodes := map[string]sim.Node{"server": srv, "a": a, "b": b, "audit": &audit{srv, a, b}}
	var s *sim.Simulator
	var err error
	if replay == nil {
		s, err = sim.New(sc.Config, nodes)
	} else {
		if sc.Config != replay.Config {
			return nil, fmt.Errorf("scenario and trace configuration differ")
		}
		s, err = sim.Replay(*replay, nodes)
	}
	if err != nil {
		return nil, err
	}
	s.Inject(0, "a", "start")
	s.Inject(0, "b", "start")
	s.Inject(sc.Config.Until, "audit", "finish")
	if sc.PartitionEnd > 0 {
		for _, id := range []string{"a", "b"} {
			s.Link(sc.PartitionStart, id, "server", true)
			s.Link(sc.PartitionStart, "server", id, true)
			s.Link(sc.PartitionEnd, id, "server", false)
			s.Link(sc.PartitionEnd, "server", id, false)
		}
	}
	s.Check = func(s *sim.Simulator) error {
		if a.Done && b.Done && srv.Value != 2 {
			return fmt.Errorf("lost update: two acknowledged increments, server value = %d (want 2)", srv.Value)
		}
		return nil
	}
	return s, nil
}
func Run(sc Scenario) (Recording, error) {
	s, err := Build(sc, nil)
	if err != nil {
		return Recording{}, err
	}
	tr, err := s.Run()
	return Recording{sc, tr}, err
}
func Replay(r Recording) error {
	s, err := Build(r.Scenario, &r.Trace)
	if err != nil {
		return err
	}
	tr, err := s.Run()
	if err != nil && err.Error() != tr.Failure {
		return err
	}
	return nil
}
