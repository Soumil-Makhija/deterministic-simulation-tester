package sim

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

type recorder struct {
	Seen   []string
	Times  []int64
	handle func(*Context, Event) error
}

func (n *recorder) Handle(c *Context, e Event) error {
	n.Seen = append(n.Seen, e.Data)
	n.Times = append(n.Times, c.Now())
	if n.handle != nil {
		return n.handle(c, e)
	}
	return nil
}
func (n *recorder) Snapshot() any {
	return struct {
		Seen  []string
		Times []int64
	}{n.Seen, n.Times}
}
func config() Config { return Config{Seed: 42, MinDelay: 1, MaxDelay: 9, Until: 50, MaxEvents: 100} }
func setup(t *testing.T, replay *Trace, reverse bool) *Simulator {
	t.Helper()
	a := &recorder{handle: func(c *Context, e Event) error {
		c.Send("b", "one")
		c.Send("b", "two")
		c.After(2, "wake")
		if e.Kind == "timer" {
			return fmt.Errorf("intentional timer failure")
		}
		return nil
	}}
	b := &recorder{}
	nodes := map[string]Node{}
	if reverse {
		nodes["b"] = b
		nodes["a"] = a
	} else {
		nodes["a"] = a
		nodes["b"] = b
	}
	var s *Simulator
	var err error
	if replay == nil {
		s, err = New(config(), nodes)
	} else {
		s, err = Replay(*replay, nodes)
	}
	if err != nil {
		t.Fatal(err)
	}
	s.Inject(0, "a", "start")
	return s
}
func TestDeterminismAndReplay(t *testing.T) {
	first, err := setup(t, nil, false).Run()
	if err == nil {
		t.Fatal("expected modeled failure")
	}
	for i := 0; i < 100; i++ {
		got, _ := setup(t, nil, i%2 == 0).Run()
		if !reflect.DeepEqual(first, got) {
			t.Fatal("nondeterminism")
		}
	}
	data, _ := json.MarshalIndent(first, "", "  ")
	var saved Trace
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	_, err = setup(t, &saved, true).Run()
	if err == nil || err.Error() != first.Failure {
		t.Fatalf("replay failed: %v", err)
	}
	saved.Choices[0].Value = (saved.Choices[0].Value + 1) % saved.Choices[0].Bound
	if _, err := setup(t, &saved, false).Run(); err == nil || !strings.Contains(err.Error(), "diverged") {
		t.Fatalf("expected divergence, got %v", err)
	}
}
func TestPartitionAndHeal(t *testing.T) {
	a := &recorder{handle: func(c *Context, e Event) error {
		c.SendAfter(2, "b", "drop")
		c.SendAfter(5, "b", "deliver")
		return nil
	}}
	b := &recorder{}
	s, _ := New(config(), map[string]Node{"a": a, "b": b})
	s.Inject(0, "a", "start")
	s.Link(1, "a", "b", true)
	s.Link(4, "a", "b", false)
	tr, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.Seen, []string{"deliver"}) || !reflect.DeepEqual(b.Times, []int64{5}) {
		t.Fatalf("unexpected delivery: %+v", b)
	}
	drops := 0
	for _, e := range tr.Steps {
		if e.Dropped {
			drops++
		}
	}
	if drops != 1 {
		t.Fatal(drops)
	}
}
func TestStableTieOrderAndHorizon(t *testing.T) {
	n := &recorder{}
	cfg := config()
	cfg.Until = 3
	s, _ := New(cfg, map[string]Node{"n": n})
	s.Inject(3, "n", "first")
	s.Inject(3, "n", "second")
	s.Inject(4, "n", "later")
	tr, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(n.Seen, []string{"first", "second"}) || len(tr.Pending) != 1 {
		t.Fatal(tr)
	}
	if _, err := s.Run(); err == nil {
		t.Fatal("second Run accepted")
	}
}
func TestInvalidSchedulingAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    func(*Context)
	}{
		{"negative", func(c *Context) { c.After(-1, "x") }},
		{"overflow", func(c *Context) { c.After(math.MaxInt64, "x") }},
		{"unknown", func(c *Context) { c.Send("missing", "x") }},
		{"zero bound", func(c *Context) { c.Random("oops", 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &recorder{handle: func(c *Context, e Event) error { tc.f(c); return nil }}
			s, _ := New(config(), map[string]Node{"n": n})
			s.Inject(1, "n", "start")
			if _, err := s.Run(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	n := &recorder{handle: func(c *Context, e Event) error { c.After(0, "again"); return nil }}
	s, _ := New(config(), map[string]Node{"n": n})
	s.Inject(0, "n", "start")
	tr, err := s.Run()
	if err == nil || len(tr.Steps) != 100 || len(tr.Pending) != 1 {
		t.Fatal(tr, err)
	}
}
func TestReplayDetectsStateAndExtraChoices(t *testing.T) {
	tr, _ := setup(t, nil, false).Run()
	tr.Steps[0].State = json.RawMessage(`{}`)
	if _, err := setup(t, &tr, false).Run(); err == nil || !strings.Contains(err.Error(), "diverged") {
		t.Fatal(err)
	}
	tr, _ = setup(t, nil, false).Run()
	tr.Choices = append(tr.Choices, Choice{"unused", 1, 0})
	if _, err := setup(t, &tr, false).Run(); err == nil || !strings.Contains(err.Error(), "diverged") {
		t.Fatal(err)
	}
}
func TestConfigAndInvariant(t *testing.T) {
	cfg := config()
	cfg.MaxDelay = -1
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("invalid bounds accepted")
	}
	s, _ := New(config(), map[string]Node{"n": &recorder{}})
	s.Inject(0, "n", "x")
	s.Check = func(s *Simulator) error { return fmt.Errorf("invariant at %d", s.Now()) }
	tr, err := s.Run()
	if err == nil || tr.Failure != "invariant at 0" {
		t.Fatal(tr, err)
	}
}

func TestReplayDoesNotUsePRNG(t *testing.T) {
	tr, _ := setup(t, nil, false).Run()
	s := setup(t, &tr, false)
	s.rng = 9999999
	if _, err := s.Run(); err == nil || err.Error() != tr.Failure {
		t.Fatal(err)
	}
}
func TestDirectedPartitionAndTimerIsolation(t *testing.T) {
	a := &recorder{handle: func(c *Context, e Event) error {
		if e.Kind == "input" {
			c.SendAfter(2, "b", "request")
			c.After(2, "timer")
		}
		return nil
	}}
	b := &recorder{handle: func(c *Context, e Event) error { c.SendAfter(0, "a", "reply"); return nil }}
	s, _ := New(config(), map[string]Node{"a": a, "b": b})
	s.Link(0, "b", "a", true)
	s.Inject(0, "a", "start")
	tr, err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a.Seen, []string{"start", "timer"}) || !reflect.DeepEqual(b.Seen, []string{"request"}) {
		t.Fatal(tr)
	}
}
func TestReplayCorruption(t *testing.T) {
	for _, mutate := range []func(*Trace){
		func(t *Trace) { t.Version = 99 }, func(t *Trace) { t.Choices[0].Label = "wrong" }, func(t *Trace) { t.Choices[0].Bound++ }, func(t *Trace) { t.Choices[0].Value = t.Choices[0].Bound }, func(t *Trace) { t.Choices = nil }, func(t *Trace) { t.Pending = nil }, func(t *Trace) { t.Failure = "wrong" },
	} {
		tr, _ := setup(t, nil, false).Run()
		mutate(&tr)
		if tr.Version != 1 {
			if _, err := Replay(tr, nil); err == nil {
				t.Fatal("version accepted")
			}
			continue
		}
		if _, err := setup(t, &tr, false).Run(); err == nil || !strings.Contains(err.Error(), "diverged") {
			t.Fatal(err)
		}
	}
}
func FuzzDeterminism(f *testing.F) {
	f.Add(uint64(42), uint8(9))
	f.Add(uint64(0), uint8(1))
	f.Fuzz(func(t *testing.T, seed uint64, delay uint8) {
		cfg := config()
		cfg.Seed = seed
		cfg.MaxDelay = int64(delay) + 1
		makeSim := func(replay *Trace) *Simulator {
			a := &recorder{handle: func(c *Context, e Event) error {
				for i := 0; i < 8; i++ {
					c.Send("b", fmt.Sprint(i))
				}
				return nil
			}}
			b := &recorder{}
			var s *Simulator
			var err error
			if replay == nil {
				s, err = New(cfg, map[string]Node{"a": a, "b": b})
			} else {
				s, err = Replay(*replay, map[string]Node{"b": b, "a": a})
			}
			if err != nil {
				t.Fatal(err)
			}
			s.Inject(0, "a", "start")
			s.Link(3, "a", "b", true)
			s.Link(8, "a", "b", false)
			return s
		}
		tr, err := makeSim(nil).Run()
		if err != nil {
			t.Fatal(err)
		}
		second, err := makeSim(nil).Run()
		if err != nil || !reflect.DeepEqual(tr, second) {
			t.Fatal("not deterministic", err)
		}
		if _, err := makeSim(&tr).Run(); err != nil {
			t.Fatal(err)
		}
	})
}
