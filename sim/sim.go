// Package sim provides a single-threaded, virtual-time discrete event simulator.
// Models must use Context for time and randomness and return deterministic state.
package sim

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

type Event struct {
	ID      uint64 `json:"id"`
	At      int64  `json:"at"`
	Kind    string `json:"kind"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
	Data    string `json:"data,omitempty"`
	Blocked bool   `json:"blocked,omitempty"`
}

type Node interface {
	Handle(*Context, Event) error
	Snapshot() any
}
type Config struct {
	Seed      uint64 `json:"seed"`
	MinDelay  int64  `json:"min_delay"`
	MaxDelay  int64  `json:"max_delay"`
	Until     int64  `json:"until"`
	MaxEvents int    `json:"max_events"`
}
type Choice struct {
	Label string `json:"label"`
	Bound uint64 `json:"bound"`
	Value uint64 `json:"value"`
}
type Step struct {
	Event   Event           `json:"event"`
	Dropped bool            `json:"dropped,omitempty"`
	State   json.RawMessage `json:"state"`
}
type Trace struct {
	Version int      `json:"version"`
	Config  Config   `json:"config"`
	Choices []Choice `json:"choices"`
	Steps   []Step   `json:"steps"`
	Pending []Event  `json:"pending"`
	Failure string   `json:"failure,omitempty"`
}

type queue []Event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].At == q[j].At {
		return q[i].ID < q[j].ID
	}
	return q[i].At < q[j].At
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(Event)) }
func (q *queue) Pop() any     { a := *q; x := a[len(a)-1]; *q = a[:len(a)-1]; return x }

type Simulator struct {
	cfg     Config
	nodes   map[string]Node
	ids     []string
	q       queue
	now     int64
	seq     uint64
	rng     uint64
	links   map[[2]string]bool
	trace   Trace
	replay  *Trace
	cursor  int
	err     error
	started bool
	Check   func(*Simulator) error
}

func New(cfg Config, nodes map[string]Node) (*Simulator, error) {
	if cfg.MinDelay < 0 || cfg.MaxDelay < cfg.MinDelay || cfg.MaxDelay == math.MaxInt64 || cfg.Until < 0 || cfg.MaxEvents <= 0 {
		return nil, errors.New("invalid simulation bounds")
	}
	s := &Simulator{cfg: cfg, nodes: make(map[string]Node), rng: cfg.Seed, links: make(map[[2]string]bool)}
	for id, n := range nodes {
		if id == "" || n == nil {
			return nil, errors.New("invalid node")
		}
		s.nodes[id] = n
		s.ids = append(s.ids, id)
	}
	sort.Strings(s.ids)
	s.trace = Trace{Version: 1, Config: cfg, Choices: []Choice{}, Steps: []Step{}, Pending: []Event{}}
	return s, nil
}

// Replay uses the recorded choices, without consulting the PRNG, and checks all
// events, states, pending messages and the final failure against the recording.
func Replay(t Trace, nodes map[string]Node) (*Simulator, error) {
	if t.Version != 1 {
		return nil, errors.New("unsupported trace version")
	}
	s, err := New(t.Config, nodes)
	if err != nil {
		return nil, err
	}
	s.replay = &t
	return s, nil
}
func (s *Simulator) Now() int64          { return s.now }
func (s *Simulator) State(id string) any { return s.nodes[id].Snapshot() }
func (s *Simulator) add(e Event) {
	if s.err != nil {
		return
	}
	if e.At < s.now {
		s.err = errors.New("cannot schedule in the past")
		return
	}
	if e.Kind != "link" && s.nodes[e.To] == nil {
		s.err = fmt.Errorf("unknown destination %q", e.To)
		return
	}
	s.seq++
	e.ID = s.seq
	heap.Push(&s.q, e)
}

// Inject schedules an external input before Run. At is absolute virtual time.
func (s *Simulator) Inject(at int64, to, data string) {
	if s.started {
		s.err = errors.New("Inject after Run started")
		return
	}
	s.add(Event{At: at, Kind: "input", To: to, Data: data})
}

// Link schedules a directed partition or heal, evaluated at message delivery.
func (s *Simulator) Link(at int64, from, to string, blocked bool) {
	if s.started {
		s.err = errors.New("Link after Run started")
		return
	}
	if s.nodes[from] == nil || s.nodes[to] == nil {
		s.err = errors.New("unknown link endpoint")
		return
	}
	s.add(Event{At: at, Kind: "link", From: from, To: to, Blocked: blocked})
}
func (s *Simulator) random(label string, bound uint64) uint64 {
	if bound == 0 {
		s.err = errors.New("zero random bound")
		return 0
	}
	var v uint64
	if s.replay != nil {
		if s.cursor >= len(s.replay.Choices) {
			s.err = errors.New("replay exhausted choices")
			return 0
		}
		c := s.replay.Choices[s.cursor]
		s.cursor++
		if c.Label != label || c.Bound != bound || c.Value >= bound {
			s.err = fmt.Errorf("replay choice mismatch at %d", s.cursor-1)
			return 0
		}
		v = c.Value
	} else {
		// SplitMix64, fixed arithmetic rather than library-version-dependent entropy.
		s.rng += 0x9e3779b97f4a7c15
		z := s.rng
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		v = (z ^ (z >> 31)) % bound
	}
	s.trace.Choices = append(s.trace.Choices, Choice{label, bound, v})
	return v
}
func (s *Simulator) snapshot() (json.RawMessage, error) {
	states := make(map[string]any, len(s.nodes))
	for _, id := range s.ids {
		states[id] = s.nodes[id].Snapshot()
	}
	return json.Marshal(states)
}
func (s *Simulator) Run() (Trace, error) {
	if s.started {
		return Trace{}, errors.New("simulator is single use")
	}
	s.started = true
	for s.q.Len() > 0 && s.err == nil {
		if s.q[0].At > s.cfg.Until {
			break
		}
		if len(s.trace.Steps) >= s.cfg.MaxEvents {
			s.err = errors.New("event budget exceeded")
			break
		}
		e := heap.Pop(&s.q).(Event)
		s.now = e.At
		dropped := false
		if e.Kind == "link" {
			s.links[[2]string{e.From, e.To}] = e.Blocked
		} else {
			dropped = e.Kind == "message" && s.links[[2]string{e.From, e.To}]
			if !dropped {
				if err := s.nodes[e.To].Handle(&Context{s: s, id: e.To}, e); err != nil {
					s.err = err
				}
			}
		}
		// Do not overwrite a scheduling error with a nil callback result.
		state, err := s.snapshot()
		if err != nil {
			s.err = fmt.Errorf("snapshot: %w", err)
		}
		s.trace.Steps = append(s.trace.Steps, Step{e, dropped, state})
		if s.err == nil && s.Check != nil {
			s.err = s.Check(s)
		}
	}
	pending := append(queue{}, s.q...)
	sort.Sort(pending)
	s.trace.Pending = append(s.trace.Pending, pending...)
	if s.err != nil {
		s.trace.Failure = s.err.Error()
	}
	actual, marshalErr := json.Marshal(s.trace)
	if marshalErr != nil {
		return s.trace, marshalErr
	}
	expected, _ := json.Marshal(s.replay)
	if s.replay != nil && (s.cursor != len(s.replay.Choices) || !bytes.Equal(actual, expected)) {
		return s.trace, errors.New("replay diverged: choices, events, state, pending queue or failure differ")
	}
	return s.trace, s.err
}

type Context struct {
	s  *Simulator
	id string
}

func (c *Context) Now() int64                               { return c.s.now }
func (c *Context) Random(label string, bound uint64) uint64 { return c.s.random(c.id+":"+label, bound) }
func (c *Context) schedule(delay int64, e Event) {
	if delay < 0 || delay > math.MaxInt64-c.s.now {
		c.s.err = errors.New("invalid or overflowing delay")
		return
	}
	e.At = c.s.now + delay
	c.s.add(e)
}
func (c *Context) Send(to, data string) {
	d := c.s.cfg.MinDelay + int64(c.s.random("delay:"+c.id+">"+to, uint64(c.s.cfg.MaxDelay-c.s.cfg.MinDelay)+1))
	c.schedule(d, Event{Kind: "message", From: c.id, To: to, Data: data})
}
func (c *Context) SendAfter(delay int64, to, data string) {
	c.schedule(delay, Event{Kind: "message", From: c.id, To: to, Data: data})
}
func (c *Context) After(delay int64, data string) {
	c.schedule(delay, Event{Kind: "timer", From: c.id, To: c.id, Data: data})
}
