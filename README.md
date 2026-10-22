# Deterministic simulation tester

Find distributed-protocol races with seeded virtual-time simulation, then replay
an exact saved execution. Standard-library Go, no services or credentials needed.

The included counter model exposes a real read/modify/write race between two
network clients: both read `0`, both write `1`, and both receive acknowledgements.
The invariant expects `2`. The corrected model uses atomic, idempotent increments.

## Try it

Requires Go 1.23 or newer. From a clone of this repository:

```sh
go build -o dst ./cmd/dst
./dst find -seed 1 -count 100 -out failure.json
# Exit 1 is expected: seed=1 model=broken events=15 failure="lost update: ..."
./dst replay -in failure.json
# Exit 0: replay verified (including the same failure)
./dst replay -in testdata/lost-update.json
./dst find -model fixed -count 1000 -partition-start 2 -partition-end 35
# no failure in 1000 seeds starting at 1 (bounded search)
```

`run` executes one seed; `find` stops at the first failure. Both accept `-model`,
`-seed`, `-max-delay` (default 9), `-until` (default 100), `-partition-start`,
`-partition-end` and `-out`. `find` also accepts `-count`. A partition with end `0`
is disabled; otherwise both clients are disconnected from the server for the
specified window. `-out` saves a trace for `run`, or the first failure for `find`;
a successful search writes no file. Existing output paths are replaced atomically.
`replay -in FILE` checks a recording in a separate execution. Use a built binary
when checking exit statuses; `go run` wraps program exit statuses.

Exit codes: `0` = run/search found no failure, or replay matched; `1` = a model,
invariant or event-budget failure was recorded; `2` = invalid input, I/O error or
replay mismatch. A deadline failure means the modeled clients did not finish by
the configured horizon; it does not establish permanent unavailability.

## What gets recorded

Version-1 JSON contains the scenario, engine configuration, every random choice
(label, bound and value), ordered events and per-event JSON state snapshots,
delivery/drop outcomes, pending queue, and failure message. The committed
[seed-1 recording](testdata/lost-update.json) is a regression fixture.

Replay consumes recorded choices **without using the PRNG**. It reruns the model
and rejects mismatched choice labels/bounds, exhausted or unused choices, changed
events, state, pending work or failure. JSON indentation is irrelevant; state
serialization and key ordering must remain stable. This is a regression check,
not an authenticity signature. A trace still requires the same model code and
initial scenario; it cannot execute arbitrary model code on its own.

## Write a model

Implement `sim.Node` with `Handle(*sim.Context, sim.Event) error` and
`Snapshot() any`. See the runnable [library example](sim/example_test.go) and
[distributed counter](examples/counter/counter.go).

```go
s, err := sim.New(sim.Config{
    Seed: 7, MinDelay: 1, MaxDelay: 9, Until: 100, MaxEvents: 10000,
}, map[string]sim.Node{"a": nodeA, "b": nodeB})
// Check err before using s.
s.Inject(0, "a", "start")
s.Link(3, "a", "b", true)  // directed partition
s.Link(20, "a", "b", false)
s.Check = func(s *sim.Simulator) error { return checkInvariant(s) }
trace, err := s.Run()
```

`Context.Send` samples a delay in the inclusive configured range; `SendAfter`
sets an explicit delay. `After` schedules a local timer; `Random(label, bound)`
records model entropy. `Now()` returns virtual ticks. Errors from callbacks,
scheduling or invariant checks stop execution and remain in the recording.
For library replay, construct **fresh** nodes with the same initial state, call
`sim.Replay(trace, nodes)`, schedule the same inputs/links and install the same
invariant. `Run` returns the reproduced model error on an exact failing replay;
`errors.Is(err, sim.ErrReplayDiverged)` distinguishes a mismatch.

Deterministic model contract:

- Use the context for all time, randomness and scheduling. No wall clock,
  external I/O, goroutines, global mutable state or unordered map iteration.
- Snapshot all relevant model state as deterministic JSON. Snapshots must have
  no side effects; avoid custom nondeterministic marshalers and unsupported data.
- Use stable node IDs and insertion order for inputs, links and generated work.
  The simulator copies the node map, but owns no copies of mutable node objects.
- Handlers and invariants must terminate. A callback panic is not recovered; the
  event budget cannot interrupt a hanging callback or bound its memory allocation.

## Engine semantics and architecture

`sim/` uses a priority queue ordered by `(virtual tick, monotonic insertion ID)`.
Equal-time ordering is stable, including faults and deadlines. Time jumps directly
to the next event; there are no sleeps. The inclusive horizon leaves later events
in the trace. The counter's audit event is inserted before generated messages and
therefore wins ties at its deadline. Invariants run after every processed event,
including dropped deliveries and topology changes. No initial-state check runs
before the first event; model authors can schedule an initial audit if needed.

Partitions affect **delivery time**, including already in-flight messages. They
drop messages, never buffer them. Links are directed; local timers and external
inputs bypass partitions. Healing affects subsequent deliveries. The counter's
12-tick client retry timers generate duplicate requests; only its corrected
protocol deduplicates applied increments. The engine itself does not guarantee
FIFO delivery, retries, duplication, durable storage, crashes or recovery. Models
can implement those through explicit events and state. Zero delays are supported;
the event budget catches infinite zero-time event chains.

SplitMix64 arithmetic and modulo bounds define a stable seed stream. Modulo bias
is acceptable for schedule exploration; this is not statistical sampling or
cryptographic entropy. Replay stores choices to avoid depending on that stream.
The CLI hosts the counter example; custom protocols use the Go package directly.

## Validate

```sh
go test -count=1 -cover ./...
go test -race -count=1 ./...
go vet ./...
./scripts/smoke.sh
go test ./sim -run '^$' -fuzz FuzzDeterminism -fuzztime=5s -parallel=2
go test ./examples/counter -run '^$' -bench BenchmarkFixedCounter -benchmem
```

The suite checks repeated traces across node-map insertion orders, recorded-choice
replay independent of PRNG state, corruption rejection, partition healing and
direction, timer isolation, delay overflow, event bounds, incomplete horizons,
95 lost updates among broken seeds 1–100, 1,000 corrected seeds with a partition,
and an additional 800 corrected delay/partition combinations. The smoke script
compares recordings from separate processes, then replays failure and success.
Go's race detector checks implementation data races; the invariant finds the
modeled distributed logical race. CI runs tests, vet, formatting and the smoke
script on Linux/macOS with Go 1.23 and stable. No email or comment steps are added.

## Limits

This is a small model-based tester inspired by deterministic distributed testing,
not FoundationDB's simulator. It does not intercept real sockets, binaries or
OS threads. Its results only cover the supplied models, invariants, faults, seeds
and finite horizons. No exhaustive scheduling, counterexample minimization,
production consensus model, crash/disk simulation or real-service adapter is
included. A passing seed search is not a correctness proof. State snapshots make
traces grow with event count and model size; CLI replay limits input to 64 MiB.
The counter's fixed protocol assumes a single live server with in-memory request
deduplication; persistence and failover are outside its guarantees.

See [validation evidence](VALIDATION.md) for observed local results.
