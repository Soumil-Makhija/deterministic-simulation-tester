# Deterministic simulation tester

A standard-library Go virtual-time simulator for small distributed-system models.
Schedule messages, timers and directed network partitions; capture seeded delay
choices, every delivered/dropped event, state snapshots and the remaining queue.
Strict replay consumes the recorded choices and rejects differences in state,
events, choices, failure or pending work. Equal-time events follow insertion IDs.

```sh
go test ./...
```

Models implement `sim.Node` (`Handle` and JSON-serializable `Snapshot`). Handlers
use `Context.Send`, `SendAfter`, `After`, `Now` and `Random`; invariants run after
each event. Handlers must avoid wall time, goroutines, untracked randomness and
unordered map iteration. Replay requires the original model and initial inputs.
The horizon is inclusive; an event budget failure guards zero-time loops.
Partitions drop messages at delivery and do not buffer them for healing.

This tests modeled interleavings, not real operating-system threads or networks.
Seed search is bounded exploration, not a correctness proof. CLI examples and
integration checks are being added in subsequent milestones.
