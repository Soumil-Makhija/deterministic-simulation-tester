package counter

import (
	"strings"
	"testing"
)

func TestLostUpdateAndReplay(t *testing.T) {
	failures := 0
	for seed := uint64(1); seed <= 100; seed++ {
		r, err := Run(Default(seed, "broken"))
		if err != nil {
			if !strings.Contains(err.Error(), "lost update") {
				t.Fatal(err)
			}
			failures++
			if err := Replay(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if failures == 0 {
		t.Fatal("no lost updates detected")
	}
	t.Logf("lost updates found for %d/100 seeds", failures)
}
func TestFixedWithPartitionsAndRetries(t *testing.T) {
	for seed := uint64(0); seed < 1000; seed++ {
		sc := Default(seed, "fixed")
		sc.PartitionStart = 2
		sc.PartitionEnd = 35
		r, err := Run(sc)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if seed < 10 {
			if err := Replay(r); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestReplayRejectsModelChanges(t *testing.T) {
	r, _ := Run(Default(1, "broken"))
	r.Scenario.Model = "fixed"
	if err := Replay(r); err == nil {
		t.Fatal("changed model accepted")
	}
}
func TestIncompleteHorizonIsFailure(t *testing.T) {
	sc := Default(1, "fixed")
	sc.Config.Until = 5
	r, err := Run(sc)
	if err == nil || !strings.Contains(r.Trace.Failure, "incomplete") {
		t.Fatal(r, err)
	}
}
