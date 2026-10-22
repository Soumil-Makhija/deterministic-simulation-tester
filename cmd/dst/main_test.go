package main

import (
	"bytes"
	"encoding/json"
	"github.com/Soumil-Makhija/deterministic-simulation-tester/examples/counter"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIWorkflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.json")
	var out, errout bytes.Buffer
	if code := run([]string{"find", "-count", "100", "-out", path}, &out, &errout); code != 1 {
		t.Fatalf("code %d: %s", code, errout.String())
	}
	if !strings.Contains(out.String(), "lost update") {
		t.Fatal(out.String())
	}
	if code := run([]string{"replay", "-in", path}, &out, &errout); code != 0 {
		t.Fatalf("code %d: %s", code, errout.String())
	}
	r, err := read(path)
	if err != nil {
		t.Fatal(err)
	}
	r.Trace.Steps[0].Event.Data = "tampered"
	if err := save(path, r); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"replay", "-in", path}, &out, &errout); code != 2 {
		t.Fatal(code)
	}
	if code := run([]string{"find", "-model", "fixed", "-count", "50", "-partition-end", "35"}, &out, &errout); code != 0 {
		t.Fatal(code, errout.String())
	}
}
func TestCLIInvalidInputs(t *testing.T) {
	for _, args := range [][]string{nil, {"oops"}, {"run", "-model", "x"}, {"run", "-max-delay", "-1"}, {"find", "-count", "0"}, {"find", "-seed", "18446744073709551615", "-count", "2"}, {"replay"}, {"run", "extra"}} {
		var b bytes.Buffer
		if code := run(args, &b, &b); code != 2 {
			t.Fatalf("%v: %d", args, code)
		}
	}
	path := filepath.Join(t.TempDir(), "trace.json")
	r, _ := counter.Run(counter.Default(1, "fixed"))
	data, _ := json.Marshal(r)
	os.WriteFile(path, append(data, []byte(` {}`)...), 0644)
	if _, err := read(path); err == nil {
		t.Fatal("trailing data accepted")
	}
}
