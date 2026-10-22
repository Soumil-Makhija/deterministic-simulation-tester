package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Soumil-Makhija/deterministic-simulation-tester/examples/counter"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
func read(path string) (counter.Recording, error) {
	var r counter.Recording
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return r, err
	}
	if info.Size() > 64<<20 {
		return r, errors.New("recording exceeds 64 MiB limit")
	}
	d := json.NewDecoder(io.LimitReader(f, (64<<20)+1))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return r, errors.New("trailing JSON data")
	}
	return r, nil
}
func save(path string, r counter.Recording) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".dst-recording-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if _, err = f.Write(append(data, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}
func run(args []string, out, errout io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errout, "usage: dst run|find|replay [flags]")
		return 2
	}
	if args[0] == "replay" {
		fs := flag.NewFlagSet("replay", flag.ContinueOnError)
		fs.SetOutput(errout)
		in := fs.String("in", "", "recording JSON")
		if fs.Parse(args[1:]) != nil || *in == "" || fs.NArg() != 0 {
			return 2
		}
		r, err := read(*in)
		if err == nil {
			err = counter.Replay(r)
		}
		if err != nil {
			fmt.Fprintln(errout, err)
			return 2
		}
		fmt.Fprintf(out, "replay verified: seed=%d events=%d failure=%q\n", r.Scenario.Config.Seed, len(r.Trace.Steps), r.Trace.Failure)
		return 0
	}
	if args[0] != "run" && args[0] != "find" {
		fmt.Fprintln(errout, "unknown command")
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(errout)
	seed := fs.Uint64("seed", 1, "first seed")
	count := fs.Uint64("count", 100, "seeds to search (find only)")
	model := fs.String("model", "broken", "broken or fixed counter")
	path := fs.String("out", "", "save recording JSON")
	max := fs.Int64("max-delay", 9, "maximum network delay ticks")
	until := fs.Int64("until", 100, "inclusive virtual horizon")
	start := fs.Int64("partition-start", 0, "partition starts at tick")
	end := fs.Int64("partition-end", 0, "heal at tick (0 disables partition)")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 || *count == 0 {
		return 2
	}
	n := uint64(1)
	if args[0] == "find" {
		n = *count
	}
	if n-1 > ^uint64(0)-*seed {
		fmt.Fprintln(errout, "seed range overflows")
		return 2
	}
	for i := uint64(0); i < n; i++ {
		sc := counter.Default(*seed+i, *model)
		sc.Config.MaxDelay = *max
		sc.Config.Until = *until
		sc.PartitionStart = *start
		sc.PartitionEnd = *end
		r, err := counter.Run(sc)
		if err != nil && r.Trace.Version == 0 {
			fmt.Fprintln(errout, err)
			return 2
		}
		if args[0] == "run" || err != nil {
			if *path != "" {
				if e := save(*path, r); e != nil {
					fmt.Fprintln(errout, e)
					return 2
				}
			}
			fmt.Fprintf(out, "seed=%d model=%s events=%d failure=%q\n", sc.Config.Seed, sc.Model, len(r.Trace.Steps), r.Trace.Failure)
			if err != nil {
				return 1
			}
			return 0
		}
	}
	fmt.Fprintf(out, "no failure in %d seeds starting at %d (bounded search)\n", n, *seed)
	return 0
}
