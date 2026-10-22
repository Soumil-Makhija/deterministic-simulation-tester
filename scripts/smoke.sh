#!/bin/sh
# Run from the repository root. Each replay is a separate process.
set -eu
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
go build -o "$tmp/dst" ./cmd/dst
expect_failure() {
 code=0
 "$@" || code=$?
 test "$code" -eq 1
}
expect_failure "$tmp/dst" find -seed 1 -count 100 -out "$tmp/first.json"
expect_failure "$tmp/dst" run -seed 1 -out "$tmp/second.json"
cmp "$tmp/first.json" "$tmp/second.json"
"$tmp/dst" replay -in "$tmp/first.json"
"$tmp/dst" replay -in testdata/lost-update.json
"$tmp/dst" find -model fixed -count 1000 -partition-start 2 -partition-end 35
"$tmp/dst" run -model fixed -out "$tmp/fixed.json"
"$tmp/dst" replay -in "$tmp/fixed.json"
