#!/bin/sh
# Run one shard of the root package's tests: scripts/ci-test-shard.sh INDEX/TOTAL
#
# Top-level test names from `go test -list .` are balanced across shards
# by recorded test durations (scripts/ci-test-durations.json, operator decision 178)
# via Largest Processing Time (LPT) bin packing.
# Each name lands in exactly one shard and a test added later is picked up
# deterministically. ci_workflow_shard_test.go proves the shards in ci.yml
# cover every test and balance durations evenly.
#
# Test hooks (used by ci_workflow_test.go):
#   PANEWIRE_SHARD_NAMES_FILE     read the names from this file, not `go test -list`
#   PANEWIRE_SHARD_DURATIONS_FILE read durations from this file, not ci-test-durations.json
#   PANEWIRE_SHARD_DRY_RUN=1      print the -run pattern and exit
set -eu

spec=${1:?usage: ci-test-shard.sh INDEX/TOTAL}
index=${spec%/*}
total=${spec#*/}
case "$index$total" in
	'' | *[!0-9]*) echo "bad shard spec: $spec" >&2; exit 2 ;;
esac
if [ "$total" -lt 1 ] || [ "$index" -ge "$total" ]; then
	echo "shard $spec out of range" >&2
	exit 2
fi

script_dir=$(cd "$(dirname "$0")" && pwd)
durations_arg="${PANEWIRE_SHARD_DURATIONS_FILE:-$script_dir/ci-test-durations.json}"

if [ -n "${PANEWIRE_SHARD_NAMES_FILE:-}" ]; then
	all=$(cat "$PANEWIRE_SHARD_NAMES_FILE")
else
	all=$(go test -list . .)
fi

# Keep only test names: drop go's trailing "ok  <pkg>  <time>" line.
# Assign tests to shards balanced by recorded durations.
names=$(printf '%s\n' "$all" | grep -v -e '^ok[[:space:]]' -e '^$' |
	python3 "$script_dir/ci-test-shard-balance.py" "$index" "$total" "$durations_arg")
if [ -z "$names" ]; then
	echo "shard $spec selected no tests" >&2
	exit 1
fi

pattern="^($(printf '%s\n' "$names" | paste -sd'|' -))\$"
if [ "${PANEWIRE_SHARD_DRY_RUN:-}" = 1 ]; then
	printf '%s\n' "$pattern"
	exit 0
fi
echo "shard $spec: $(printf '%s\n' "$names" | wc -l | tr -d ' ') tests"
exec go test -v -run "$pattern" .
