#!/bin/sh
# Run one shard of the root package's tests: scripts/ci-test-shard.sh INDEX/TOTAL
#
# Every top-level test name from `go test -list .` is sorted and dealt out
# round-robin, so each name lands in exactly one shard and a test added later
# is picked up by the shard that its position selects. There is no pattern list
# to keep in step with the code. ci_workflow_test.go proves the shards in
# ci.yml cover every test.
#
# Test hooks (used by ci_workflow_test.go):
#   PANEWIRE_SHARD_NAMES_FILE  read the names from this file, not `go test -list`
#   PANEWIRE_SHARD_DRY_RUN=1   print the -run pattern and exit
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

if [ -n "${PANEWIRE_SHARD_NAMES_FILE:-}" ]; then
	all=$(cat "$PANEWIRE_SHARD_NAMES_FILE")
else
	all=$(go test -list . .)
fi

# Keep only test names: drop go's trailing "ok  <pkg>  <time>" line.
names=$(printf '%s\n' "$all" | grep -v -e '^ok[[:space:]]' -e '^$' | LC_ALL=C sort |
	awk -v i="$index" -v n="$total" '(NR - 1) % n == i')
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
exec go test -run "$pattern" .
