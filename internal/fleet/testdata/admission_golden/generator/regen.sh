#!/bin/sh
# Regenerates ../verdicts.json: pacelever's own gate (the CLI's --account-key
# flow) run over build.py's scenarios in a throwaway clone of pacelever.
# docs/quota.md §Admission. Needs git, go and python3; changes no repository.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
ref=${PACELEVER_REF:-v0.3.28}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git clone -q --depth 1 --branch "$ref" https://github.com/mgh3326/pacelever.git "$work/pacelever"
cp "$here/pacelever_golden_test.go" "$work/pacelever/zz_panewire_golden_test.go"
python3 "$here/build.py" >"$work/scenarios.json"
(cd "$work/pacelever" && PW_SCENARIOS="$work/scenarios.json" PW_OUT="$work/golden.json" go test -run TestPanewireAdmissionGolden -count=1 .)
python3 "$here/pack.py" "$work/golden.json" "$here/../verdicts.json"
