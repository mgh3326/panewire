#!/usr/bin/env bash
# Prove the copied-back digest replay test catches an original-only lookup.
set -eu
probe_tmp=$(mktemp -d "${RUNNER_TEMP:-/tmp}/pw-copy-key-mutant.XXXXXX")
probe_dir="$probe_tmp/worktree"
cleanup() {
  git -C "$probe_dir" restore lane_inbox_store.go >/dev/null 2>&1 || true
  git -C "$probe_dir" diff --exit-code >/dev/null 2>&1 || return 1
  git worktree remove "$probe_dir" >/dev/null 2>&1 || return 1
  rm -- "$probe_tmp/result.log"
  rmdir "$probe_tmp"
}
trap cleanup EXIT
git worktree add --detach "$probe_dir" HEAD >/dev/null
python3 - "$probe_dir/lane_inbox_store.go" <<'PY'
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
start = source.index('\t// MySQL stores oversized keys as digests.')
end = source.index('\tfresh, err := s.insertOnce(', start)
path.write_text(source[:start] + source[end:])
PY
if (cd "$probe_dir" && go test -count=1 -run '^TestLaneInboxStorePostgresCopyBackLongKey/digest$' .) >"$probe_tmp/result.log" 2>&1; then
  echo 'original-only lookup mutant unexpectedly passed'
  exit 1
fi
if ! grep -q 'replay: id=' "$probe_tmp/result.log"; then
  cat "$probe_tmp/result.log"
  echo 'mutant did not fail on the digest replay assertion'
  exit 1
fi
echo 'original-only lookup mutant was caught by digest replay'
# A lookup that checks only the original form must fail after replay succeeds.
git -C "$probe_dir" restore lane_inbox_store.go
python3 - "$probe_dir/lane_inbox_store.go" <<'PY'
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
old = 'if s.dialect == dialectPostgres && len(key) > mysqlEventKeyMax {'
assert source.count(old) == 2
path.write_text(source.replace(old, 'if false && s.dialect == dialectPostgres && len(key) > mysqlEventKeyMax {', 1))
PY
if (cd "$probe_dir" && go test -count=1 -run '^TestLaneInboxStorePostgresCopyBackLongKey/digest$' .) >"$probe_tmp/result.log" 2>&1; then
  echo 'original-only accessor mutant unexpectedly passed'
  exit 1
fi
if ! grep -q 'holds: false' "$probe_tmp/result.log"; then
  cat "$probe_tmp/result.log"
  echo 'mutant did not fail on the digest accessor assertion'
  exit 1
fi
echo 'original-only accessor mutant was caught by holds'
