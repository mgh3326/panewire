#!/usr/bin/env bash
# Prove the copied-back digest replay test catches an original-only lookup.
set -eu
probe_dir=$(mktemp -d "${RUNNER_TEMP:-/tmp}/pw-copy-key-mutant.XXXXXX")
cleanup() {
  git worktree remove --force "$probe_dir" >/dev/null 2>&1 || true
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
# Removing the lookup also makes encoding/hex unused.
python3 - "$probe_dir/lane_inbox_store.go" <<'PY'
import pathlib
import sys
path = pathlib.Path(sys.argv[1])
source = path.read_text().replace('\t"encoding/hex"\n', '')
path.write_text(source)
PY
if (cd "$probe_dir" && go test -count=1 -run '^TestLaneInboxStorePostgresCopyBackLongKey/digest$' .) >"$probe_dir/result.log" 2>&1; then
  echo 'original-only lookup mutant unexpectedly passed'
  exit 1
fi
if ! grep -q 'replay: id=' "$probe_dir/result.log"; then
  cat "$probe_dir/result.log"
  echo 'mutant did not fail on the digest replay assertion'
  exit 1
fi
echo 'original-only lookup mutant was caught by digest replay'
