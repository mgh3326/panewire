# Spawn package import

The `internal/fleet`, `internal/arbiter` and `internal/fleetevent` packages were
imported from the former launcher at revision
`6519bc855fa5597cf21cd7648e097e2b2fde809f`. This is code ownership moving into
panewire, with no dependency on the old executable or module.

`scripts/import-spawn.py SOURCE_WORKTREE` reproduces the initial import. It
rewrites the module path, package name, tool name and environment prefix; binary
SQLite test fixtures remain byte-identical. It refuses to overwrite modified
files. `--check` verifies the pinned source and manifest, leaving later edits
alone. `docs/spawn-import.json` records source and initial-import SHA-256 hashes.

Post-import changes are confined to CLI routing, combined-binary version
preflight, presets, aliases, workspace defaults/label resolution, actionable
refusal output and verified failure retry cleanup. The original package tests
remain in the package. The two version checks collapse to one because the
launcher and hub are now one binary. Job ledger schema and exit codes stay the
same. The parent repository's SQLite dependency remains in use.

Run `python3 scripts/check-spawn-parity.py SOURCE_WORKTREE --output REPORT.json`
to compare the pinned launcher with panewire's fake-backed dry-run plans across
every imported policy profile and worker/builder/reviewer/tester role. It
normalizes only the tool name and environment prefix. Missing snapshots fail.
Before accepting equality, controls must detect an output mutation and a swap
of two distinct profile/role assignments. It writes the matrix and mismatches
to the requested report. Full test suites run in Linux CI; local checks remain
limited to touched packages and the parity matrix.

The one-wave cutover finishes hub in-process orchestration and caller updates
before the desk archives the replaced launcher repository or removes binaries.
