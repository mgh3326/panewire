# panewire contributor conventions

These rules exist so that every change — by a person or an agent — lands in
the same shape. When a rule and existing code disagree, follow the rule and
leave the old code for a cleanup PR rather than copying its pattern.

## Layout

- The `panewire` package lives at the repository root; `cmd/panewire` stays a
  thin `main` wrapper.
- `internal/` holds packages the root imports (`fleet`, `arbiter`,
  `fleetevent`) and the shared helper packages (`durable`, `textbound`) that
  both the root and those packages use. An internal package never imports the
  root, so a helper every package needs goes in its own `internal/` package,
  with a short unexported wrapper in the root if the root uses it often.
- The rules below apply to `internal/` exactly as to the root.
- Name files by feature, not by ticket: `relay_dedupe.go`, `hub_spawn.go`.
  A ticket or round number never starts a file name.
- Contracts and runbooks live in `docs/`; link them from code comments and
  from `README.md` instead of restating them.
- Imports follow goimports: one standard-library group first, then module
  imports. `TestImportGroupsPutStandardLibraryFirst` enforces it for every
  package, since gofmt does not.

## Shared helpers — use these, do not hand-roll another copy

| Need | Helper | File |
|---|---|---|
| Replace a file atomically (temp → fsync → rename → dir fsync) | `durable.ReplaceFileAtomically` (root: `replaceFileAtomically`) | `internal/durable` |
| Same, confined to an `os.Root` | `durable.ReplaceFileAtomicallyInRoot` | `internal/durable` |
| No-clobber publish via `os.Link` | `durable.WriteSyncedTemporaryFile` + `durable.SyncDirectoryBestEffort` | `internal/durable` |
| JSON HTTP response | `writeJSON(w, status, value)`; a pinned pre-encoded body: `writeJSONBytes` | `http_json.go` |
| UTF-8-safe cut | `textbound.PrefixWithin` / `SuffixWithin` / `RunePrefix` (root: `utf8PrefixWithin`, `utf8SuffixWithin`) | `internal/textbound` |
| Bound text for a wire/record/display contract | the named variant | `text_bounds.go` |
| Mode-0600 env/credential file | `loadMode0600Env` | `env_file.go` |
| CLI exit status | the `Exit*` constants (`internal/arbiter` keeps its own frozen set) | `wait.go` |

- Never slice a string with `s[:n]` or `[]rune(s)[:n]` inline where `n` can
  cut text; use the `textbound` cuts. The truncation variants in
  `text_bounds.go` are wire/record contracts — reuse the one whose contract
  you share, and add a new one there (not in the caller's file) only for a
  new contract.
- Persisted files carry their final mode before any byte is written, and are
  fsynced before they become visible.
- Log with `log/slog` in the root package (`internal/fleet` reports through
  its `App`'s `warn`); never print credentials, tokens, or request bodies.
- Some HTTP bodies are pinned byte for byte (`/healthz` and the lanes
  refusal bodies); check the tests before changing how a handler writes.
- Code that needs the current time and must be testable takes a
  `now func() time.Time` dependency instead of calling `time.Now()` inline.
  When a type already carries one, every recorded timestamp and time-based
  rule goes through it (`HubServer.now`, `HubClient.nowUTC()`); never add a second accessor. Elapsed
  time for timeouts and deadlines stays on the wall clock, because a frozen
  test clock would never expire it.

## Tests

- Test files follow `<feature>[_<ticket>]_test.go`: the feature first, and an
  optional trailing ticket token (`_r20t5`, `_t1002`) for traceability.
- Wait for asynchronous work with the helpers in `test_helpers_test.go`, not
  a fixed `time.Sleep` or a new deadline loop: `eventually` (fails with a
  label), `waitUntil` (returns a bool so the failure can report observed
  state), `awaitClientEvent` (next node-client event of a kind). Wait on the
  outcome you assert, not on an earlier step that precedes it.
- `time.Sleep` in a test is only for simulated latency, fixture pacing, or a
  window that proves nothing *more* happens; say which in a comment.
- The PR `test (ubuntu-latest)` check splits the root package across runners with
  `scripts/ci-test-shard.sh`. It deals out every name from `go test -list .`, so a new test
  needs no CI edit; `ci_workflow_shard_test.go` fails if any test would run in no shard.
- Tests must pass on macOS and Linux, as a normal user and as root, and with
  `-count=N`. Skip a case only for a real environment limit, with the reason
  in the `t.Skip` message.
- Golden files under `testdata/job_golden` are the frozen job record
  contract once produced by the retired spawner; a change that alters any
  byte updates the golden in the same commit. See `job_golden_test.go`.

## Before pushing

CI runs these checks. A person may run them all before pushing. A fleet session (a pane spawned by panewire) follows the spawn preamble instead: it runs `gofmt -l .`, `go vet ./...` and `go test` for the packages it changed, on a host the preamble allows (never `go test` on mbp-server: use `remote-gotest`), and leaves `go test ./...` and the `-race` runs to CI.

```sh
gofmt -l .        # must print nothing
go vet ./...
go test ./...
go test -race -count=3 -run 'T1002|T1034' .
go test -race -run '^TestQuiet' .
go test -race -run '^(TestAsyncSpawn|TestSpawn|TestNodeSpawn)' .
```

These are the same checks CI runs (`.github/workflows/ci.yml`).
