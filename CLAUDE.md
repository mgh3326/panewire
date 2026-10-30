# panewire contributor conventions

These rules exist so that every change — by a person or an agent — lands in
the same shape. When a rule and existing code disagree, follow the rule and
leave the old code for a cleanup PR rather than copying its pattern.

## Layout

- One Go package, `panewire`, at the repository root. `cmd/panewire` and
  `cmd/panewire-assistant` stay thin `main` wrappers.
- Name files by feature, not by ticket: `relay_dedupe.go`, `hub_spawn.go`.
  A ticket or round number never starts a file name.
- Contracts and runbooks live in `docs/`; link them from code comments and
  from `README.md` instead of restating them.

## Shared helpers — use these, do not hand-roll another copy

| Need | Helper | File |
|---|---|---|
| Replace a file atomically (temp → fsync → rename → dir fsync) | `replaceFileAtomically` | `durable_file.go` |
| No-clobber publish via `os.Link` | `writeSyncedTemporaryFile` + `syncDirectoryBestEffort` | `durable_file.go` |
| JSON HTTP response | `writeJSON(w, status, value)` | `http_json.go` |
| Bound text length | `utf8PrefixWithin` or one of the contract variants | `text_bounds.go` |
| Mode-0600 env/credential file | `loadMode0600Env` | `env_file.go` |
| CLI exit status | the `Exit*` constants | `cli.go` |

- Never slice a string with `s[:n]` where `n` can land inside a UTF-8
  sequence; use `utf8PrefixWithin`. The truncation variants in
  `text_bounds.go` are wire/record contracts — reuse the one whose contract
  you share, and add a new one there (not in the caller's file) only for a
  new contract.
- Persisted files carry their final mode before any byte is written, and are
  fsynced before they become visible.
- Log with `log/slog`; never print credentials, tokens, or request bodies.
- Code that needs the current time and must be testable takes a
  `now func() time.Time` dependency instead of calling `time.Now()` inline.

## Tests

- Test files follow `<feature>[_<ticket>]_test.go`: the feature first, and an
  optional trailing ticket token (`_r20t5`, `_t1002`) for traceability.
- Wait for asynchronous work with `eventually` (`test_helpers_test.go`), not a
  fixed `time.Sleep` or a new deadline loop.
- Tests must pass on macOS and Linux, as a normal user and as root, and with
  `-count=N`. Skip a case only for a real environment limit, with the reason
  in the `t.Skip` message.
- Golden files under `testdata/job_golden` are the frozen job record
  contract once produced by the retired spawner; a change that alters any
  byte updates the golden in the same commit. See `job_golden_test.go`.

## Before pushing

```sh
gofmt -l .        # must print nothing
go vet ./...
go test ./...
go test -race -count=3 -run 'T1002|T1034' .
go test -race -run '^TestQuiet' .
```

These are the same checks CI runs (`.github/workflows/ci.yml`).
