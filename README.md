# panewire

Panewire is a local daemon and CLI for watching agent panes, waiting on stable state, and delivering prompts with verified evidence.

R2 adds `prompt` delivery with target preflight, identity checks, submission proof, optional uptake confirmation, and SQLite delivery audit records.

## Scope

The first usable slice is deliberately small:

- `wait` for a durable inbox file or a stable herdr agent state.
- `prompt` for verified delivery to an already-running pane.
- A local SQLite log of observed herdr and inbox events.

The file inbox and pane injection remain the system of record. panewire automates the surrounding watching, checking, waiting, and recording procedures; it does not turn session-to-session coordination into server communication.

Spawning remains the responsibility of `wrk`, including scopefuel gating, arbiter decisions, and landed verification. panewire is a messaging and monitoring layer for sessions that are already alive.

See [docs/design.md](docs/design.md) for the complete contract and acceptance criteria.

## Installation and use

R1 ships one executable, `panewire`, with a `daemon` subcommand. This avoids two separately versioned Go artifacts while keeping the launchd entry point explicit (`panewire daemon`).

The daemon uses `modernc.org/sqlite` to keep the build cgo-free and `log/slog` for structured startup/reconnect diagnostics. Inbox watching uses fsnotify with one registration per directory: macOS kqueue is not recursive, so the watcher walks the root and adds newly created directories immediately. This is simpler to operate and test on the required macOS runner than relying on a separate FSEvents backend.

```sh
go install github.com/mgh3326/panewire/cmd/panewire@latest
panewire daemon --herdr-socket "$HOME/.config/herdr/herdr.sock" \
  --db "$HOME/Library/Application Support/panewire/panewire.sqlite3" \
  --inbox-root "$HOME/work/herdr-inbox"
```

Install [deploy/dev.panewire.panewired.plist](deploy/dev.panewire.panewired.plist) as `~/Library/LaunchAgents/dev.panewire.panewired.plist` after replacing its user-specific paths. It uses the same `panewire` executable and restarts on exit.

```sh
panewire wait --file "$HOME/work/herdr-inbox/jobs/example/report.md" --settle 2s --timeout 10m
panewire wait --agent rob1320-r1 --status idle --settle 2s --timeout 10m
panewire prompt --from 'sender (role, pane-id)' --to orch-mock \
  --file "$HOME/work/herdr-inbox/jobs/example/prompt.md" --uptake status-transition
```

Prompt files begin with recipient metadata such as `expect: name=orch-mock cwd=/work`; `name=` or `cwd=` is required. `cwd=` uses exact absolute-path matching. Panewire records the prompt SHA-256, path, target identity, revisions, and evidence states, but does not store prompt text by default. To opt in, pass `--store-prompt-body` to `panewire prompt` or configure the daemon with `--store-prompt-body`; the body is then kept only in the separate `delivery_bodies` table.

The CLI never falls back to herdr directly. If the daemon socket is absent it exits 4. `PANEWIRE_SOCKET` is available for tests and non-default local installations.

## Stage 2 offline R1

`panewire submit` writes a metadata-only stage 2 outbox record; it does not
open a remote connection or load a credential file. The stage 2 publisher and
receiver loop is an explicit `Stage2.Enabled` daemon configuration gate and is
off by default, so the existing launchd invocation remains a stage 1 daemon.

```sh
panewire submit --db /safe/local/stage2.sqlite3 --file ./brief.md \
  --from-machine sender --destination-machine receiver --namespace jobs \
  --logical-path jobs/example/brief.md --classification personal_non_company
```

The R1 implementation is fixture-only: its Supabase adapter is exercised
against an in-process HTTP fake. Live Supabase credentials and smoke testing
are intentionally deferred to R2.

## Status

R2: prompt target safety, verified submission/uptake, privacy-preserving delivery audit, schema guard, events, inbox watcher, and wait. Live prompt smoke remains prohibited in the fixture test job; use an operator-approved scratch pane separately.
