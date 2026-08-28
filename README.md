# panewire

Panewire is a local daemon and CLI for watching agent panes and waiting on stable state.

R1 implements the daemon, herdr event subscription, recursive inbox watching, and `wait`. Prompt delivery remains outside this R1 binary surface; R2 is intentionally not included.

## Scope

The first usable slice is deliberately small:

- `wait` for a durable inbox file or a stable herdr agent state.
- A local SQLite log of observed herdr and inbox events.

The file inbox and pane injection remain the system of record. panewire automates the surrounding watching, checking, waiting, and recording procedures; it does not turn session-to-session coordination into server communication.

Spawning remains the responsibility of `wrk`, including scopefuel gating, arbiter decisions, and landed verification. panewire is a messaging and monitoring layer for sessions that are already alive.

See [docs/design.md](docs/design.md) for the complete R0 contract and acceptance criteria.

## Installation and use

R1 ships one executable, `panewire`, with a `daemon` subcommand. This avoids two separately versioned Go artifacts while keeping the launchd entry point explicit (`panewire daemon`).

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
```

The CLI never falls back to herdr directly. If the daemon socket is absent it exits 4. `PANEWIRE_SOCKET` is available for tests and non-default local installations.

## Status

R1: scaffold, schema drift guard, events, inbox watcher, and wait implementation. See [docs/design.md](docs/design.md) for the contract and explicit R2 boundary.
