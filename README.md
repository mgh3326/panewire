# panewire

Panewire is a local daemon and CLI for watching agent panes, waiting on stable state, and relaying prompts with verified delivery.

R0 is a design-only release. The repository contains the proposed contract for `panewired` (the launchd-resident daemon), the `panewire` CLI, and the SQLite event/delivery log; it intentionally contains no implementation code.

## Scope

The first usable slice is deliberately small:

- `wait` for a durable inbox file or a stable herdr agent state.
- `prompt` for verified delivery to an already-running pane.
- A local SQLite log of observed events, preflight evidence, and delivery outcomes.

The file inbox and pane injection remain the system of record. panewire automates the surrounding watching, checking, waiting, and recording procedures; it does not turn session-to-session coordination into server communication.

Spawning remains the responsibility of `wrk`, including scopefuel gating, arbiter decisions, and landed verification. panewire is a messaging and monitoring layer for sessions that are already alive.

See [docs/design.md](docs/design.md) for the complete R0 contract and acceptance criteria.

## Planned installation

After an implementation is approved, the intended distribution is a single Go binary installed with `go install`, with `panewired` kept alive by a per-user launchd plist on company MacBooks. These commands are design targets only in R0.

## Status

R0: public repository and design document. Implementation is intentionally prohibited until hostile review and operator approval are complete.
