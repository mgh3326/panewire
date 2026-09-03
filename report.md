# R16 implementation report

Implemented operator-only on-demand burst request/hold/release, target
heartbeat confirmation, active/expired/released/lost leases, and idle-down
suppression. The existing R12 pressure trigger and spawn paths are unchanged.

R14 F1 now clears a returned working owner's orphan marker and emits
`job.recovered`; F2 delivers the hub-issued `job.assigned` epoch to the new
owner on heartbeat directives and the node adopts it for later heartbeats.

Verification passed locally: `gofmt`, `go build ./...`, `go vet ./...`,
`go test ./...`, and `go test -race ./...`. CI: https://github.com/mgh3326/panewire/actions/runs/33779494656

Mutant assertions recorded RED for: hold suppression removal, TTL expiration
removal, operator authorization removal, F1 active-return recovery removal,
and F2 assigned-epoch delivery removal.

PR=https://github.com/mgh3326/panewire/pull/27 HEAD=209d0eb7d8b487ed54c34ff59a7f57d2c1a0fc04 MUTANTS_RED=5/5
