# On-demand burst holds

The existing R12 pressure policy is unchanged. This additive operator-only
path asks the hub to send its normal `burst` up event through the policy's
`wake_via` node, then waits for the target's authenticated heartbeat before it
returns a lease.

```sh
panewire burst request --target desktop --hold 30m --reason pg-backup-secondary \
  --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
panewire burst holds --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
panewire burst release --lease-id hold-... --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

`request` defaults to a 120-second confirmation timeout. Wake delivery is
fail-open: a missing wake-via node or an absent target heartbeat returns JSON
with a reason and exit code 3, so a caller can skip only its secondary path.
A lease is `active`, `released`, `expired`, or `lost`; target disconnect marks
an active lease `lost`. Holds are in-memory like hub presence and must not be
treated as durable scheduling state.

While an active lease exists, the target's idle down decision is suppressed.
The hub replies to target heartbeats with `burst.holds`; the node echoes its
current state in subsequent heartbeat envelopes. This does not alter pressure
thresholds, cooldowns, or the normal R12 wake path.
