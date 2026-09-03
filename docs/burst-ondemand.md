# On-demand burst holds

The existing R12 pressure policy is unchanged. This additive operator-only
path asks the hub to send its normal `burst` up event through the policy's
`wake_via` node, then waits for the target's authenticated agent connection before it
returns a lease.

```sh
panewire burst request --target desktop --hold 30m --reason pg-backup-secondary \
  --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
panewire burst holds --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
panewire burst release --lease-id hold-... --hub-url https://hub.example --hub-token-env ~/.config/panewire/operator.env
```

`request` defaults to a 120-second confirmation timeout. Holds may be at most
24 hours; a supplied confirmation timeout may be at most 10 minutes. Wake delivery is
fail-open: a missing wake-via node or an absent target heartbeat returns JSON
with a reason and exit code 3, so a caller can skip only its secondary path.
A lease is `active`, `released`, or `lost`; target disconnect marks an active
lease `lost`. Expired leases are automatically released and pruned from the
in-memory list. Holds are in-memory like hub presence and must not be treated
as durable scheduling state.

While an active lease exists, the hub suppresses the target's idle down
decision. The hub replies to target heartbeats with `burst.holds`; the node
also refuses a delayed `burst` down event while that directive is active. The
node echoes its current state in subsequent heartbeat envelopes. On-demand
wakes use the normal R12 wake path and update its cooldown accounting, so a
request during `cooldown_minutes` fails open with `cooldown_active` and exit 3.
Pressure thresholds and the normal R12 wake path otherwise remain unchanged.
