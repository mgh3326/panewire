# Quota-stop

Idle-wake inspects the visible screen once for a named worker with an active local job. It records quota decisions locally in the daemon database and writes an hk note at `quota-stop/<wake-hash>`; no screen text is stored. The `policy/quota-stop` hk document supplies the four Markdown values in its `## Values` section. The cached document is refreshed at most every five minutes and remains available across restarts. If neither source is usable, the decision is handover.

Each observation has three bounds: 2 s for finding the pane's job and reserving the wake, its own 5 s for the screen read, and 5 s for the policy and task lookups a quota screen needs. Every read screen logs one `quota-stop observation` line with `outcome` (`quota`, `not_quota`, `screen_read_timeout`, `observation_canceled` or `screen_read_failed`) and `scan_ms`/`read_ms`; the screen text is never logged. A failed read records a `handover` decision with the same reason: `screen_read_timeout` is the read's own deadline, `observation_canceled` the caller's cancellation, `screen_read_failed` the read command's error.

The default is shadow (`quota_stop.act=false`, represented by the zero-value `Config.QuotaStopAct`); no pane input or handover event is sent. Set the daemon flag `--quota-stop-act` to enable actions. The daemon account needs `handoffkeep` and its configured credentials, plus `herdr`. Job and pane identity, state, screen, and harness are checked before a single resume attempt. A nonce is appended to the policy resume text to verify its submission. An ambiguous submission is never retried. Dialogs are handed over instead of choosing an option. The operator-desk handover is a durable lane event.

This feature does not change spawn routing or make the SWE-2 free hold fleet-wide. The spawn order is panewire's pool-order policy ([config](config.md), decision/2026-11-04/pool-order-owner).

A blocked pane is never resumed. herdr refuses `agent.prompt` on a blocked pane (`agent_blocked`) and sends nothing, so a quota stop seen on a blocked wake is handed over as `blocked_dialog`, and a due wait whose pane is blocked at resume time is handed over the same way without reading the screen or spending the job's one wait.

## Job end on a quota stop

Independent of `--quota-stop-act`, the job watcher ends a job whose worker, tester or reviewer pane is idle on a quota stop screen and has no report. It writes the hk record `state=lost` with `end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"` (`reset_at` left out when unknown, at most 128 characters, no `result`), emits `job.lost` with `reason: "quota_stop"`, `pool` and `reset_at`, and releases the quota reservation. It only reads the screen and never sends input to the pane. The classifier is `internal/quotastop`; the devin line is matched anywhere in the status line, including the `▲ Something went wrong — ` prefix. A report with the end signal still takes the done path.
