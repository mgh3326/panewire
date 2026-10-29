# Quota-stop

Idle-wake inspects the visible screen once for a named worker with an active local job. It records quota decisions locally in the daemon database and writes an hk note at `quota-stop/<wake-hash>`; no screen text is stored. The `policy/quota-stop` hk document supplies the four Markdown values in its `## Values` section. The cached document is refreshed at most every five minutes and remains available across restarts. If neither source is usable, the decision is handover.

The default is shadow (`quota_stop.act=false`, represented by the zero-value `Config.QuotaStopAct`); no pane input or handover event is sent. Set the daemon flag `--quota-stop-act` to enable actions. The daemon account needs `handoffkeep` and its configured credentials, plus `herdr`. Job and pane identity, state, screen, and harness are checked before a single resume attempt. A nonce is appended to the policy resume text to verify its submission. An ambiguous submission is never retried. Dialogs are handed over instead of choosing an option. The operator-desk handover is a durable lane event.

This feature does not change spawn routing or make the SWE-2 free hold fleet-wide. The canonical spawn policy remains free first for eligible tasks; urgent work can move to paid models under contention.
