# Quota-stop

Idle-wake inspects the visible screen once for a named worker with an active local job. It records quota decisions locally in the daemon database and writes an hk note at `quota-stop/<wake-hash>`; no screen text is stored. The `policy/quota-stop` hk document supplies the four Markdown values in its `## Values` section, and the optional `max_wait` (below) and `reviewer_wait` (hk 5355, under "A quota stop, by role") lines. The cached document is refreshed at most every five minutes and remains available across restarts. If neither source is usable, the decision is handover.

Each observation has three bounds: 2 s for finding the pane's job and reserving the wake, 5 s shared by the screen read, herdr's lookups and the devin session record (each later step gets only the time the read left, so none adds a second timeout), and 5 s for the policy and task lookups a quota screen needs. Every read screen logs one `quota-stop observation` line with `outcome` (`quota`, `not_quota`, `screen_read_timeout`, `observation_canceled` or `screen_read_failed`) and `scan_ms`/`read_ms`; the screen text is never logged. A failed read records a `handover` decision with the same reason: `screen_read_timeout` is the read's own deadline, `observation_canceled` the caller's cancellation, `screen_read_failed` the read command's error.

Detection is structured-first for a devin pane (hk 4542): the devin CLI's own record of the session's last turn decides the free-model limit, and the screen text is the fallback. herdr reports the pane's session id in `agent_session.value`; the CLI writes that session's process pid into `<data>/devin/cli/session_locks/<session>.lock` and the process log into `<data>/devin/cli/logs/devin_<start>_<pid>.log` (`<data>` is `$XDG_DATA_HOME`, else `~/.local/share`; only these files are read, never a credential). A failed turn ends with the log line `Sending error response … method=session/prompt error=Error { … message: "<Devin's message>" }`; a free-model limit exhausts the inference retries first, so that last line carries `Reached free model rate limit … reset in N …`, and no later `method="session/prompt"` line exists — a prompt means the session is running again. The log must name the session (`for session <id>`) so a recycled pid cannot stand in. At most the last 16 MiB of the log is read: a longer log that holds no turn outcome, a missing lock or log, an unnamed session, or a last turn that failed for another reason (Devin's request-rate limit) is no evidence and the screen decides as before.

A devin pane whose record is a free-model stop is a `devin-free` stop with the record's reset even when the screen shows no limit, and one whose record shows the session has run since (or never failed a turn) is not a free-limit stop however much the screen reads like one — the quoted banner and multi-row directive cases of the residual risk below. The record never overrides another pool's banner, which the classifier still reads.

A provider credit rejection is a stop too (hk 4982): the classifier names the pool from the rejection's own wording — Anthropic's "Your credit balance is too low" is `anthropic-api`, OpenAI's `insufficient_quota` or "You exceeded your current quota" is `openai-api` — and sets `CreditExhaustedAt` at the observation. It is recognised, never acted on here: the job watcher publishes the rejection and the hub's credit trigger sweeps the pool, so this manager records no decision and never resumes or hands over on a credit stop.

A Command Code window limit is a stop too (hk 5183): omp renders the provider's 429 as its error row — `Error: 429 You've reached your 5-hour usage limit for your plan. Your limit resets at 2026-11-18T08:31:56.856Z. Please wait for the window to reset or upgrade your plan to continue. (type=rate_limit_error param=RATE_LIMITED)` — followed by `Retry failed after 1 attempts: Provider requested 5075005ms wait, exceeds retry.maxDelayMs`. The classifier names the pool `commandcode` and reads the reset from the row's own `resets at` RFC 3339 timestamp (a timestamp that does not parse leaves the reset unknown, handed over as `reset_unknown`). Like the credit row it anchors on the rendered error row only (the `429` status, optionally behind `Error:`, `API Error:` or `⏺`), never on the sentence anywhere in ordinary output, so a final answer quoting it is no stop. The row wraps: continuation rows made only of the sentence's words, a number, a timestamp and the `(type=…)` suffix join it until that suffix closes it, and the retry notice and omp's `raw-http-request=` path under it are chrome, not later output; any other row after the limit row cancels the stop. It is not a choice dialog (its "upgrade your plan" text is not a menu) and not a credit rejection (`CreditExhaustedAt` stays zero), so it goes through the ordinary decision below: an implementer waits when the reset is within start plus `--expected` and is otherwise handed over (urgent priority hands over); a reviewer or consult gets the behaviour of its role in "A quota stop, by role". Only the 5-hour wording is captured from a real screen. The sentence is the provider's own message that omp renders, so omp's installed source does not carry the weekly wording either; any one-word window in `You've reached your <window> usage limit` (`weekly`, `7-day`) is read the same way, a guess until a weekly screen is captured. The stop is pool-scoped: the hand-over does not carry the job's `account_key`, so the successor excludes the `commandcode` pool until the reset (the credit rejection above, unlike it, marks only the job's account).

Otherwise the observation asks `herdr agent explain <pane> --json` for the matched rule and the agent kind; the top-level `agent` and `matched_rule` keys are read as herdr 0.9.5 returns them, and a `result`-wrapped reply is accepted too. A `quota_limit` rule means herdr recognised the stop: the screen text supplies the reset, and when the text has no parseable limit the pool comes from the agent kind with the reset unknown, handed over as `reset_unknown`. Otherwise the screen classifier decides. herdr 0.9.5 ships no `quota_limit` rule, so every detection today is the structured record's or the classifier's; the herdr path is covered by fixture only, not by a live quota stop. The basis (`structured`, `herdr:quota_limit` or `classifier`) is logged on each `quota-stop observation` and `quota-stop action` line; it is not stored in the database.

A wait also shows on the pane. A `wait` decision the manager will wait out (a worker or tester, or any role with `--quota-stop-act`) reports herdr's display-only token `panewire_quota` from source `panewire.quota`: the value is `<pool> wait until <reset RFC 3339 UTC>` and the TTL is the reset plus five minutes, capped at herdr's 24 h maximum. A capped token is re-reported five minutes before it lapses, and a resume or hand-over clears it. A herdr that refuses the report, or the clear, is retried half a minute later and logged once per outage, never once per retry; the refusal never changes the decision, and a refused clear is retried until the token's TTL has passed. The token never feeds a decision — the `quota_stop_decisions` row stays the authority — and herdr keeps tokens in memory only, so a token published before a daemon restart is left to its TTL. A sidebar row shows it only where the desk configures `$panewire_quota`.

A candidate the idle-wake route suppressed, for any reason (`design_lane`, `ambiguous_owner`, `parentless_lane`, `unknown_owner`, `invalid_route`), is still read once per idle episode, keyed by the `quota_stop_decisions` event id, so a stop on a pane the route would not wake is decided (hk 4233, hk 4507). Suppression is about waking, not reading. Only candidates whose pane is still in that idle episode (`idle_wake_panes` at the same `state_change_seq`, idle or done) are scanned, each tick until observed; a candidate whose pane has no single local job is looked at again at most every 5 minutes.

The default is shadow (`quota_stop.act=false`, represented by the zero-value `Config.QuotaStopAct`); no pane input or handover event is sent. Set the daemon flag `--quota-stop-act` to enable actions. The daemon account needs `handoffkeep` and its configured credentials, plus `herdr`. Job and pane identity, state, screen, and harness are checked before a single resume attempt. A nonce is appended to the policy resume text to verify its submission. An ambiguous submission is never retried. Dialogs are handed over instead of choosing an option. The operator-desk handover is a durable lane event.

This feature does not change spawn routing. A devin free-tier stop also writes this host's `devin-free-limit.json` (the record `panewire quota observe devin-free-limit` writes) and publishes it at once through `panewire quota refresh devin`: the record is merged over the account's current shared reading without a provider measurement, so every host's selection skips the free rungs as `free_limit` from the stop itself, until the reset. A replacement for a `quota_stop pool=devin-free` also excludes the free band (`--exclude-pool devin-free`), so it never takes another free rung of the same account. A paid Devin weekly stop (`quota_stop pool=devin`) is the mirror: it cools only the paid band and its replacement excludes only that band (`devin-paid`), so the free rungs stay usable while only the paid window is spent. The spawn order is panewire's pool-order policy ([config](config.md), decision/2026-11-04/pool-order-owner and decision/2026-11-11/model-routing-escalation A31).

A blocked pane is never resumed. herdr refuses `agent.prompt` on a blocked pane (`agent_blocked`) and sends nothing, so a quota stop seen on a blocked wake is handed over as `blocked_dialog`, and a due wait whose pane is blocked at resume time is handed over the same way without reading the screen or spending the job's one wait.

## A quota stop, by role

hk 3334. The wait or hand-over rule is hk `policy/quota-stop`; this section is the mechanics. The role split is in [config](config.md) ("A quota-stopped job, by role").

**Which role.** The role is the one the `job.spawned` receipt names (reviewer, implementer, consult). The claim records only implementer or builder, so it is never used alone (`Job.QuotaRole`, `quotaStopManager.jobFacts`).

**Reviewer.** The job watcher sees an idle reviewer pane on a quota screen with no report and ends the job at once (hk 4507): the hk record `state=lost` with `end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"`, `job.lost` with `reason: "quota_stop"`, `pool` and `reset_at`, and the quota lease released. `job.lost` and the release wait for the lost hk record, through a durable close intent: the watcher records `job.quota_stop_intent` (the stop and its exact `end_reason`) before the first hk write, and reconciles it every poll, past the ordinary completion deadline, across screen-read errors, herdr outages and watcher restarts. The `end_reason` is frozen at the first attempt (a fresh parse of `reset in 5 minutes` differs on every poll). hk decides first: exactly the intended `lost` record finishes the job (announce, release) even if the screen can no longer be read; any other terminal record (completed or escalated meanwhile, or a different `lost`) is left alone and nothing is announced or released; an unreadable hk record keeps the intent. Only a live record needs another write, and that needs a readable screen that still shows the stop; an unreadable screen or herdr never cancels the intent, and while it is open no other terminal path ends the job (the missing-pane fallback, a stable report with its end signal, the unanswered-decision timeout), so hk is always settled first;  and a readable screen that no longer shows a stop, a written report or a vanished pane cancels it (`job.quota_stop_cancelled`). The finish releases the lease first and then writes the closing `job.lost`, so a crash or a failure between the two leaves the intent open and both idempotent steps repeat on the next poll or in a re-armed watcher. That `job.lost` carries `final: true`: the hk record is already lost, so a watcher or a re-arm never adopts it as a live hand-off grace and never reprompts the pane. There is no grace and no hub replacement for this path: the record is already ended, which the hub's replacement below needs live, and mergewake respawns the review on another model from the lost record.

**Reviewer on a short reset (hk 5355).** A reviewer whose stop resets within the reviewer wait is waited out, not ended, and is resumed once. The quota-stop manager decides (`quotaDecisionReviewer`): it waits when the reset is no further away than the policy's `reviewer_wait` (default 10 min; never above the watcher's 10 min hold, `fleet.QuotaReviewerWait`, nor above `max_wait`) and falls before the review deadline (the job's start plus the task's expected time, or `default_expected` when the task names none); a reset already past is due at once (`reset_passed`). It resumes the pane once at the reset with the policy's resume text, through the same one-wait, nonce-verified submission as an implementer. Every other stop ends the reviewer at once, as above, by handing it to the watcher's intent (`RecordReviewerQuotaIntent`): an urgent priority (`urgent`), a second stop after the one resume (`resume_limit`), a dialog (`choice_dialog`), an unreadable reset (`reset_unknown`), a reset beyond the reviewer wait (`reset_beyond_reviewer_wait`), a reset past the review deadline (`reset_past_review_deadline`), or a job whose start is unknown (`start_unknown`). The watcher holds a short stop for the manager, so its end is not decided by the screen alone: when it sees a reset within the reviewer wait it records `job.quota_stop_wait` (once per job; the hold is the only wait a reviewer gets) and extends its completion deadline by the wait, and it does not end the job while the hold stands. The hold ends when the pane moves on (resumed, back to work, or a report written) or the pane vanishes, and then the next look decides afresh. If the pane is still on the stop when the hold's wait (reset plus five minutes) runs out, the watcher ends the reviewer as above. A second stop after a resume is never held again. A watcher restarted inside the wait re-arms the hold from the durable `job.quota_stop_wait` event only when the stop on the pane has the recorded pool and remaining duration (within 2 s) and the event's `until` is still ahead, once per process. The reset time is not compared, because a relative limit is read again as now plus its duration. Any other stop is ended at once. Residual: a second stop whose countdown text is identical to the recorded one (the same duration, within 2 s) is held until the recorded `until`; the manager still decides it (`resume_limit`) and hands it to the intent path.

**Replacing an announced stop.** A stop announced on a live job (an implementer the quota-stop manager handed over, or a credit-stopped job) is replaced by the hub. The watcher records `job.lost` with `reason: "quota_stop"`, `pool` and `reset_at`, and leaves the hk record and the quota lease alone for `QuotaStopHandOffGrace` (10 minutes). The hub runs the whole replacement under a hard 6 minute context deadline and gives the successor a 150 s spawn deadline, so it is done, one way or the other, well before the watcher ends the job (the 4 minute margin covers the relay from the watcher to the hub). hk never rewrites a lost record, so the pointer to the successor can only be written together with `state=lost`. The hub, on that `job.lost`, finds the job's stored submission and the pool in its hk record, probes the pane (it must be idle), and submits a successor: same task, role, review pass, owner and brief, a new job id `<old>-q<n>` (`n` counts the replacements already in the id; at most 2), a new agent label and checkout directory, a `--report-relative` that points under the old job moved under the new one, and `--exclude-pool <pool>`. A job that inherited its task (`--parent-job`, no `--task`) is replaced with the same arguments. Before it places anything the hub saves the old pane's screen and closes the pane (`pane.close` re-reads it and refuses a pane that changed, in which case nothing is placed, so one job holds the task), and it checks that the whole `end_reason` fits hk's 128 characters. Once the successor is placed (or, when the wait for it ends with its operation still reconciling or past its deadline, because a late node result can still start it and hk cannot repair the record afterwards: a pointer to a successor that later fails is a recoverable chain, a running successor nobody points to is duplicate work) it has the holder's node write the hk record `state=lost`, `end_reason="quota_stop pool=<pool> replaced_by=<new job>"` (`job.mark_lost` with `end_reason`). `replaced_by` is the last `key=value` token and the id fits `[A-Za-z0-9._-]{1,128}` (the mergewake hk 3335 contract, pinned by `TestQuotaStopEndReasonFormatIsPinned`). The hub logs the replacement and sends the owner one info lane event, "replaced <old> (<pool> quota) with <new> on <host>/<profile>". The watcher keeps watching through the grace even past its ordinary completion deadline, and a pane that vanishes during it is not ended by the generic missing-pane path (the hub closed it). A pane that goes back to work during the grace records `job.reprompted` and the job lives on. When the successor cannot be placed (no other pool; the pane is then already closed), or the pane is not idle, or the hub is old or unreachable, the watcher ends the job itself after the grace, leaving a record the hub already wrote alone, as before otherwise: `state=lost`, `end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"`, quota released.

**Implementer.** The watcher does not end the job. The quota-stop manager acts for this role even when `--quota-stop-act` is off (it stays observe-only for every other role, and for a decision with no proven quota screen). Its decision (`quotaDecisionJob`): wait when the reset is at or before the job's start plus the task's expected time (hk `--expected`; a task that names none is judged on the policy's `default_expected` counted from now, hk 5349), otherwise hand over (`reset_too_late`). A reset that has already passed is a wait that is due at once (`reset_passed`): the pane is resumed, never handed over or ended for a stop that is over (hk 5349; the 2026-11-19 loss of `pw-terminal-owner-5344`, whose task named no expected time, was a short request-rate-limit stop handed over as `expected_missing` and ended `job.lost` with no replacement). An unknown reset still hands over (`reset_unknown`); an urgent priority (`policy/quota-stop`) and a second stop after one resume also hand over. On the hub's pool sweep (an exhausted pool, below) the resumes already taken are the hub's own wait rows plus the node's recorded resumes for the job (`job.result`, `quota_resumes`); a node record that cannot be read counts as one resume, so that stop hands over instead of waiting again (hk 5311).

**`max_wait` (hk 4503).** The optional `- max_wait: <duration>` line in `policy/quota-stop` `## Values` (`1h`, `1d`, read like `default_expected`) caps how far away a reset may be and still be waited out. A reset further away than that hands over with reason `reset_beyond_max_wait`, checked before the expected-time comparison, in both `quotaDecisionJob` and `quotaDecision`: a free SWE-2 limit 13 hours away is not waited out by a task that expects 24 hours. A reset exactly at the limit still waits; short free rate limits (seconds to minutes) still wait. A document without the line keeps the decision above unchanged. A present line that is not a positive duration, or appears twice, makes the whole policy unavailable (`policy_unavailable`, handed over), like the other values. A wait resumes the same pane once at the reset with the policy's resume text, nothing else, and sends the owner one info lane event. A hand-over records `job.lost` with `reason: "quota_stop"` and the hub replaces the job as for a reviewer, with the end reason `quota_stop pool=<pool> handed_over_to=<new job>`, the same worktree and branch, and one brief line naming the earlier job's report and branch. The record is written **before** the successor is claimed (the ticket's order, and the spawn refuses a task still bound to a job that is not lost); when the spawn then fails, the pointer names a job that never ran and the owner is told by the spawn failure notice. If the hub cannot even write it, the watcher ends the job after the grace, as for a reviewer. A hand-over also starts the job's watcher when it has already ended (a worker that waited past its completion deadline), and a node restart re-arms a watcher that died during the grace.

A hub restarted between claiming a reviewer's successor and writing the old record is repaired by a periodic pass (`reconcileQuotaReplacements`, every 2 minutes, for successors older than the replacement budget): a successor that placed whose predecessor's record is still live gets the pane closed and the record written with `replaced_by`. A successor that never left `accepted` (no node) can never place a job and gets no pointer; one still reconciling or past its deadline with a node binding does.

The hub's own hand-over of an implementer on an exhausted pool (hk 4633) is described under "An exhausted pool": it continues a clean, fully pushed branch on another host; on a node that also advertises `handover_snapshot_v1`, dirty or unpushed work is saved as a verified snapshot commit and continues there too (hk 5324). Known limits of this screen-triggered path: an implementer on another host does not carry uncommitted changes or unpushed commits (the ticket's hand-over is the same worktree path, branch and brief; there is no state transfer), and a pane that resumed and settled idle again between the hub's probe and `pane.close` is closed.

## Job end on a quota stop

The job watcher ends a reviewer job whose pane is idle on a quota stop screen and has no report. It writes the hk record `state=lost` with `end_reason="quota_stop pool=<pool> reset_at=<RFC3339 UTC>"` (`reset_at` left out when unknown, at most 128 characters, no `result`), emits `job.lost` with `reason: "quota_stop"`, `pool` and `reset_at`, and releases the quota reservation. It only reads the screen and never sends input to the pane; for a devin pane the free-model stop comes from the session record first (Detection above) and the screen rule is the fallback. The classifier is `internal/quotastop`; the devin line is matched anywhere in the status line, including the `▲ Something went wrong — ` prefix. Devin's short request-rate-limit banner (`Reached request rate limit … reset in 8 seconds (at 10:57 UTC)`, hk 4644) is a Devin stop too; its text names no rung, so the running profile decides the pool (a SWE-2 free rung is `devin-free`, any other Devin rung `devin`). The paid Devin weekly line `Quota exhausted — Your weekly usage quota has been exhausted` is the `devin` pool (paid `devin-ds41` spends the account's paid window, hk 4545), not `devin-free`, so it writes no free-limit record. Its reset is read by the same grammar as the free line; the paid reset wording is not yet captured, so a paid stop with no readable reset is handed over as reset unknown. Codex renders the provider's own sentence, whose apostrophe may be U+2019 (`You’ve hit your usage limit`) and whose reset may be a calendar date in local time (`try again at Nov 17th, 2026 12:02 AM`); both forms are read, so the 2026-11-16 codex stop (hk 4982) is a `codex` stop with that reset. An unparsable reset still recognises the stop; it is handed over as reset unknown.

**Where the limit sits.** The limit line is found anywhere on the visible screen, not only near the prompt, as long as no newer agent output follows it. What may follow is the CLI's chrome (rules, an empty prompt, the option footer) and user turns, where a user turn is exactly one prompt-marked row (`❭`, `❯`, `›` followed by text), which is how a relay directive appears. Until a rule starts the composer, the rows after a user turn are trusted only if blank; any other row there (an answer, an option-like line, a continuation of the message) is agent output or of unknown ownership, so it is neither accepted nor taken for a status. A flush-left limit line after a blank row that follows a user turn is accepted: that is how a new limit follows the user's message. For a devin pane only, Devin's indented banner after a blank row is accepted under the three conditions in the residual risk below (hk 4680; the last-block rule of hk 4690). The reset and the choice flag are read from the limit line and the chrome after it, never from the injected text. A relative reset adds its parts: `reset in 13 hours 9 minutes` is 13 h 9 min. A devin free-tier stop writes the host's `devin-free-limit.json` (hk 4507). Devin's free-model and request-rate-limit banners are the messages the classifier joins across rows (hk 4644, hk 4680): their body continues onto the following rows indented like it, up to the closing `Send a message to retry`. Devin's working-directory row is chrome when it is one status mark (`⏺` or `●`), a space, and one path token (`~/…` or `/…`) and nothing else; a mark outside that set, or prose on the row, is agent output. A composer rule labelled with the first turn's text (`──── … ─`) is chrome after a status, never under a user turn, and its label is not read. Any other later row is still agent output. omp's own `raw-http-request=<file>` diagnostic row directly under a provider credit rejection is that harness's chrome, not output (hk 4982); anything after it is later output. A resume re-read of this screen matches either Devin pool, because the screen cannot tell the rung from a free one or a paid one. A devin stop is a choice only on the dialog's own menu header (`Choose an option`); the "Upgrade" and "credits" wording of Devin's limit banners is advertising, so the banner waits and resumes (hk 4683).

*Known residual risk (operator decision, hk 4507): flat screen text cannot prove who wrote a row.* A directive that wraps or spans several rows is not detected (fail closed: no stop). A complete status quoted by a user message after a blank row, or an answer whose own row starts with the prompt mark, looks like the incident screen and can end a live reviewer. Since hk 4542 a devin pane's own session record decides the free-model limit before the screen is consulted, so this reading cannot end a devin reviewer; the screen still decides when that record is unavailable (no session id, no log, or an unreadable outcome).
*Residual risk, operator decision 264 (hk 4690; it replaces the row allow-list of hk 4680, operator decision B): for a devin pane (`ClassifyAgent`, herdr agent kind `devin`), an indented Devin banner after a blank row is a new limit when (1) the agent kind is devin, (2) the banner carries its limit text, its reset (relative, or `at HH:MM UTC`) and its closing sentence `Send a message to retry`, which ends the wrapped body (a banner without that sentence is refused here), and (3) the banner is in the screen's last block, as defined next. Other agent kinds never take this path.*

**Last block (hk 4690).** The block of a banner is the run of rows from the banner's head down to the next blank row. A blank row ends a block. Rows in the banner's own block are not read, except a user turn (below): whatever follows the banner before the first blank row is part of its block, so an unknown row there does not end the stop. After a blank row, a block made only of Devin chrome is not a block of output: chrome is a rule, the labelled composer rule (`──── … ─`), the empty `❭ Ask Devin` input row or its placeholder, the working-directory row (`⏺` or `●` and one path, see below) and the model footer. Any other row after a blank row starts a block of output, a quoted banner or a later message, so the banner is then not in the last block and is not a stop. Once a blank row has been seen, a row that is not chrome also refutes the stop, even inside a block that also holds chrome. The banner is in the last block when every row after it is either in its own block or chrome after a blank row.

A **user turn** is one row whose first character is a prompt mark (`❭`, `❯` or `›`) followed by text. The empty prompt and the placeholders (`❭ Ask Devin …`) are chrome, not turns. A user turn anywhere after the banner, in its own block or after a blank row, means the pane is no longer stopped (the "user turn after" negative of hk 4680). A relay directive under a user turn is therefore never read as a stop by this path.

*Known residual risk, operator decision B and 264: a row directly under the banner, with no blank row between, is inside the banner's block and is not read. Prose there, a complete Devin banner quoted after a blank row followed by the composer, or an answer whose own row starts with the prompt mark each look like the incident screen, and a quoted banner directly followed by output reads as a stop for that devin pane. Devin's screens so far put a blank row between a banner and any output, so the first form has not been captured; the structured session record of hk 4542 decides a devin pane's free-model limit when it is available, and this screen path remains its fallback.*

**A completed job that its owner re-prompts (hk 4982).** Neither path watches it. The node watcher is per job and a restart pass skips a terminal job unless it ended on an announced quota stop (`internal/fleet/watch.go`, `rearmWatchers`); the hub's pool triggers read only live spawned jobs. A prompt typed into the pane of a completed job writes no job event, so no watcher starts and no trigger reads that pane: the 2026-11-17 re-prompt of `pa-traffic-v1-4967` (completed, then prompted 1 h 55 min later for a merge conflict) was watched by nothing. The rule is that an owner **respawns** the work instead of re-prompting a completed job's pane: a spawn places a new job under both paths, while a re-prompt leaves the old pane unwatched. Covering the re-prompt itself — noticing a completed job's pane go busy again and reviving or re-arming the job — is follow-up hk 4983.

## An exhausted pool

When a node publishes an exhausted free bucket or a 100% account-wide window
with a future reset, the hub scans live spawned jobs placed on that pool and
account. Free-bucket exhaustion applies only to free rungs; paid windows apply
to paid rungs. A newer reading that clears the same bucket supersedes the
trigger, and the hub checks the reset and freshness again before acting.

A free-limit reset inside `policy/quota-stop` `max_wait` (1h) sends no owner
notice (hk 4765): the pane's quota stop waits it out and retries on it, so a
hand-over instruction would undo the wait. Resets past that horizon still
notify. Either way the sweep cools the Devin free band (`devin-free`) until
the reset, so the next selection skips the free rows while the paid rungs
stay eligible.

Two more triggers start the same sweep for a pool that publishes no quota
reading of its own (hk 4625). A hub pass runs once at startup and then every
ten minutes over the pools live jobs are placed on:

- **Retired catalog.** Every catalog row of the pool is retired. The hub reads
  the bench catalog through the quota adapter and its cache; a stale catalog is
  refused, so a notice never rests on a day-old row set. A pool the catalog does
  not list at all fires nothing.
- **Credit exhausted.** The pane of a live job on the pool shows the provider's
  own credit rejection: Anthropic's "Your credit balance is too low", OpenAI's
  `insufficient_quota` / "You exceeded your current quota", or Command Code's
  "You have insufficient credits to make this request". The hub
  reads the pane's visible tail through its node (`pane.read`, the reader the
  quota-stop manager uses) and applies the quota-stop activity rule
  (`internal/quotastop`), so the same words quoted by a later answer are not a
  stop. omp writes its own `raw-http-request=<file>` diagnostic row under the
  rejection; that row is chrome, so the trigger reads the rejection above it
  (hk 4982, the 2026-11-17 anthropic-api stop the trigger read every ten
  minutes and did not fire on). The rejection must name the job's own pool.

The retired cause and the anthropic-api / openai-api credit causes name no
account and no reset: they fire per pool, and every live job on the pool gets
the one notice below, whose text names what ran out. A pool cause is held until
the pool comes back — a catalog row un-retired — after which both of the pool's
triggers re-arm and a repeat is logged as new. While a cause is held, later
passes still notify jobs that were placed after it fired.

**Command Code is scoped to the account (hk 5138).** That pool holds several
accounts per host and fleet-wide, so its rejection marks only the job's
recorded `account_key`, never the pool. The mark is stored in the hub database
(both the SQLite and Postgres schemas), served to selection in the fleet
snapshot, and skipped there with reason `credit_exhausted account=<key>` — the
path a 7d-exhausted account takes, leaving the pool's other accounts placeable.
It lifts at the first quota reading of that account observed after the mark
that reports at least one remaining credit (a fresh omp report), because a
reading below one credit is still exhausted; a reading with no balance says
nothing, so the mark then holds until 24 h, whichever comes first. Selection
also drops any mark past 24 h, and refuses an account whose reading reports
fewer than one credit even without a mark (`credit_exhausted balance=<n>`,
decision 403). A job whose placement record has no account marks nothing rather
than the pool. An implementer on the marked account is handed over
through the quota-stop flow's successor (`hub_quota_replace.go`), and that
successor excludes no pool: the mark keeps it off the spent account while the
other accounts stay usable.

**An implementer is handed over by the hub (hk 4633, operator decisions 358
and A336; decision `evidence/2026-11-18/pw-handover-4633/decision-root-causes.md`).**
On a node that advertises `close_ops_v1` and `handover_check_v1`, the hub
decides with the §Implementer rule (the job's start, the task's priority and
`--expected`, `policy/quota-stop`) for every cause: window, free limit past
`max_wait`, credit, retired and account credit.

- **Wait:** no action; the owner gets one info line naming the reset, and the
  hub stores a final `wait` row with that reset. The same stop seen again says
  nothing more (a notice lost in a crash is replayed); a stop with another
  reset is the second stop and hands over.
- **Hand over:** a pool action row of kind `handover` (`pool_actions`, the
  #669 store). **A non-final handover row owns the old job** — settled
  included, while it still owes its successor. One store predicate,
  `poolActionOwns`, answers that, and reassignment, task-role release, the
  quota-stop replacement (`replaceQuotaStopped`, also hk 5138's credit path,
  and `reconcileQuotaReplacements`) and the pool sweep all check it. The old
  job's task role stays reserved until the successor's claim takes it over in
  the same transaction, so no other spawn for the task is admitted between
  them. In reverse, an existing `<old>-q<n>` spawn means do nothing: whichever
  creates its row (or successor) first owns the job. The steps are, in this
  order:
  1. **Eligibility and saved successor.** The old node runs the read-only
     `worktree.handover_check` from the worktree root. Tracked changes,
     non-ignored untracked files, or any assume-unchanged or skip-worktree
     entries (including in populated submodules) give `dirty_worktree`.
     HEAD must equal the branch tip read with `git ls-remote origin`; an
     unpushed or never-pushed branch gives `unpushed_commits`. A node that
     advertises `handover_snapshot_v1` does not stop there (hk 5324): the hub
     runs `worktree.handover_snapshot`, which commits the tracked contents and
     unpushed commits as a snapshot commit, pushes it to the immutable ref
     `refs/panewire/handover/<job>/<commit>`, and reads it back from origin —
     a refused, failed or unproven snapshot means no row and no close, and the
     owner notice names the reason. Untracked files are never pushed: they are
     listed in the result, and ignored files are not preserved. On a node
     without the capability a dirty or unpushed worktree keeps the refusal:
     no action row and no close; the owner receives the reason and the
     hand-over-or-respawn notice. The snapshot's ref, commit, tree, parent and
     untracked list are stored on the hand-over row before any close.
     Otherwise the row stores the complete successor with a fixed request id:
     the same role, owner and brief; job `<old>-q<n>` (at most two); exactly
     one `--parent-job <old>`; the spent pool or band excluded, and the old host
     excluded too (`--exclude-host`), because the old worktree keeps the branch
     there until the close settles and a checkout on that host is refused
     `branch_in_use`. Before the row is stored the hub checks another host is
     connected and accepting; with none, the hand-over is refused here with the
     owner notice naming `branch_in_use` and no close is ever sent (hk 5311); its own report
     path; no start conditions; and a spec checkout at the pushed commit — or,
     with a snapshot, at the snapshot commit fetched under its hand-over ref,
     still on the same branch so a plain `git push` continues it.
     A pinned profile is dropped, a pinned host is refused, the label is
     bounded, and the end reason and final submission are validated.
  2. **Recheck.** Before the close, the hub holds a row during a spawn drain,
     confirms the stop and task binding, and requires the same idle pane and
     working directory. Inside `pane.close_op`, after checking the job's
     attempt, the node reads the branch tip from origin before the final pane
     and open-question checks — for a snapshot hand-over, it instead proves
     the hand-over ref on origin still names the stored commit. It then
     checks the local worktree and HEAD with a two-second deadline as the
     last step before close: clean against the pushed tip, or, with a
     snapshot, HEAD still the snapshot's parent and the tracked state still
     matching the snapshot commit. A timeout, dirty or unpushed worktree —
     in the snapshot case any work the snapshot does not hold — refuses the
     close; its journalled reason makes the row `handover_failed` and the
     owner gets the fallback notice. A writer or resumed pane racing the
     close itself is not stopped: the continuation note says the old
     `host:path` may still exist until cleanup and names
     `jobs/<old job>/abandoned-worktree.patch` under the node inbox root —
     for a snapshot it also names the ref and the untracked paths, and the
     patch saves those files and later work. Ignored files are not
     preserved. The ordinary cleanup rules apply; this path creates no keep
     mark or lock.
  3. **Close.** The reconciler closes the old pane through `pane.close_op` to
     the job's node at its epoch, keeping `pane.close`'s open-question guard
     for an implementer. The node writes the old record `lost` with
     `end_reason="quota_stop pool=<pool> replaced_by=<new job>"`, the pinned
     format (`TestQuotaStopEndReasonFormatIsPinned`), before the submit,
     because the spawn refuses a task still bound to a job that is not lost.
     If the attempt remains unreadable or release cannot be proved for 30
     minutes, the hub sends the owner one notice naming the job, host, reason
     and row. The row stays pending and keeps the job fenced; the owner can
     check the host and hand the job over by hand. The one exception is a job
     whose directory the node proves gone (hk 5296): a node that advertises
     `close_missing_v1` fences the request with a durable tombstone and
     answers `missing_epoch`, and the row then ends `handover_failed` (reason
     `job_missing`) with the owner's notice and the fence released
     ([node-commands.md](node-commands.md#close-operations), "A missing
     job"). A node without it keeps the hold and the notice.
  4. **Submit.** Once the close is settled and the record reads `lost` with
     that reason, the stored successor is claimed with the same request id on
     every retry. A crash between the claim and saving that fact replays the
     same operation; nothing is captured or claimed twice.
  5. **Final.** The row stays non-final (I2) until the successor settles.
     Placed: the owner gets one info line, `[panewire] handed <old> over to
     <new> on <host>/<profile> (<pool> <cause>)`. Failed for good, or the old
     record ended another way: the row ends `handover_failed`, the task role
     is released, and the owner gets the hand-over-or-respawn notice with the
     reason.

The decision reads the task the job record names (a `--parent-job` successor
inherits it) and the job's start; a task whose `refs.job_id` is unavailable or names another job
is left alone and reported to the owner. For an implementer on a node without those capabilities, past
the replacement limit, or when no successor can be built (no branch, a pinned
host, an end reason over its limit, an unreadable pane or a refused read),
and for a reviewer or consult on a node that does not advertise
close operations, the hub records a `lane.event` asking the owner to hand the
job over or end it and respawn it on another pool. On a capable node, the hub
instead stores a close intent for a reviewer or consult, closes and settles it
through the node, confirms the planned hk `lost` record, then notifies the
owner to respawn it. A refused or unconfirmed close holds the notice. A job
whose id starts `mwr-` is reported to the `mergewake` lane. Notice producer
ids stay fixed across retries; a later hand-over after a refused attempt has
its own id, so its result is reported too.
The action reconciler runs independently of pane liveness and heartbeats.
The node and ownership contracts are in [node-commands.md](node-commands.md),
"Hub close protocol".

## Hub close (hk 4652)

When the hub ends a job by closing its pane with `pane.close_op`, the **node**
writes the planned terminal hk record, releases the quota lease and records
the final `job.lost` (`reason: "hub_close"`). The job's watcher only
defers while the close is open, including during a quota hand-off grace:
it writes neither the grace-expiry `quota_stop` record nor the
missing-pane `lost`, so the successor pointer is not lost to a competing
write. The node settles after every close answer, status and release, at
node start and every 30 seconds. The full writer, order and restart
contract is [node-commands.md](node-commands.md), "Hub close protocol".

