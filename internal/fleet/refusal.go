package fleet

import (
	"errors"
	"slices"
	"strings"
)

// ErrQuotaRefreshPending leaves an accepted hub request for another placement
// pass after a measurement. It is never used by ordinary synchronous spawns.
var ErrQuotaRefreshPending = errors.New("quota refresh pending")

// parentJobNotFound is a --parent-job that resolved to no record after the
// bounded wait, as opposed to a job store that could not answer.
type parentJobNotFound struct{ cause error }

func (e *parentJobNotFound) Error() string { return e.cause.Error() }
func (e *parentJobNotFound) Unwrap() error { return e.cause }

// quotaHostRefusal is a target host's quota the hub could not confirm, with the
// shared-observation reason (no_usable_presence, stale, measure_failed, ...).
type quotaHostRefusal struct {
	cause  error
	reason string
}

func (e *quotaHostRefusal) Error() string { return e.cause.Error() }
func (e *quotaHostRefusal) Unwrap() error { return e.cause }

// AdmissionRefusalReason is the stable reason code for a spawn refused before
// any node was bound. docs/hub-spawn-operations.md lists the codes.
func AdmissionRefusalReason(err error) string {
	var parent *parentJobNotFound
	if errors.As(err, &parent) {
		return "parent_job_not_found"
	}
	var quota *quotaHostRefusal
	if errors.As(err, &quota) {
		return "quota_unconfirmed:" + quota.reason
	}
	if exitCode(err) == exitEvictionUnconfirmed {
		return "quota_unconfirmed"
	}
	return "admission_refused"
}

// SpawnRefusalFix is the repair hint for a refusal the hub observed in-process.
func SpawnRefusalFix(err error) string { return spawnRefusalFix(err) }

func spawnRefusalFix(err error) string {
	var parent *parentJobNotFound
	if errors.As(err, &parent) {
		return "Pass --parent-job with an existing job id, or use --task TASK_ID for top-level work; the job id may have been mistyped or never landed."
	}
	var quota *quotaHostRefusal
	if errors.As(err, &quota) {
		if quota.reason == "no_usable_presence" {
			return "No host measurement names an account for this pool on the target. Refresh the pool on a host that holds it with panewire quota refresh POOL, or choose a host that does."
		}
		return "Refresh the named pool on the target with panewire quota refresh POOL, confirm a fresh observation of its account, then retry."
	}
	rc := 0
	var exit *ExitError
	if errors.As(err, &exit) {
		rc = exit.Code
	}
	if row, ok := findSpawnRefusal(err.Error(), rc); ok {
		return row.hint
	}
	return "Run panewire preflight --json and repair the failed dependency or configuration named above before repeating the command."
}

// SpawnRefusalCode is the closed-set code for a refusal's text and exit status,
// or "" when the table has no row for it. The hub prefixes it to form the
// spawn_failed:<code> reason (docs/hub-spawn-operations.md).
func SpawnRefusalCode(text string, rc int) string {
	row, _ := findSpawnRefusal(text, rc)
	return row.code
}

// IsSpawnRefusalCode reports whether code is in the closed set. Sentry takes
// only these, so a tag or fingerprint cannot grow past the table.
func IsSpawnRefusalCode(code string) bool {
	return code != "" && slices.ContainsFunc(spawnRefusals, func(row spawnRefusal) bool { return row.code == code })
}

func findSpawnRefusal(text string, rc int) (spawnRefusal, bool) {
	for _, row := range spawnRefusals {
		if row.match == "" && row.rc == rc || row.match != "" && strings.Contains(text, row.match) {
			return row, true
		}
	}
	return spawnRefusal{}, false
}

// spawnRefusal is one row of the refusal table: what identifies the refusal, its
// code and its repair hint. A row matches on text, or on rc when match is empty.
type spawnRefusal struct {
	match string
	rc    int
	code  string
	hint  string
}

const (
	spawnWorkspaceHint = "Run herdr workspace list on the executing host; configure the workspace label (workspace = \"workers\") so spawn can recreate it when herdr has deleted it, or pass the exact -w WORKSPACE_ID for an ambiguous label."
	spawnBriefHint     = "Pass -p with a readable nonempty brief file; for presets, save it under jobs/JOB/brief.md or override -p."
	spawnPreflightHint = "Run panewire preflight --json to identify the failing host check; fix that check or pass an eligible --host with --override-reason."
)

// spawnRefusals is the closed set of spawn-failure codes. The first matching row
// wins, so its order is the precedence: the trust prompt's status, then text,
// then the remaining exit statuses. Codes match ^[a-z0-9_]+$ and are at most 40
// bytes.
var spawnRefusals = []spawnRefusal{
	// Same status as the trust prompt, so this text row must come first.
	{match: "HOOKS_REVIEW_REQUIRED", code: "hooks_review", hint: "The host owner reviews the hooks in Codex on the executing host (the executing CODEX_HOME), then repeat the same command; the brief was withheld and panewire never answers the screen."},
	// The real refusal names a path and the word "trust", so the status decides.
	{rc: exitTrustPrompt, code: "trust_prompt", hint: "Approve only this worktree in the executing CODEX_HOME, then repeat the same command; the brief was withheld until trust is confirmed."},
	// These name a path or an id, which the broader matches below could hit.
	// Auto placement exhaustion lists the tried hosts, whose notes can say "brief".
	{match: "no remote candidate completed", code: "local_disabled", hint: "Read the tried list above: add the repository to that node's spawn.json.cwd_map or repair the named refusal, or enable local.spawn on this host; then replay the saved request."},
	{match: "no placement candidate accepted", code: "placement", hint: "Read the tried list above; add the repository to that node's spawn.json.cwd_map or repair the named refusal, then replay the saved request."},
	// NO_ELIGIBLE_HOST with every candidate dropped for an unmapped repo: the
	// repair is the node's cwd_map or work root, not the quota account.
	{match: "no node maps repo", code: "placement", hint: "Add the repository to that node's spawn.json.cwd_map, or give the node a work root so it can clone the checkout, then retry."},
	// Admission refused before a host was bound: the cause the hub reports in
	// Sentry's admission_refused class (docs/observability.md).
	{match: "missing seat policy", code: "seat_policy_missing", hint: "Run panewire resolve --role ROLE --t Tn to select a rung with an admitted seat policy, or repair the policy that omits this profile, then retry."},
	{match: "quota catalog", code: "quota_catalog", hint: "Read the reason after quota catalog: for an unknown, retired or unplaced rung (e6_arm_required) select an admitted rung with panewire resolve, or set only the exact PANEWIRE_E6_ARM the operator authorized; for a stale or unreadable catalog check handoffkeep's bench catalog with panewire catalog list. Then retry; admission cannot decide without it."},
	{match: "symlink path refused", code: "symlink_path", hint: "Replace the named symlink with a real directory or point the executing node's jobs root at a symlink-free path, then replay the request."},
	{match: "report must be under", code: "report_path", hint: "Pass --report inside this job's own jobs/JOB directory, or its --parent-job directory, then retry."},
	{match: "cwd is not a directory", code: "cwd_not_directory", hint: "Run panewire preflight --json on the executing node and repair its checkout or spawn.json.cwd_map path, then replay the request."},
	{match: "task does not exist", code: "task_not_found", hint: "Run handoffkeep tasks show TASK_ID on the executing node; if the task exists, that node's handoffkeep points at another server."},
	{match: "task has another claimant", code: "task_claimed", hint: "Run handoffkeep tasks show TASK_ID, then release or rebind the stale claim, or use a different task for new work."},
	{match: "OMP_SETUP_WIZARD", code: "omp_setup_wizard", hint: "The installed omp no longer honors OMP_SKIP_SETUP. Run omp once on the executing host to finish setup, then repeat the same command after cleanup succeeds."},
	{match: "agent not in foreground", code: "foreground_missing", hint: "Inspect the failed pane with panewire find JOB, verify the selected harness binary starts on that host, then repeat the same command after cleanup succeeds."},
	{match: "refusing broad workspace trust", code: "trust_too_broad", hint: "Pass -c with the specific worktree path; do not grant trust to a broad parent directory."},
	{match: "malformed workspace trust store", code: "trust_store_malformed", hint: "Repair the selected harness's workspace trust store on the executing host, preserving unrelated entries, then repeat the same command."},
	{match: "trust", code: "trust_store", hint: "Inspect the selected harness's workspace trust store on the executing host and approve only this worktree, then repeat the same command."},
	{match: "entries; run herdr workspace list", code: "herdr_workspace_ambiguous", hint: spawnWorkspaceHint},
	{match: "workspace", code: "workspace_unresolved", hint: spawnWorkspaceHint},
	// Both exit 76 and say "brief", so the broad row below would misname them.
	// An unconfirmed submission is not a bad brief: point at the pane capture.
	{match: "BRIEF_SUBMISSION_UNCONFIRMED", code: "submit_unconfirmed", hint: "Read the saved jobs/JOB/spawn-failure-*.txt capture. If the brief sat in the composer on a loaded host, raise local.brief_submit_timeout_ms there; retry the same command after verified cleanup."},
	{match: "agent must be idle before initial brief", code: "submit_unconfirmed", hint: spawnBriefHint},
	// hk's secret guard names the pattern, never the matched text; the row
	// must win over the broad "brief" match below so the SPAWN_FAILED reason
	// keeps the guard's code and this hint. The code itself cannot be
	// secret_like_content: the reason's ": " separator would read as hk's
	// free-form "code: reason" and the redactor would eat it.
	{match: "secret_like_content", code: "hk_guard_refused", hint: "hk's secret guard refused the write as secret-like content and named the pattern in the reason; remove the secret-shaped text from the brief or task field and retry."},
	{match: "brief", code: "brief_invalid", hint: spawnBriefHint},
	{match: "task title", code: "task_title", hint: "Set the task title marker to (grade X, Tn), matching --t, then retry."},
	{match: "task scale", code: "task_scale", hint: "Make --t match the task title's T level, then retry."},
	{match: "task bound", code: "task_bound", hint: "Use the job id already bound to this task, or a different task for new work."},
	{match: "task unavailable", code: "task_unavailable", hint: "Run handoffkeep tasks show TASK_ID and restore the task to an available state before retrying."},
	{match: "--owner", code: "owner_flag", hint: "Pass --owner LANE explicitly."},
	{match: "containment_required", code: "containment_required", hint: "Use --containment hk:<task id> for a pin that works around a problem, or --operator-request REF for an approved pin."},
	{match: "--override-reason", code: "override_reason_required", hint: "Use -m auto --host auto, or provide --override-reason TEXT for an intentional pin."},
	{match: "role_restricted:", code: "role_restricted", hint: "Pick a seat the refusal's rule admits: the role or profile it names, or a rung whose role list covers the request, then retry."},
	{match: "alias", code: "host_alias", hint: "Run panewire hosts and use the canonical host key, or fix the aliases list in the hosts config."},
	{match: "requires PANEWIRE_E6_ARM=", code: "e6_arm_required", hint: "Set the exact PANEWIRE_E6_ARM value named in the refusal only if the operator authorized that rung; otherwise select an admitted rung."},
	{rc: exitDuplicate, code: "duplicate", hint: "Inspect the existing job with panewire find JOB. Use a new --job only for new work; never override a live duplicate. Failed launches with verified cleanup can retry on the same host."},
	{rc: exitSubmitUnconfirmed, code: "submit_unconfirmed", hint: "Read the saved jobs/JOB/spawn-failure-*.txt capture and fix the harness submission failure; retry the same command after verified cleanup."},
	{rc: exitAuthUnavailable, code: "auth_unavailable", hint: "Sign in to the selected quota account on the executing host and refresh that pool with panewire quota refresh POOL, then retry."},
	{rc: exitQuotaUnknown, code: "quota_unknown", hint: "Run panewire quota refresh POOL for the named pool, confirm a fresh quota observation, then retry."},
	{rc: exitLocalDisabled, code: "local_disabled", hint: "Enable local.spawn in the hosts config or configure an admitted remote host, then retry."},
	{rc: exitPlacement, code: "placement", hint: spawnPreflightHint},
	{rc: exitPreflight, code: "preflight", hint: spawnPreflightHint},
	{rc: exitSeatRefused, code: "seat_refused", hint: "Use panewire resolve --role ROLE --t Tn to select a rung admitted for the requested role and task, then retry with that policy."},
	{rc: exitLaneUnregistered, code: "lane_unregistered", hint: "Run panewire lanes to inspect routing and register the committed pane's lane before dispatching more work; do not relaunch the live job."},
	{rc: exitStateConflict, code: "state_conflict", hint: "Inspect the job's saved report and events under jobs/JOB; reconcile the conflicting task/job binding before retrying."},
	{rc: exitRoleWait, code: "role_wait", hint: "Wait until a listed eligible pool resets, refresh it with panewire quota refresh POOL, then retry."},
	{rc: exitRoleEscalate, code: "role_escalate", hint: "Emit a decision to the parent lane naming the exhausted role pools; choose another authorized policy only after its reply."},
	{rc: exitUsage, code: "usage", hint: "Run panewire spawn --help and supply or correct the exact flag named in the refusal."},
}
