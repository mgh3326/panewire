package fleet

import (
	"errors"
	"strings"
)

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
	var exit *ExitError
	if errors.As(err, &exit) && exit.Code == exitTrustPrompt {
		return "Approve only this worktree in the executing CODEX_HOME, then repeat the same command; the brief was withheld until trust is confirmed."
	}
	var parent *parentJobNotFound
	if errors.As(err, &parent) {
		return "Pass --parent-job with an existing job id, or use --task TASK_ID for top-level work; the job id may have been mistyped or never landed."
	}
	var quota *quotaHostRefusal
	if errors.As(err, &quota) {
		if quota.reason == "no_usable_presence" {
			return "No host measurement names an account for this pool on the target. Refresh the pool on a host that holds it with pacelever refresh POOL, or choose a host that does."
		}
		return "Refresh the named pool on the target with pacelever refresh POOL, confirm a fresh observation of its account, then retry."
	}
	message := err.Error()
	for _, fix := range []struct{ match, hint string }{
		// These name a path or an id, which the broader matches below could hit.
		{"BRIEF_SUBMISSION_UNCONFIRMED", "Read the saved jobs/JOB/spawn-failure-*.txt capture. If the brief sat in the composer on a loaded host, raise local.brief_submit_timeout_ms there; retry the same command after verified cleanup."},
		{"no remote candidate completed", "Read the tried list above; add the repository to that node's spawn.json.cwd_map or repair the named refusal, then replay the saved request."},
		{"no placement candidate accepted", "Read the tried list above; add the repository to that node's spawn.json.cwd_map or repair the named refusal, then replay the saved request."},
		{"symlink path refused", "Replace the named symlink with a real directory or point the executing node's jobs root at a symlink-free path, then replay the request."},
		{"report must be under", "Pass --report inside this job's own jobs/JOB directory, or its --parent-job directory, then retry."},
		{"cwd is not a directory", "Run panewire preflight --json on the executing node and repair its checkout or spawn.json.cwd_map path, then replay the request."},
		{"task does not exist", "Run handoffkeep tasks show TASK_ID on the executing node; if the task exists, that node's handoffkeep points at another server."},
		{"task has another claimant", "Run handoffkeep tasks show TASK_ID, then release or rebind the stale claim, or use a different task for new work."},
		{"OMP_SETUP_WIZARD", "The installed omp no longer honors OMP_SKIP_SETUP. Run omp once on the executing host to finish setup, then repeat the same command after cleanup succeeds."},
		{"agent not in foreground", "Inspect the failed pane with panewire find JOB, verify the selected harness binary starts on that host, then repeat the same command after cleanup succeeds."},
		{"refusing broad workspace trust", "Pass -c with the specific worktree path; do not grant trust to a broad parent directory."},
		{"malformed workspace trust store", "Repair the selected harness's workspace trust store on the executing host, preserving unrelated entries, then repeat the same command."},
		{"trust", "Inspect the selected harness's workspace trust store on the executing host and approve only this worktree, then repeat the same command."},
		{"workspace", "Run herdr workspace list on the executing host; set local.workspace to its id or pass -w WORKSPACE_ID."},
		{"brief", "Pass -p with a readable nonempty brief file; for presets, save it under jobs/JOB/brief.md or override -p."},
		{"task title", "Set the task title marker to (grade X, Tn), matching --t, then retry."},
		{"task scale", "Make --t match the task title's T level, then retry."},
		{"task bound", "Use the job id already bound to this task, or a different task for new work."},
		{"task unavailable", "Run handoffkeep tasks show TASK_ID and restore the task to an available state before retrying."},
		{"--owner", "For lead use --lane and --parent without --owner; for work pass --owner or set PANEWIRE_LANE."},
		{"--override-reason", "Use -m auto --host auto, or provide --override-reason TEXT for an intentional pin."},
		{"alias", "Run panewire hosts and use the canonical host key, or fix the aliases list in the hosts config."},
		{"requires PANEWIRE_E6_ARM=", "Set the exact PANEWIRE_E6_ARM value named in the refusal only if the operator authorized that rung; otherwise select an admitted rung."},
	} {
		if strings.Contains(message, fix.match) {
			return fix.hint
		}
	}
	if exit != nil {
		switch exit.Code {
		case exitDuplicate:
			return "Inspect the existing job with panewire find JOB. Use a new --job only for new work; never override a live duplicate. Failed launches with verified cleanup can retry on the same host."
		case exitSubmitUnconfirmed:
			return "Read the saved jobs/JOB/spawn-failure-*.txt capture and fix the harness submission failure; retry the same command after verified cleanup."
		case exitAuthUnavailable:
			return "Sign in to the selected quota account on the executing host and refresh that pool with pacelever refresh POOL, then retry."
		case exitQuotaUnknown:
			return "Run pacelever refresh POOL for the named pool, confirm a fresh quota observation, then retry."
		case exitLocalDisabled:
			return "Enable local.spawn in the hosts config or configure an admitted remote host, then retry."
		case exitPlacement, exitPreflight:
			return "Run panewire preflight --json to identify the failing host check; fix that check or pass an eligible --host with --override-reason."
		case exitSeatRefused:
			return "Use panewire resolve --role ROLE --t Tn to select a rung admitted for the requested role and task, then retry with that policy."
		case exitLaneUnregistered:
			return "Run panewire lanes to inspect routing and register the committed pane's lane before dispatching more work; do not relaunch the live job."
		case exitStateConflict:
			return "Inspect the job's saved report and events under jobs/JOB; reconcile the conflicting task/job binding before retrying."
		case exitRoleWait:
			return "Wait until a listed eligible pool resets, refresh it with pacelever refresh POOL, then retry."
		case exitRoleEscalate:
			return "Emit a decision to the parent lane naming the exhausted role pools; choose another authorized policy only after its reply."
		case exitUsage:
			return "Run panewire spawn --help and supply or correct the exact flag named in the refusal."
		}
	}
	return "Run panewire preflight --json and repair the failed dependency or configuration named above before repeating the command."
}
