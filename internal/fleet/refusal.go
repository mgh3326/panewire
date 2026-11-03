package fleet

import (
	"errors"
	"strings"
)

func spawnRefusalFix(err error) string {
	var exit *ExitError
	if errors.As(err, &exit) && exit.Code == exitTrustPrompt {
		return "Approve only this worktree in the executing CODEX_HOME, then repeat the same command; the brief was withheld until trust is confirmed."
	}
	message := err.Error()
	for _, fix := range []struct{ match, hint string }{
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
			return "Read the saved jobs/JOB/spawn-failed-screen.txt and fix the harness submission failure; retry the same command after verified cleanup."
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
