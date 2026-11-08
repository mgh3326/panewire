package panewire

import "strings"

func nodeSpawnRefusalFix(code, rule string) string {
	switch code {
	case "spawn_disabled":
		switch rule {
		case "workspace_required":
			return "Run herdr workspace list on this node and set spawn.json.workspace to its configured default id."
		case "herdr_session_required":
			return "Set spawn.json.herdr_session to the node's configured herdr session."
		case "enabled_false":
			return "Choose an enabled node or have the operator enable its spawn.json policy."
		case "config_json_invalid", "config_trailing_data":
			return "Correct spawn.json JSON and remove retired runtug_path or other unknown keys."
		case "cwd_map_key_pattern", "cwd_map_dir_absolute":
			return "Correct the named spawn.json.cwd_map key and give it an absolute node-local path."
		default:
			return "Create or repair the configured regular spawn.json file with enabled, workspace, herdr_session and cwd_map."
		}
	case "cwd_unmapped":
		return "Add the requested repository key to spawn.json.cwd_map with its absolute node-local path, or choose a mapped node."
	case "brief_doc_unavailable":
		return "Publish the brief under brief_doc_prefix and repair the node's mode-0600 handoffkeep_env credential file."
	case "timeout":
		return "Inspect the saved operation and same node journal; reconcile the attempt before retrying or closing any pane."
	}
	if reason, ok := strings.CutPrefix(code, "checkout_refused:"); ok {
		switch reason {
		case "cancelled":
			return "the node ran past the hub deadline; check the node's load, then replay the saved request"
		case "work_root_unset":
			return "Set spawn.json.work_root to an existing absolute node-local work root and replay the saved request."
		case "busy":
			return "Wait for the node's current checkout operation to finish, then replay the same saved request."
		case "dirty":
			return "Preserve or commit changes in the node checkout before replaying the same saved request."
		case "origin-mismatch", "branch-elsewhere":
			return "Inspect the node checkout's origin and worktree binding; correct the conflicting mapping without overwriting unrelated work."
		case "sha-missing":
			return "Push the requested commit to the specified origin ref, then replay the saved request."
		case "git_too_old":
			return "Upgrade git on the executing node to a version supporting the guarded worktree operations."
		default:
			return "Run panewire preflight --json on the executing node and repair the reported checkout refusal before replaying this request."
		}
	}
	return "Inspect the saved operation and executing node; reconcile unknown ownership before retrying."
}
