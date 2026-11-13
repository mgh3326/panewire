package panewire

import "strings"

// spawnRefusalReplayable reports whether replaying the same saved request can
// succeed once an operator has done what the refusal's fix says. It is the one
// place that decides a node refusal is final (`failed`, reservation kept) or
// replayable (`retryable`, reservation released); completeSpawn applies it to
// every outcome, so no completion path classifies on its own.
//
// The checkout refusals follow node_spawn_refusal.go: a fix that ends in
// "replay the same saved request" (busy, dirty, sha-missing, work_root_unset)
// is replayable, and so is git_too_old, which the operator repairs by upgrading
// git. origin-mismatch and branch-elsewhere ask the operator to inspect and
// correct the node's checkout mapping, so the same request meets the same
// conflict. The argument refusals are the request's own flags: replaying the
// saved request meets the same rule, so the operator corrects it instead.
// Every other reason keeps its caller's classification.
func spawnRefusalReplayable(reason string) bool {
	code, _, _ := strings.Cut(reason, ": ")
	switch code {
	case "checkout_refused:origin-mismatch", "checkout_refused:branch-elsewhere", "invalid_args", "invalid_request":
		return false
	}
	return true
}

// spawnSubmitterLane is the lane that submitted the spawn, when it is not the
// task owner. `--requested-by` names the submitting session on the operator
// flag set; `--parent` is the lane that spawned the job. `--lane` is
// the spawned pane's own lane, so it never names the submitter.
func spawnSubmitterLane(args []string) string {
	for _, flag := range []string{"--requested-by", "--parent"} {
		if lane := hubSpawnArg(args, flag); LaneNamePattern.MatchString(lane) {
			return lane
		}
	}
	return ""
}
