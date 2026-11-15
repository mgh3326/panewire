package panewire

import "io"

// runNodeCLI dispatches `panewire node <verb>`: the node-side operator verbs
// that act on one host's worktrees, panes and job directories.
func runNodeCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 {
		return ExitUsage
	}
	switch args[0] {
	case "worktree-list":
		return runNodeWorktreeListCLI(args[1:], stdout, stderr, deps)
	case "pane-read":
		return runNodePaneReadCLI(args[1:], stdout, stderr, deps)
	case "job-result":
		return runNodeJobResultCLI(args[1:], stdout, stderr, deps)
	case "job-done":
		return runNodeJobDoneCLI(args[1:], stdout, stderr, deps)
	}
	return ExitUsage
}
