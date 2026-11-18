package fleet

import "strings"

// trimpathFlag is the go flag that keeps absolute source paths out of a
// package's action id and its built binary (hk 5160).
const trimpathFlag = "-trimpath"

// goFlagsForPane returns the GOFLAGS a spawned pane takes: the spawner's own
// value with trimpathFlag appended when it is absent, so an existing flag is
// kept, never replaced. A value that already carries the flag is returned
// unchanged. A later -trimpath wins over an earlier -trimpath=false because the
// go command's flag parser keeps the last boolean occurrence.
//
// The flag belongs in every fleet pane because each session works in its own
// worktree directory: without it the main module's directory is part of the
// action id, so two worktrees of one commit share no cache entry.
func goFlagsForPane(existing string) string {
	if hasTrimpathFlag(existing) {
		return existing
	}
	return strings.TrimSpace(existing + " " + trimpathFlag)
}

func hasTrimpathFlag(flags string) bool {
	for _, f := range strings.Fields(flags) {
		if f == trimpathFlag {
			return true
		}
	}
	return false
}
