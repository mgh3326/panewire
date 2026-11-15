//go:build unix

package teststate

import (
	"io/fs"
	"os"
	"syscall"
)

// ownedByCurrentUser reports whether info describes an entry this user owns. A
// stale directory left by another account in a shared temp dir is not ours to
// remove.
func ownedByCurrentUser(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return true
	}
	return int(stat.Uid) == os.Geteuid()
}
