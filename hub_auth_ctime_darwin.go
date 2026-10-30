package panewire

import (
	"os"
	"syscall"
)

// hubFileChangeTime is the inode change time, in nanoseconds; see sameHubAuthFile.
func hubFileChangeTime(info os.FileInfo) int64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return int64(stat.Ctimespec.Sec)*1e9 + int64(stat.Ctimespec.Nsec)
}
