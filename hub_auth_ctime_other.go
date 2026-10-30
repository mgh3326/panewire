//go:build !darwin && !linux

package panewire

import "os"

// hubFileChangeTime has no portable source here; sameHubAuthFile then relies
// on identity, mode, size and mtime alone.
func hubFileChangeTime(os.FileInfo) int64 { return 0 }
