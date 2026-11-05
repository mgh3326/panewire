package panewire

import (
	"os"

	"github.com/mgh3326/panewire/internal/durable"
)

// The durable file helpers live in internal/durable so internal packages
// share them; these wrappers keep the root package's call sites short.

func writeSyncedTemporaryFile(dir, pattern string, contents []byte, perm os.FileMode) (string, error) {
	return durable.WriteSyncedTemporaryFile(dir, pattern, contents, perm)
}

func replaceFileAtomically(path, pattern string, contents []byte, perm os.FileMode) error {
	return durable.ReplaceFileAtomically(path, pattern, contents, perm)
}

func replaceFileAtomicallyInRoot(root *os.Root, name string, contents []byte, perm os.FileMode) error {
	return durable.ReplaceFileAtomicallyInRoot(root, name, contents, perm)
}

func syncDirectoryBestEffort(dir string) { durable.SyncDirectoryBestEffort(dir) }
