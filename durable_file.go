package panewire

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"syscall"
)

// Durable file publication is shared by every inbox, store, and policy
// writer so each one gives the same guarantee: the file is written under a
// temporary name in the destination directory, carries its final mode before
// any byte is written, and is fsynced before it becomes visible under its
// final name. Callers pick the publish step (replace with rename, or
// no-clobber with link); the temporary file is always theirs to remove.

// writeSyncedTemporaryFile creates a temporary file in dir from pattern,
// writes contents with perm, fsyncs and closes it, and returns its path. The
// caller removes the path after publishing it (a removal after a link or
// rename is a harmless no-op on the published name).
func writeSyncedTemporaryFile(dir, pattern string, contents []byte, perm os.FileMode) (string, error) {
	temporary, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := temporary.Name()
	if err := temporary.Chmod(perm); err != nil {
		_ = temporary.Close()
		_ = os.Remove(name)
		return "", err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// replaceFileAtomically publishes contents at path with perm, replacing any
// existing file. Readers see either the previous file or the complete new one.
func replaceFileAtomically(path, pattern string, contents []byte, perm os.FileMode) error {
	name, err := writeSyncedTemporaryFile(filepath.Dir(path), pattern, contents, perm)
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := os.Rename(name, path); err != nil {
		return err
	}
	syncDirectoryBestEffort(filepath.Dir(path))
	return nil
}

// replaceFileAtomicallyInRoot is replaceFileAtomically confined to root: name
// is relative to root and nothing outside it can be reached through a
// symlink. The temporary file is created O_EXCL|O_NOFOLLOW, and the rename
// replaces a symlink at name rather than writing through it.
func replaceFileAtomicallyInRoot(root *os.Root, name string, contents []byte, perm os.FileMode) error {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return err
	}
	dir := filepath.Dir(name)
	temporaryName := filepath.Join(dir, "."+filepath.Base(name)+"-"+hex.EncodeToString(suffix))
	temporary, err := root.OpenFile(temporaryName, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, perm)
	if err != nil {
		return err
	}
	defer root.Remove(temporaryName)
	if err := temporary.Chmod(perm); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := root.Rename(temporaryName, name); err != nil {
		return err
	}
	if directory, err := root.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

// syncDirectoryBestEffort fsyncs dir after a rename or link. Directory sync is
// best effort across the supported filesystems: the rename or link is the
// visibility boundary, so a directory fsync failure must not turn a
// successfully published, parseable file into a reported failure.
func syncDirectoryBestEffort(dir string) {
	if directoryFile, err := os.Open(dir); err == nil {
		_ = directoryFile.Sync()
		_ = directoryFile.Close()
	}
}
