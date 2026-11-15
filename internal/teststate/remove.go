package teststate

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// RemoveAll removes path and everything under it, making every directory
// writable and searchable first. A Go module cache is 0555: os.RemoveAll alone
// fails to unlink from a directory it cannot write, and leaves the tree behind.
// A path that does not exist is not an error.
//
// It never follows a symbolic link: a link is removed, the tree it names is
// not.
func RemoveAll(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return os.Remove(path)
	}
	return removeDir(path)
}

func removeDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.Mode().Perm()&0o700 != 0o700 {
		if err := os.Chmod(path, info.Mode().Perm()|0o700); err != nil {
			return err
		}
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		child := filepath.Join(path, entry.Name())
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return err
		}
		if info.IsDir() {
			if err := removeDir(child); err != nil {
				return err
			}
			continue
		}
		if err := os.Remove(child); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return os.Remove(path)
}
