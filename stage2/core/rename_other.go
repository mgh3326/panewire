//go:build !darwin && !linux

package core

import "golang.org/x/sys/unix"

// Link/unlink is the conservative no-clobber fallback on Unix variants that
// expose neither Darwin renameatx_np nor Linux renameat2.
func renameNoReplace(fromFD int, from string, toFD int, to string) error {
	if err := unix.Linkat(fromFD, from, toFD, to, 0); err != nil {
		return err
	}
	return unix.Unlinkat(fromFD, from, 0)
}
