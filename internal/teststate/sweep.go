package teststate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SweepStale removes every entry directly under base that matches one of
// prefixes, is a directory owned by this user, and was last modified more than
// maxAge before now. It returns the paths it removed.
//
// It never follows a symbolic link: a name that is a link, or a file, is left
// alone, and the tree a link points at is never touched. A removal that fails
// (read-only entries are made writable first, but a directory another process
// still writes to may not be) is reported with the rest of the failures, and
// the remaining candidates are still swept.
func SweepStale(base string, now time.Time, maxAge time.Duration, prefixes ...string) ([]string, error) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	var removed []string
	var failures []error
	for _, entry := range entries {
		name := entry.Name()
		if !hasAnyPrefix(name, prefixes) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			// Renamed or removed between the directory read and the stat: not
			// ours to judge any more.
			continue
		}
		if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
			continue
		}
		if now.Sub(info.ModTime()) < maxAge {
			continue
		}
		if !ownedByCurrentUser(info) {
			continue
		}
		path := filepath.Join(base, name)
		if err := RemoveAll(path); err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", path, err))
			continue
		}
		removed = append(removed, path)
	}
	return removed, errors.Join(failures...)
}

func hasAnyPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
