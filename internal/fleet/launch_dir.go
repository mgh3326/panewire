package fleet

import (
	"errors"
	"os"
	"path/filepath"
)

// LaunchDir is the directory a node launches a spawn in. It is handed to
// ExecuteNode in process: no flag and no wire field sets it.
//
// A cwd_map or default repo directory is LaunchDir{Path: dir}, and the launcher
// enters its -L lane itself, as a local spawn does. A checkout spawn's comes
// from BindLaunchDir, which resolves the lane once after prepare and binds the
// directory it found (hk 6431). The launcher then opens the tab at Path, joins
// no lane to it, and refuses with target_changed when Path no longer names the
// bound directory (docs/hub-spawn-operations.md).
type LaunchDir struct {
	Path string

	bound bool
	// identity is the bound directory's device and inode (os.SameFile). Nil on
	// a bound LaunchDir means Path was no directory: no directory matches it.
	identity os.FileInfo
}

// ErrLaneOutsideWorktree reports a -L lane that leads out of its prepared
// worktree, or a lane link that resolves to nothing.
var ErrLaneOutsideWorktree = errors.New("lane outside worktree")

// BindLaunchDir resolves where a spawn runs under the prepared worktree and
// binds that directory:
//
//   - no lane, nothing at worktree/lane, or something there that is neither a
//     directory nor a link: the worktree;
//   - a directory there: that directory;
//   - a link there: the directory it resolves to, under its resolved path, so
//     no later lookup follows the link. A link to a file leaves the spawn in
//     the worktree.
//
// A lane that resolves outside the worktree, and a link that does not resolve,
// are ErrLaneOutsideWorktree: the agent would run outside the held job dir.
func BindLaunchDir(worktree, lane string) (LaunchDir, error) {
	path := worktree
	if lane != "" {
		candidate := filepath.Join(worktree, lane)
		info, err := os.Lstat(candidate)
		if err == nil && (info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			target, err := resolveInside(worktree, candidate)
			if err != nil {
				return LaunchDir{}, err
			}
			if info.IsDir() {
				path = candidate
			} else if at, err := os.Stat(target); err == nil && at.IsDir() {
				path = target
			}
		}
	}
	dir := LaunchDir{Path: path, bound: true}
	// Lstat, as verify does: a link at the path is never the bound directory.
	if info, err := os.Lstat(path); err == nil && info.IsDir() {
		dir.identity = info
	}
	return dir, nil
}

// resolveInside resolves path and returns it when it is the worktree or lies in
// it, both with every link followed; otherwise ErrLaneOutsideWorktree.
func resolveInside(worktree, path string) (string, error) {
	root, err := filepath.EvalSymlinks(worktree)
	if err != nil {
		return "", ErrLaneOutsideWorktree
	}
	target, err := filepath.EvalSymlinks(path)
	if err != nil || target != root && !within(root, target) {
		return "", ErrLaneOutsideWorktree
	}
	return target, nil
}

// verify refuses a bound launch whose path is not the bound directory now. A
// LaunchDir that binds nothing has nothing to verify. path is what the launcher
// is about to use, so a path other than the bound one is refused too.
func (d LaunchDir) verify(path string) error {
	if !d.bound {
		return nil
	}
	info, err := os.Lstat(path)
	if err == nil && info.IsDir() && d.identity != nil && os.SameFile(d.identity, info) {
		return nil
	}
	return fail(exitStateConflict, "target_changed: %s is not the directory this spawn bound after prepare; nothing is launched there", path)
}
