package fleet

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"syscall"
)

// TrustGuard validates executable ownership and the canonical task/lease roots.
// Tests inject Runner directly; no environment variable disables this guard.
func TrustBinary(path string, uid uint32) error {
	resolved, e := filepath.EvalSymlinks(path)
	if e != nil {
		return fail(exitConfig, "unusable trusted binary: %v", e)
	}
	info, e := os.Stat(resolved)
	if e != nil {
		return e
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || stat.Uid != uid || info.Mode().Perm()&0111 == 0 {
		return fail(exitConfig, "binary must be executable, owned by this uid and not group/world writable: %s", path)
	}
	return nil
}
func (a *App) trust() error {
	u, e := user.Current()
	if e != nil {
		return fail(exitConfig, "cannot determine canonical home")
	}
	canonical := u.HomeDir
	if filepath.Clean(a.Home) != filepath.Clean(canonical) || filepath.Clean(a.Jobs) != filepath.Join(canonical, "work", "herdr-inbox", "jobs") {
		return fail(exitConfig, "HOME and ARBITER_INBOX_ROOT must select canonical paths")
	}
	if data := os.Getenv("XDG_DATA_HOME"); data != "" && filepath.Clean(data) != filepath.Join(canonical, ".local", "share") {
		return fail(exitConfig, "XDG_DATA_HOME must select the canonical local ledger state")
	}
	for _, bin := range []string{"handoffkeep"} {
		want := filepath.Join(canonical, ".local", "bin", bin)
		selected := a.Bins[bin]
		if selected == "" {
			selected = bin
		}
		path, e := exec.LookPath(selected)
		if e != nil {
			return e
		}
		real, e := filepath.EvalSymlinks(path)
		if e != nil {
			return e
		}
		trusted, e := filepath.EvalSymlinks(want)
		if e != nil || real != trusted {
			return fail(exitConfig, "%s must select canonical %s", bin, want)
		}
		if e = TrustBinary(path, uint32(os.Getuid())); e != nil {
			return e
		}
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	return TrustBinary(exe, uint32(os.Getuid()))
}
