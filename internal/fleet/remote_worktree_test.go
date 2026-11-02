package fleet

import (
	"context"
	"strings"
	"testing"
)

// remoteWT is a fake ssh target holding the state ensureWorktree probes.
type remoteWT struct {
	cloneExists   bool
	wtExists      bool
	wtBranch      string
	haveLocalRef  bool
	haveRemoteRef bool
	defaultBase   string
	addCode       int
	ancestorCode  int
	cmds          []string
}

func (r *remoteWT) ssh(args string) Result {
	r.cmds = append(r.cmds, args)
	has := func(s string) bool { return strings.Contains(args, s) }
	code := func(ok bool) Result {
		if ok {
			return Result{}
		}
		return Result{Code: 1}
	}
	switch {
	case has("_spawn-stdin"):
		return Result{Out: "OK pane=w:p1\n"}
	case has("test -d") && has("repo-a.wk"):
		return code(r.wtExists)
	case has("test -d"):
		return code(r.cloneExists)
	case has("remote get-url origin"):
		return Result{Out: "git@git.invalid:org/repo-a.git\n"}
	case has("branch --show-current"):
		return Result{Out: r.wtBranch + "\n"}
	case has("show-ref"):
		return code(r.haveLocalRef)
	case has("rev-parse --verify"):
		return code(r.haveRemoteRef)
	case has("symbolic-ref"):
		if r.defaultBase == "" {
			return Result{Code: 128, Err: "fatal: not a symbolic ref"}
		}
		return Result{Out: r.defaultBase + "\n"}
	case has("worktree add"):
		return Result{Code: r.addCode, Err: "fatal: worktree add failed"}
	case has("merge-base --is-ancestor"):
		return Result{Code: r.ancestorCode}
	}
	return Result{}
}

func (r *remoteWT) saw(sub string) bool {
	for _, c := range r.cmds {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

// worktreeFixture places a job whose local checkout is a linked worktree
// (repo-a.wk of repo-a) on one remote host named h1.
func worktreeFixture(t *testing.T, remote *remoteWT, branch string, hosts map[string]map[string]any) (*App, Spawn, *fakeRunner) {
	t.Helper()
	src := &fakePlacementSource{hosts: []HostView{{Name: "h1", Connected: true, LoadRatio: ptrF(0.1)}, {Name: "h2", Connected: true, LoadRatio: ptrF(0.5)}}}
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", remote.ssh)
	base := f.Do
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		if c.Name == "git" {
			switch {
			case strings.Contains(args, "--show-toplevel"):
				return Result{Out: a.Home + "/work/repo-a.wk\n"}
			case strings.Contains(args, "--git-common-dir"):
				return Result{Out: a.Home + "/work/repo-a/.git/worktrees/repo-a.wk\n" + a.Home + "/work/repo-a/.git\n"}
			case strings.Contains(args, "branch --show-current"):
				return Result{Out: branch + "\n"}
			case strings.HasSuffix(args, "rev-parse HEAD"):
				return Result{Out: "0123456789abcdef0123456789abcdef01234567\n"}
			}
		}
		return base(c)
	}
	s.CWD = a.Home + "/work/repo-a.wk"
	a.Config.Hosts = hosts
	a.Config.HostOrder = []string{"h1", "h2"}
	return a, s, f
}

func spawnInput(t *testing.T, f *fakeRunner) string {
	t.Helper()
	for _, c := range f.Calls {
		if c.Name == "ssh" && strings.Contains(strings.Join(c.Args, " "), "_spawn-stdin") {
			return c.Input
		}
	}
	return ""
}

func indexOf(cmds []string, sub string) int {
	for i, c := range cmds {
		if strings.Contains(c, sub) {
			return i
		}
	}
	return -1
}

func TestRemoteWorktreeMissingIsPreparedFromOriginDefault(t *testing.T) {
	r := &remoteWT{cloneExists: true, defaultBase: "origin/main"}
	a, s, f := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": {}, "h2": {}})
	handled, rec, e := a.placeAuto(context.Background(), s, "brief")
	if e != nil || !handled || rec["chose"] != "h1" {
		t.Fatalf("handled=%v rec=%v err=%v", handled, rec, e)
	}
	add := indexOf(r.cmds, "worktree add")
	if add < 0 || !r.saw("fetch origin") {
		t.Fatalf("worktree must be fetched and added: %v", r.cmds)
	}
	if !strings.Contains(r.cmds[add], `-b 'wk/feat'`) || !strings.Contains(r.cmds[add], `'origin/main'`) || !strings.Contains(r.cmds[add], `"$HOME"'/work/repo-a.wk'`) {
		t.Fatalf("new branch must start from origin's default under the work root: %s", r.cmds[add])
	}
	if r.saw("git clone") {
		t.Fatal("an existing clone must not be cloned again")
	}
	if add > indexOf(r.cmds, "_spawn-stdin") {
		t.Fatal("the worktree must exist before the spawn leg")
	}
	if !strings.Contains(spawnInput(t, f), `"~/work/repo-a.wk"`) {
		t.Fatalf("remote cwd must be the worktree: %s", spawnInput(t, f))
	}
}

func TestRemoteWorktreeClonesMissingRepoFirst(t *testing.T) {
	r := &remoteWT{defaultBase: "origin/main"}
	a, s, _ := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": {}, "h2": {}})
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	clone, add := indexOf(r.cmds, "git clone"), indexOf(r.cmds, "worktree add")
	if clone < 0 || add < 0 || clone > add {
		t.Fatalf("clone must precede the worktree add: %v", r.cmds)
	}
	if !strings.Contains(r.cmds[clone], "git@git.invalid:org/repo-a.git") || !strings.Contains(r.cmds[clone], `"$HOME"'/work/repo-a'`) {
		t.Fatalf("clone must use the local origin and the canonical name: %s", r.cmds[clone])
	}
}

func TestRemoteWorktreeTracksExistingRemoteBranch(t *testing.T) {
	r := &remoteWT{cloneExists: true, haveRemoteRef: true, defaultBase: "origin/main"}
	a, s, _ := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": {}, "h2": {}})
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	add := r.cmds[indexOf(r.cmds, "worktree add")]
	if !strings.Contains(add, `'origin/wk/feat'`) || strings.Contains(add, "origin/main") {
		t.Fatalf("a PR head must be tracked from origin: %s", add)
	}
	if r.saw("symbolic-ref") {
		t.Fatal("the default branch is not needed for an existing remote branch")
	}
}

func TestRemoteWorktreeExistingIsReused(t *testing.T) {
	r := &remoteWT{cloneExists: true, wtExists: true, wtBranch: "wk/feat"}
	a, s, _ := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": {}, "h2": {}})
	handled, rec, e := a.placeAuto(context.Background(), s, "brief")
	if e != nil || !handled || rec["chose"] != "h1" {
		t.Fatalf("%v %v", rec, e)
	}
	for _, sub := range []string{"fetch", "worktree add", "git clone"} {
		if r.saw(sub) {
			t.Fatalf("a reused worktree must not run %q: %v", sub, r.cmds)
		}
	}
}

func TestRemoteWorktreeFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		remote *remoteWT
		branch string
		want   string
	}{
		{"add fails", &remoteWT{cloneExists: true, defaultBase: "origin/main", addCode: 128}, "wk/feat", "worktree add"},
		{"existing on other branch", &remoteWT{cloneExists: true, wtExists: true, wtBranch: "main"}, "wk/feat", "not on branch wk/feat"},
		{"unknown default branch", &remoteWT{cloneExists: true}, "wk/feat", "default branch is unknown"},
		{"local commits missing remotely", &remoteWT{cloneExists: true, defaultBase: "origin/main", ancestorCode: 128}, "wk/feat", "push the branch and update"},
		{"reused worktree behind local HEAD", &remoteWT{cloneExists: true, wtExists: true, wtBranch: "wk/feat", ancestorCode: 1}, "wk/feat", "update the remote worktree"},
		{"detached local HEAD", &remoteWT{cloneExists: true, defaultBase: "origin/main"}, "", "detached HEAD"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f := worktreeFixture(t, tc.remote, tc.branch, map[string]map[string]any{"h1": {}, "h2": {}})
			a.Config.Local = map[string]any{"spawn": true}
			handled, _, e := a.placeAuto(context.Background(), s, "brief")
			if !handled || e == nil || !strings.Contains(e.Error(), tc.want) {
				t.Fatalf("want a refusal containing %q, got handled=%v err=%v", tc.want, handled, e)
			}
			if exitCode(e) != exitPlacement {
				t.Fatalf("exit %d", exitCode(e))
			}
			if spawnInput(t, f) != "" {
				t.Fatal("no spawn may follow a failed preparation, on this host or the next")
			}
		})
	}
}

func TestRemoteWorktreeUnreachableMovesOn(t *testing.T) {
	r := &remoteWT{cloneExists: true}
	a, s, _ := worktreeFixture(t, r, "wk/feat", map[string]map[string]any{"h1": {}, "h2": {}})
	base := a.Runner.(*fakeRunner).Do
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		if c.Name == "ssh" && strings.Contains(strings.Join(c.Args, " "), "h1 test -d") {
			return Result{Code: exitUnreachable}
		}
		return base(c)
	}
	r.defaultBase = "origin/main"
	handled, rec, e := a.placeAuto(context.Background(), s, "brief")
	if e != nil || !handled || rec["chose"] != "h2" {
		t.Fatalf("an unreachable host is skipped like before: %v %v", rec, e)
	}
}

func TestRemoteWorktreeDerivesRootFromCwdMap(t *testing.T) {
	r := &remoteWT{cloneExists: true, defaultBase: "origin/main"}
	a, s, f := worktreeFixture(t, r, "wk/feat", nil)
	a.Config.Hosts = map[string]map[string]any{
		"h1": {"cwd_map": map[string]any{a.Home + "/work/other": "/Users/remote/work/other"}},
		"h2": {},
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	add := r.cmds[indexOf(r.cmds, "worktree add")]
	if !strings.Contains(add, "'/Users/remote/work/repo-a.wk'") || !strings.Contains(add, "-C '/Users/remote/work/repo-a'") {
		t.Fatalf("worktree and clone must sit under the derived work root: %s", add)
	}
	if !strings.Contains(spawnInput(t, f), `"/Users/remote/work/repo-a.wk"`) {
		t.Fatalf("remote cwd must follow the derived root: %s", spawnInput(t, f))
	}
}

func TestRemoteWorktreeExplicitEntryStaysAuthoritative(t *testing.T) {
	r := &remoteWT{cloneExists: true, defaultBase: "origin/main"}
	a, s, f := worktreeFixture(t, r, "wk/feat", nil)
	a.Config.Hosts = map[string]map[string]any{
		"h1": {"cwd_map": map[string]any{
			a.Home + "/work/repo-a.wk": "/srv/elsewhere/repo-a.wk",
			a.Home + "/work/other":     "/Users/remote/work/other",
		}},
		"h2": {},
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(spawnInput(t, f), `"/srv/elsewhere/repo-a.wk"`) {
		t.Fatalf("the explicit entry decides the cwd: %s", spawnInput(t, f))
	}
	if add := r.cmds[indexOf(r.cmds, "worktree add")]; !strings.Contains(add, "'/srv/elsewhere/repo-a.wk'") || !strings.Contains(add, "-C '/srv/elsewhere/repo-a'") {
		t.Fatalf("a missing mapped path is prepared where the entry says: %s", add)
	}
}

func TestRemoteWorkRoot(t *testing.T) {
	local := "/Users/a/work"
	m := func(kv ...string) map[string]any {
		out := map[string]any{}
		for i := 0; i < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return map[string]any{"cwd_map": out}
	}
	tests := []struct {
		name string
		h    map[string]any
		want string
		err  bool
	}{
		{"no entries", map[string]any{}, "~/work", false},
		{"unrelated entry", m("/srv/x", "/srv/y/x"), "~/work", false},
		{"per-checkout entry", m(local+"/p", "/Users/b/work/p"), "/Users/b/work", false},
		{"root entry", m(local, "/Users/b/work"), "/Users/b/work", false},
		{"agreeing entries", m(local+"/p", "/Users/b/work/p", local+"/q.x", "/Users/b/work/q.x"), "/Users/b/work", false},
		{"disagreeing entries", m(local+"/p", "/Users/b/work/p", local+"/q", "/srv/q"), "", true},
		{"relative target ignored", m(local+"/p", "rel/p"), "~/work", false},
	}
	for _, tc := range tests {
		got, e := remoteWorkRoot(tc.h, local)
		if (e != nil) != tc.err || got != tc.want {
			t.Errorf("%s: got %q, %v", tc.name, got, e)
		}
	}
}

func TestRemotePathQuoting(t *testing.T) {
	for in, want := range map[string]string{
		"~":         `"$HOME"`,
		"~/work/a":  `"$HOME"'/work/a'`,
		"/srv/a b":  `'/srv/a b'`,
		"/srv/it's": `'/srv/it'"'"'s'`,
	} {
		if got := remotePath(in); got != want {
			t.Errorf("remotePath(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestLocalRepoCanonicalCheckoutIsNotAWorktree(t *testing.T) {
	// A canonical checkout whose toplevel string differs from the .git
	// parent (symlinked root, separate git dir) has git-dir == common-dir.
	a := testApp(t)
	f := &fakeRunner{Do: func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.Contains(args, "--show-toplevel"):
			return Result{Out: "/private/var/work/repo-a\n"}
		case strings.Contains(args, "--git-common-dir"):
			return Result{Out: "/var/work/repo-a/.git\n/var/work/repo-a/.git\n"}
		}
		return Result{}
	}}
	a.Runner = f
	repo := a.localRepo(context.Background(), "/private/var/work/repo-a")
	if repo == nil || repo.wt != "" || repo.name != "repo-a" {
		t.Fatalf("a canonical checkout must keep its own name: %+v", repo)
	}
}
