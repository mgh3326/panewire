package fleet

import (
	"context"
	"strings"
)

// ensureWorktree makes dir a worktree of the target's clone, on the branch the
// local worktree has checked out, so a job placed on a remote host runs the
// same branch without a hand-made cwd_map entry. An existing directory is
// reused when it is on that branch. Otherwise the branch is tracked from
// origin when it exists there (a PR head) or created from origin's default
// branch. Every failure is an error: the caller must not spawn elsewhere.
func (a *App) ensureWorktree(ctx context.Context, h map[string]any, host string, repo *repoResolution, dir string) error {
	if repo.branch == "" {
		return fail(exitPlacement, "cannot prepare a worktree on %s: %s has no branch checked out (detached HEAD)", host, repo.wt)
	}
	branch, wt := shellQuote(repo.branch), remotePath(dir)
	clone := remotePath(firstNonempty(repo.root, "~/work") + "/" + repo.name)
	git := "git -C " + clone + " "

	r := a.sshRun(ctx, h, host, "test -d "+wt)
	switch r.Code {
	case 0:
		o := a.sshRun(ctx, h, host, "git -C "+wt+" branch --show-current")
		if o.Code == exitUnreachable {
			return fail(exitUnreachable, "worktree check unreachable")
		}
		if got := strings.TrimSpace(o.Out); o.Code != 0 || got != repo.branch {
			return fail(exitPlacement, "worktree %s on %s exists but is not on branch %s (found %q)", dir, host, repo.branch, got)
		}
		return a.verifyHead(ctx, h, host, repo, dir)
	case exitUnreachable:
		return fail(exitUnreachable, "worktree check unreachable")
	case 1:
	default:
		return check(r, "worktree check on "+host)
	}

	if e := a.prepStep(ctx, h, host, git+"fetch origin", "fetch on "+host, "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4"); e != nil {
		return e
	}
	add := git + "worktree add "
	switch {
	case a.sshRun(ctx, h, host, git+"show-ref --verify --quiet refs/heads/"+branch).Code == 0:
		add += wt + " " + branch
	case a.sshRun(ctx, h, host, git+"rev-parse --verify --quiet "+shellQuote("refs/remotes/origin/"+repo.branch+"^{commit}")).Code == 0:
		add += "-b " + branch + " " + wt + " " + shellQuote("origin/"+repo.branch)
	default:
		o := a.sshRun(ctx, h, host, git+"symbolic-ref --short refs/remotes/origin/HEAD")
		if o.Code == exitUnreachable {
			return fail(exitUnreachable, "worktree base lookup unreachable")
		}
		base := strings.TrimSpace(o.Out)
		if o.Code != 0 || !strings.HasPrefix(base, "origin/") {
			return fail(exitPlacement, "cannot prepare worktree %s on %s: origin's default branch is unknown", dir, host)
		}
		add += "-b " + branch + " " + wt + " " + shellQuote(base)
	}
	if e := a.prepStep(ctx, h, host, add, "worktree add "+dir+" on "+host); e != nil {
		return e
	}

	return a.verifyHead(ctx, h, host, repo, dir)
}

// verifyHead refuses a target worktree that lacks the local HEAD: the job
// would run code without the local commits (unpushed, or a stale worktree).
func (a *App) verifyHead(ctx context.Context, h map[string]any, host string, repo *repoResolution, dir string) error {
	if repo.head == "" {
		return nil
	}
	r := a.sshRun(ctx, h, host, "git -C "+remotePath(dir)+" merge-base --is-ancestor "+shellQuote(repo.head)+" HEAD")
	if r.Code == exitUnreachable {
		return fail(exitUnreachable, "worktree verification unreachable")
	}
	if r.Code != 0 {
		return fail(exitPlacement, "worktree %s on %s does not contain local commit %s of %s; push the branch and update the remote worktree first", dir, host, short(repo.head), repo.branch)
	}
	return nil
}

// prepStep runs one preparation command on the target; a failure keeps ssh's
// 255 so placement can still tell an unreachable host apart.
func (a *App) prepStep(ctx context.Context, h map[string]any, host, script, what string, opts ...string) error {
	r := a.sshRun(ctx, h, host, script, opts...)
	if r.Code == exitUnreachable {
		return fail(exitUnreachable, "%s unreachable", what)
	}
	if r.Code != 0 {
		return fail(exitPlacement, "%s failed: %s", what, strings.TrimSpace(r.Err+" "+r.Out))
	}
	return nil
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
