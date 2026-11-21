package fleet

import (
	"context"
	"strings"
)

// HandoverRefPrefix is the namespace of the snapshot refs a hand-over pushes:
// refs/panewire/handover/<job>/<commit> (hk 5259, hk 5324; docs/node-commands.md).
const HandoverRefPrefix = "refs/panewire/handover/"

// The reasons on a hand-over ref line of the sweep log (hk 5870). A release
// logs handoverRefReleased; every other reason names what kept the ref.
const (
	handoverRefReleased     = "released"
	handoverRefBranchKept   = "branch_unproven_kept"
	handoverRefListFailed   = "list_failed_kept"
	handoverRefFetchFailed  = "fetch_failed_kept"
	handoverRefNotContained = "not_contained_kept"
	handoverRefUnproven     = "containment_unproven_kept"
	handoverRefDeleteFailed = "delete_failed_kept"
)

// releaseHandoverRefs runs when a successor's job ends (hk 5870): it completed,
// or its PR merged. cwd is the successor's worktree, still on disk, so the
// clone is reachable from it; the release runs before the worktree is removed.
// A job that ended any other way keeps its refs.
func (s *TaskPaneSweeper) releaseHandoverRefs(ctx context.Context, a *App, cwd string, end paneSweepEnd) {
	if end.state != "completed" && !end.merged {
		return
	}
	clone, branch, ok := s.nodeWorktree(ctx, a, cwd)
	if !ok || branch == "" {
		return
	}
	releaseHandoverRefsOf(ctx, a, clone, branch, s.info)
}

// releaseHandoverRefsOf deletes each hand-over ref on origin whose snapshot
// commit the branch's origin tip contains. The branch must be on origin, and a
// ref is deleted only when its commit is an ancestor of that tip: then the
// work the snapshot carries is on origin under the branch, and nothing the
// branch does not contain is touched. Every other ref is kept, with a log line
// naming why. A delete is conditional on the commit that was proven
// (--force-with-lease), so a ref moved since the check survives.
func releaseHandoverRefsOf(ctx context.Context, a *App, clone, branch string, info func(string, ...any)) {
	if info == nil {
		info = func(string, ...any) {}
	}
	tip := "refs/remotes/origin/" + branch
	if r := a.call(ctx, "git", "-C", clone, "fetch", "--quiet", "--no-tags", "origin", "+refs/heads/"+branch+":"+tip); r.Code != 0 {
		info("hand-over refs kept", "branch", branch, "reason", handoverRefBranchKept)
		return
	}
	list := a.call(ctx, "git", "-C", clone, "ls-remote", "origin", HandoverRefPrefix+"*")
	if list.Code != 0 {
		info("hand-over refs kept", "branch", branch, "reason", handoverRefListFailed)
		return
	}
	for _, line := range strings.Split(list.Out, "\n") {
		sha, ref, found := strings.Cut(strings.TrimSpace(line), "\t")
		if !found || !strings.HasPrefix(ref, HandoverRefPrefix) {
			continue
		}
		if r := a.call(ctx, "git", "-C", clone, "fetch", "--quiet", "--no-tags", "origin", ref); r.Code != 0 {
			info("hand-over ref kept", "ref", ref, "branch", branch, "reason", handoverRefFetchFailed)
			continue
		}
		switch contained := a.call(ctx, "git", "-C", clone, "merge-base", "--is-ancestor", sha, tip); contained.Code {
		case 0:
		case 1:
			info("hand-over ref kept", "ref", ref, "branch", branch, "reason", handoverRefNotContained)
			continue
		default:
			info("hand-over ref kept", "ref", ref, "branch", branch, "reason", handoverRefUnproven)
			continue
		}
		if r := a.call(ctx, "git", "-C", clone, "push", "--quiet", "--no-verify", "--force-with-lease="+ref+":"+sha, "origin", ":"+ref); r.Code != 0 {
			info("hand-over ref kept", "ref", ref, "branch", branch, "reason", handoverRefDeleteFailed)
			continue
		}
		info("hand-over ref released", "ref", ref, "branch", branch, "reason", handoverRefReleased)
	}
}
