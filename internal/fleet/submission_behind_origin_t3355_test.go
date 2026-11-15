package fleet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// hk 3355: a writing role whose HEAD is a strict ancestor of
// refs/remotes/origin/<branch> is refused at submit. The node would find the
// origin tip differs from the sha and answer branch-elsewhere.
func TestSubmissionRefusesWorktreeBehindOrigin(t *testing.T) {
	const origin = "https://git.invalid/org/repo-a.git"
	const newer = "fedcba9876543210fedcba9876543210fedcba98"
	detachedMain := HubCheckout{Origin: origin, Mode: "detached", Sha: hubHead, Fetch: "refs/heads/main", Dir: "repo-a.wk"}
	for _, tc := range []struct {
		name    string
		state   gitState
		role    string
		refused bool
		want    HubCheckout
	}{
		{"behind implementer", gitState{pushed: true, originTip: newer}, "implementer", true, HubCheckout{}},
		{"equal keeps the branch spec", gitState{pushed: true}, "implementer", false,
			HubCheckout{Origin: origin, Mode: "branch", Branch: "wk/x", Sha: hubHead, Dir: "repo-a.wk"}},
		{"ahead and pushed elsewhere keeps today's behaviour", gitState{onOrigin: true, originTip: newer, contained: []string{"refs/remotes/origin/main"}}, "implementer", false, detachedMain},
		{"behind reviewer is unchanged", gitState{pushed: true, originTip: newer, pointsAt: []string{"refs/remotes/origin/main"}}, "reviewer", false, detachedMain},
		{"behind counsel is unchanged", gitState{pushed: true, originTip: newer, pointsAt: []string{"refs/remotes/origin/main"}}, "consult", false, detachedMain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, hub, _ := hubFixture(t, tc.state, "wk/x", hubHosts())
			a.Config.Local["machine_id"] = "source-a"
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			s.Prompt = filepath.Join(a.Home, "brief.md")
			if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment = "job-a", "T1", "parent", "h1", "fixture", "hk:12"
			s.Role = tc.role
			if tc.refused {
				cwd, err := filepath.EvalSymlinks(s.CWD)
				if err != nil {
					t.Fatal(err)
				}
				want := "worktree is behind origin/wk/x; run: git -C " + cwd + " fetch && git -C " + cwd + " merge --ff-only origin/wk/x"
				request, err := a.PrepareSubmission(t.Context(), s)
				if err == nil {
					t.Fatalf("behind worktree accepted: checkout %+v", request.Repository.Checkout)
				}
				if exitCode(err) != exitPlacement || err.Error() != want {
					t.Fatalf("err=%v (exit %d), want %q (exit %d)", err, exitCode(err), want, exitPlacement)
				}
				if err := a.SubmitSpawn(t.Context(), s); err == nil || err.Error() != want {
					t.Fatalf("submit err=%v, want %q", err, want)
				}
				if len(hub.spawns) != 0 {
					t.Fatalf("a refused submission reached the hub: %+v", hub.spawns)
				}
				if _, err := os.Stat(filepath.Join(a.Jobs, "job-a", "submission.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("a refused submission was written: %v", err)
				}
				return
			}
			request, err := a.PrepareSubmission(t.Context(), s)
			if err != nil {
				t.Fatal(err)
			}
			if got := request.Repository.Checkout; got == nil || *got != tc.want {
				t.Fatalf("checkout: %+v, want %+v", got, tc.want)
			}
		})
	}
}
