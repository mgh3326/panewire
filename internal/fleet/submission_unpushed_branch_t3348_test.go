package fleet

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// hk 3348: a writing role's branch must already be on origin. Without
// refs/remotes/origin/<branch> the node prepares a detached checkout at
// whatever origin ref holds HEAD, which is not the worktree the caller passed.
func TestSubmissionRequiresBranchOnOrigin(t *testing.T) {
	const origin = "https://git.invalid/org/repo-a.git"
	unpushed := gitState{pointsAt: []string{"refs/remotes/origin/main"}}
	for _, tc := range []struct {
		name    string
		state   gitState
		role    string
		refused bool
		want    HubCheckout
	}{
		{"implementer must push", unpushed, "implementer", true, HubCheckout{}},
		{"reviewer keeps the detached spec", unpushed, "reviewer", false,
			HubCheckout{Origin: origin, Mode: "detached", Sha: hubHead, Fetch: "refs/heads/main", Dir: "repo-a.wk"}},
		{"counsel keeps the detached spec", unpushed, "consult", false,
			HubCheckout{Origin: origin, Mode: "detached", Sha: hubHead, Fetch: "refs/heads/main", Dir: "repo-a.wk"}},
		{"pushed branch stays in branch mode", gitState{pushed: true}, "implementer", false,
			HubCheckout{Origin: origin, Mode: "branch", Branch: "wk/x", Sha: hubHead, Dir: "repo-a.wk"}},
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
				want := "branch wk/x is not on origin; push it first: git -C " + cwd + " push -u origin wk/x"
				request, err := a.PrepareSubmission(t.Context(), s)
				if err == nil {
					t.Fatalf("unpushed branch accepted: checkout %+v", request.Repository.Checkout)
				}
				if exitCode(err) != exitPlacement || err.Error() != want {
					t.Fatalf("err=%v, want %q", err, want)
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

// A branch that is on origin but whose local head runs ahead keeps today's
// behaviour: it does not earn the push refusal.
func TestSubmissionAheadBranchIsNotThePushRefusal(t *testing.T) {
	a, s, _, _, _ := hubFixture(t, gitState{onOrigin: true, contained: []string{"refs/remotes/origin/main"}}, "wk/x", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	s.Prompt = filepath.Join(a.Home, "brief.md")
	if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment, s.Role = "job-a", "T1", "parent", "h1", "fixture", "hk:12", "implementer"
	request, err := a.PrepareSubmission(t.Context(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Repository.Checkout; got == nil || got.Mode != "detached" || got.Fetch != "refs/heads/main" {
		t.Fatalf("ahead branch must keep the detached fallback: %+v", got)
	}
}
