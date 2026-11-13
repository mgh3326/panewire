package fleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubmissionCheckoutRoles(t *testing.T) {
	for _, tc := range []struct {
		name, role, branch string
		canonical, refused bool
		mode, fetch        string
	}{
		{"canonical reviewer", "reviewer", "main", true, false, "detached", "refs/heads/main"},
		{"canonical counsel", "consult", "main", true, false, "detached", "refs/heads/main"},
		{"canonical implementer", "implementer", "main", true, true, "", ""},
		{"linked implementer", "implementer", "wk/x", false, false, "branch", ""},
		{"linked reviewer", "reviewer", "wk/x", false, false, "detached", "refs/heads/wk/x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, f, hub, _ := hubFixture(t, gitState{pushed: true, pointsAt: []string{"refs/remotes/origin/" + tc.branch}}, tc.branch, hubHosts())
			a.Config.Local["machine_id"] = "source-a"
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			s.Prompt = filepath.Join(a.Home, "brief.md")
			if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment, s.Role = "job-a", "T1", "parent", "h1", "fixture", "hk:12", tc.role
			if tc.canonical {
				a.Repository = &Repository{Name: "repo-a", Origin: "https://git.invalid/org/repo-a.git", LocalRoot: s.CWD}
			}
			base := f.Do
			f.Do = func(c Command) Result {
				if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "symbolic-ref --short refs/remotes/origin/HEAD") {
					return Result{Out: "origin/main\n"}
				}
				return base(c)
			}
			if tc.refused {
				err := a.SubmitSpawn(t.Context(), s)
				if ErrorCode(err) != exitPlacement || !strings.Contains(err.Error(), "git worktree add -b wk/<slug>") || !strings.Contains(err.Error(), "-c") {
					t.Fatalf("expected worktree refusal, got %v", err)
				}
				if len(hub.spawns) != 0 {
					t.Fatal("hub called")
				}
				return
			}
			req, err := a.PrepareSubmission(t.Context(), s)
			if err != nil {
				t.Fatal(err)
			}
			got := req.Repository.Checkout
			if got.Mode != tc.mode || got.Fetch != tc.fetch || got.Sha != hubHead {
				t.Fatalf("checkout: %+v", got)
			}
			if tc.mode == "branch" && got.Branch != tc.branch {
				t.Fatalf("branch: %+v", got)
			}
		})
	}
}
