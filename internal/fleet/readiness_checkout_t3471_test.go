package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A linked-worktree checkout is cloned into the node's work root, so an
// unlisted repo is hostable there. Without a checkout there is nothing to
// clone and the cwd_map key must stay listed.
func TestReadinessRepoUnmappedCheckoutException(t *testing.T) {
	state := func(workRoot bool) ReadinessState {
		r := readyNode()
		r.Repos = []string{"other"}
		r.Config.WorkRoot = workRoot
		return ReadinessState{Readiness: r}
	}
	for _, tc := range []struct {
		name     string
		workRoot bool
		checkout bool
		want     string
	}{
		{"checkout and work root", true, true, ""},
		{"checkout without work root", false, true, ReadyRepoUnmapped},
		{"work root without checkout", true, false, ReadyRepoUnmapped},
		{"neither", false, false, ReadyRepoUnmapped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			need := ReadinessNeed{Kind: "claude", Repo: "panewire", Checkout: tc.checkout}
			if got := state(tc.workRoot).Drop(need, time.Now()); got != tc.want {
				t.Fatalf("drop=%q want %q", got, tc.want)
			}
		})
	}
}

// A checkout for a repo no node maps is admitted on a node with a work root,
// and dropped when no node has one.
func TestSpawnCheckoutIsAdmittedOnAWorkRootNode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workRoot bool
		checkout bool
		wantHost string
	}{
		{"checkout and work root", true, true, "mac-b"},
		{"checkout without work root", false, true, ""},
		{"no checkout", true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := ReadinessState{Readiness: readyNode()}
			state.Readiness.Repos = []string{"other"}
			state.Readiness.Config.WorkRoot = tc.workRoot
			a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": state, "cloud-small": state})
			if tc.checkout {
				a.Repository = &Repository{Name: "panewire", Worktree: "panewire.wk"}
			}
			sel, err := a.selectProfile(context.Background(), s)
			if tc.wantHost != "" {
				if err != nil || sel.chosen == nil || sel.chosen.host != tc.wantHost {
					t.Fatalf("checkout must be hostable on a work root: %v %v", sel, err)
				}
				return
			}
			if err == nil || sel.outcome != outcomeNoEligibleHost {
				t.Fatalf("unmapped repo without a work root must drop: %v %v", sel, err)
			}
			if !strings.Contains(err.Error(), ReadyRepoUnmapped) || !strings.Contains(err.Error(), "no node maps repo panewire") {
				t.Fatalf("refusal does not name repo_unmapped: %v", err)
			}
		})
	}
}

// The incident: every host candidate was dropped repo_unmapped while one pool
// held no presence. The refusal must name the host-side cause, not send the
// operator to sign in.
func TestRepoUnmappedRefusesNoEligibleHostNotAuth(t *testing.T) {
	a, s, q, _ := accountConsultFixture(t)
	a.Repository = &Repository{Name: "panewire"}
	bad := ReadinessState{Readiness: readyNode()}
	bad.Readiness.Repos = []string{"other"}
	a.State = readinessSnapshot(t, a, map[string]ReadinessState{"host-a": bad, "host-b": bad}, a.Now())
	q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"})
	q.PoolViews["devin"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(5)}
	s.Role, s.ParentFamily = "reviewer", "deepseek"

	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel == nil {
		t.Fatalf("expected a refusal with a record, got %v", err)
	}
	if sel.outcome != outcomeNoEligibleHost || exitCode(err) != exitPlacement {
		t.Fatalf("outcome=%s err=%v, want NO_ELIGIBLE_HOST/exitPlacement", sel.outcome, err)
	}
	if strings.Contains(err.Error(), outcomeAuthUnavailable) {
		t.Fatalf("host-side drops were refused as an account problem: %v", err)
	}
	if !strings.Contains(err.Error(), ReadyRepoUnmapped) || !strings.Contains(err.Error(), "no node maps repo panewire") {
		t.Fatalf("refusal does not name the dominant host-side drop: %v", err)
	}
	if fix := spawnRefusalFix(err); !strings.Contains(fix, "cwd_map") || !strings.Contains(fix, "work root") {
		t.Fatalf("fix does not match repo_unmapped: %q", fix)
	}
	for _, c := range sel.cands {
		if c.code == outcomeNoEligibleHost && c.reason != ReadyRepoUnmapped {
			t.Fatalf("%s was dropped for %s, want every host drop to be repo_unmapped", c.row.Profile, c.reason)
		}
	}
}
