package fleet

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/spawnargs"
)

// hk 4959: the family a seat is handed out with (policy.json, and the brief's
// Mergewake-Author-Family trailer) must be a name the node accepts for
// --author-family, on the spawn path and on the consult path.

// Every seat the embedded policy hands out is accepted by the node's
// --author-family rule. The cognition seats (devin-*) were refused before hk 4959.
func TestHandedOutFamiliesAccepted_t4959(t *testing.T) {
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for name, seat := range p.Profiles {
		if got := spawnargs.Refusal([]string{"--author-family", seat.Family}, false); got != "" {
			t.Errorf("profile %s is handed out as family %q, which the node refuses: %s", name, seat.Family, got)
		}
	}
}

// A consult for a Devin author (family cognition), named by --author-family or
// read from the author's job record, is admitted by the hub's argument rule.
func TestConsultForCognitionAuthorAdmitted_t4959(t *testing.T) {
	for _, tc := range []struct {
		name   string
		viaJob bool
	}{
		{name: "author family flag"},
		{name: "author job record", viaJob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, q := consultAuthorFixture(t, "cognition")
			if tc.viaJob {
				a.JobStore = authorJobStore{rec: map[string]any{"task_id": "42", "family": "cognition", "profile": "devin-swe2"}}
			}
			rows := consultAuthorRows()
			q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
			useFakeHK(t).SetDoc("design/consult-q", "## Question\nWhy did the round-3 findings happen?")

			repo := &Repository{
				Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: t.TempDir(),
				Worktree: "author-checkout",
				Checkout: &HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/x", Sha: strings.Repeat("a", 40), Dir: "author-checkout"},
			}
			in := ConsultPlanInput{
				Task: "42", Question: "hk:doc/design/consult-q",
				PR: "example/project#7", Head: strings.Repeat("b", 40), Requester: "mergewake/example/project#7",
				Owner: "operator-desk", Repository: repo,
			}
			if tc.viaJob {
				in.AuthorJob = "author-job"
			} else {
				in.AuthorFamily = "cognition"
			}
			ctx := context.Background()
			plan, err := a.PlanConsult(ctx, in)
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			s, cleanup, err := a.hubSpawn(plan.Request)
			if err != nil {
				t.Fatalf("hub binds the planned consult: %v", err)
			}
			defer cleanup()
			// The args the hub sends for this consult must pass the node's rule
			// (spawnargs.Refusal), and must carry the cognition family they were planned for.
			args, err := hubSpawnArgs(a.Jobs, s)
			if err != nil {
				t.Fatalf("hub args for the planned consult: %v", err)
			}
			if !slices.Contains(args, "cognition") || !slices.Contains(args, "--author-family") {
				t.Fatalf("hub args carry no cognition author family: %v", args)
			}
			if reason := spawnargs.Refusal(args, true); reason != "" {
				t.Fatalf("the node refuses the planned consult for a cognition author: %s", reason)
			}
			if err := os.MkdirAll(s.CWD, 0700); err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if row.Profile == plan.Seat.Profile {
					q.LaunchValue = Launch{Profile: row.Profile, ModelID: row.ModelID, Effort: row.Effort, Grade: row.Grade, Gate: row.Gate, Pool: row.Pool}
				}
			}
			a.Config.Local = map[string]any{"workspace": "workspace"}
			s.DryRun = true
			a.planOnly, a.planFromHub = true, true
			if err := a.Spawn(ctx, s); err != nil {
				t.Fatalf("hub admission refused the consult for a cognition author: %v", err)
			}
			if a.lastPlan == nil || a.lastPlan.Role != "consult" {
				t.Fatalf("admitted plan = %+v, want a consult", a.lastPlan)
			}
			if fam := a.Policy.Profiles[a.lastPlan.Profile].Family; fam == "cognition" {
				t.Fatalf("consult seat %s is in the author's family cognition", a.lastPlan.Profile)
			}
		})
	}
}

// authorJobStore answers the hk record of the consult's author job.
type authorJobStore struct{ rec map[string]any }

func (s authorJobStore) Claim(context.Context, string, string, string) error { return nil }
func (s authorJobStore) Put(context.Context, string, map[string]any) error   { return nil }
func (s authorJobStore) Get(context.Context, string) (map[string]any, error) {
	return s.rec, nil
}
