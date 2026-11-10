package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// ompSeedCatalog reads the omp seed rows, the catalog the desk publishes for
// the omp seats.
func ompSeedCatalog(t *testing.T) []CatalogRow {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "catalog", "omp-credit-seed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var seed struct {
		Catalog []CatalogRow `json:"catalog"`
	}
	if err := json.Unmarshal(b, &seed); err != nil {
		t.Fatal(err)
	}
	return seed.Catalog
}

// ompSeedRowsFor returns one profile's seed rows, keyed by effort.
func ompSeedRowsFor(t *testing.T, profile string) map[string]CatalogRow {
	t.Helper()
	rows := map[string]CatalogRow{}
	for _, r := range ompSeedCatalog(t) {
		if r.Profile == profile {
			rows[r.Effort] = r
		}
	}
	return rows
}

// (a) omp-haiku and omp-luna are seats shaped like omp-sonnet and omp-terra61.
// Their model and pool come from the seed, and their launch argv is the same
// `--model <model>:<effort>` with the setup skip that the existing seats get.
func TestOMPSeatsResolveToTheirModelAndPool(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct {
		profile, model, pool, family, like, likeModel string
	}{
		{"omp-haiku", "anthropic/claude-haiku-5-5", "anthropic-api", "anthropic", "omp-sonnet", "anthropic/claude-sonnet-5-5"},
		{"omp-luna", "openai/gpt-6-luna", "openai-api", "openai", "omp-terra61", "openai/gpt-6.1-terra"},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			seat, ok := a.Policy.Profiles[tc.profile]
			if !ok {
				t.Fatalf("policy has no seat %s", tc.profile)
			}
			like := a.Policy.Profiles[tc.like]
			if seat.Kind != "omp" || seat.Family != tc.family || seat.Capability != like.Capability || seat.Effort != like.Effort || !slices.Equal(seat.Roles, like.Roles) {
				t.Fatalf("seat %+v is not shaped like %s %+v", seat, tc.like, like)
			}
			rows := ompSeedRowsFor(t, tc.profile)
			if len(rows) != 5 {
				t.Fatalf("seed has %d rungs for %s, want 5", len(rows), tc.profile)
			}
			for effort, r := range rows {
				if r.ModelID != tc.model || r.Pool != tc.pool {
					t.Fatalf("%s@%s seeds %s on %s, want %s on %s", tc.profile, effort, r.ModelID, r.Pool, tc.model, tc.pool)
				}
			}
			argv, env, err := a.launchArgs(seat, Launch{ModelID: tc.model, Effort: "high"})
			if err != nil {
				t.Fatal(err)
			}
			likeArgv, likeEnv, err := a.launchArgs(like, Launch{ModelID: tc.likeModel, Effort: "high"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(argv, []string{"--model", tc.model + ":high"}) || !reflect.DeepEqual(likeArgv, []string{"--model", tc.likeModel + ":high"}) {
				t.Fatalf("argv %v, %s launches %v", argv, tc.like, likeArgv)
			}
			if !reflect.DeepEqual(env, likeEnv) || !reflect.DeepEqual(env, []string{"OMP_SKIP_SETUP=1"}) {
				t.Fatalf("env %v, %s has %v", env, tc.like, likeEnv)
			}
		})
	}
}

// (c) Under a catalog where the new rungs are unrated, a graded -m auto worker
// spawn never picks them.
func TestOMPSeatsUnratedAreNotAutoPicked(t *testing.T) {
	var rows []CatalogRow
	for _, r := range ompSeedCatalog(t) {
		if r.Profile == "omp-haiku" || r.Profile == "omp-luna" {
			rows = append(rows, r)
		}
	}
	if len(rows) != 10 {
		t.Fatalf("seed has %d unrated rungs, want 10", len(rows))
	}
	rows = append(rows, ompRow("sonnet", "claude-sonnet-5-5", "claude"))
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		"anthropic-api": {Class: "credit"},
		"openai-api":    {Class: "credit"},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "anthropic-api", "openai-api"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "omp-haiku": "anthropic-api", "omp-luna": "openai-api"})
	serveOMPRouting(t, a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if chosen := sel.record()["chosen"].(map[string]any); chosen["canon"] != "sonnet" {
		t.Fatalf("auto spawn chose %v, want sonnet", chosen)
	}
	for _, p := range []string{"omp-haiku", "omp-luna"} {
		for effort := range ompSeedRowsFor(t, p) {
			if got := candOutcome(sel.record(), p, effort)["reason"]; got != "grade_unrated" {
				t.Fatalf("%s@%s reason %v, want grade_unrated", p, effort, got)
			}
		}
	}
}

// ompSeatRung is one new seat as a pinned spawn sees it: the model omp lists
// for it, and the footer name the screen shows once it runs that model.
type ompSeatRung struct {
	profile, model, pool, selector, footerName string
}

var ompSeatRungs = []ompSeatRung{
	{"omp-haiku", "anthropic/claude-haiku-5-5", "anthropic-api", "anthropic/claude-haiku-5-5", "Haiku 5.5"},
	{"omp-luna", "openai/gpt-6-luna", "openai-api", "openai/gpt-6-luna", "GPT-6 Luna"},
}

// pinnedOMPSeat prepares a pinned graded spawn of one new seat. omp lists the
// seat's model and the footer names it, so the launch proves on the screen.
func pinnedOMPSeat(t *testing.T, r ompSeatRung) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, fr := ompSpawnFixture(t)
	s.Profile = r.profile
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{{Profile: r.profile, Effort: "high", Grade: "unrated", ModelID: r.model, Pool: r.pool, Gate: "default"}}
	q.LaunchValue = Launch{Profile: r.profile, ModelID: r.model, Effort: "high", Grade: "unrated", Pool: r.pool, Gate: "default"}
	q.GateDo = gatePools(map[string]string{r.profile: r.pool})
	base := fr.Do
	fr.Do = func(c Command) Result {
		if c.Name == "omp" && strings.Join(c.Args, " ") == "models --json" {
			name := strings.TrimPrefix(r.footerName, "Claude ")
			return Result{Out: fmt.Sprintf(`{"models":[{"id":%q,"selector":%q,"name":%q}]}`, r.model, r.selector, name)}
		}
		return base(c)
	}
	ompSeat(fr, "", func(int) string {
		return strings.ReplaceAll(ompCapture(t, "screen_opus_high_worktree.txt"), "Opus 5.5", r.footerName)
	})
	return a, s, fr
}

// (c, pinned) A pinned spawn with --containment reaches the new seat's launch
// once the operator arms its unrated rung for the graded task (E6 arm).
func TestOMPSeatsPinnedContainmentResolves(t *testing.T) {
	for _, r := range ompSeatRungs {
		t.Run(r.profile, func(t *testing.T) {
			a, s, fr := pinnedOMPSeat(t, r)
			s.E6Arm = r.profile + "@high"
			if err := a.Spawn(context.Background(), s); err != nil {
				t.Fatalf("armed pinned spawn refused: %v", err)
			}
			if n := ompPrompts(fr); n != 1 {
				t.Fatalf("brief sent %d times, want 1", n)
			}
		})
	}
}

// Without the arm an unrated rung does not cover a graded task: the spawn is
// refused before any agent starts.
func TestOMPSeatsUnarmedPinnedRefused(t *testing.T) {
	for _, r := range ompSeatRungs {
		t.Run(r.profile, func(t *testing.T) {
			a, s, fr := pinnedOMPSeat(t, r)
			err := a.Spawn(context.Background(), s)
			if err == nil || !strings.Contains(err.Error(), "PANEWIRE_E6_ARM="+r.profile+"@high") {
				t.Fatalf("unarmed unrated rung covered a graded task: %v", err)
			}
			for _, c := range fr.Calls {
				if strings.HasPrefix(strings.Join(c.Args, " "), "agent start") {
					t.Fatal("unarmed unrated rung reached launch")
				}
			}
		})
	}
}

// (d) The existing omp seats keep their policy, launch argv, pool and model.
// These are preservation pins: the values are the ones shipped before this change.
func TestOMPExistingSeatsUnchanged(t *testing.T) {
	a := testApp(t)
	for _, tc := range []struct {
		profile, model, pool, family string
	}{
		{"omp-opus", "anthropic/claude-opus-5-5", "anthropic-api", "anthropic"},
		{"omp-sonnet", "anthropic/claude-sonnet-5-5", "anthropic-api", "anthropic"},
		{"omp-sol", "openai/gpt-6.1-sol", "openai-api", "openai"},
		{"omp-terra61", "openai/gpt-6.1-terra", "openai-api", "openai"},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			seat := a.Policy.Profiles[tc.profile]
			if seat.Kind != "omp" || seat.Family != tc.family || seat.Capability != "standard" || seat.Effort != "high" {
				t.Fatalf("seat changed: %+v", seat)
			}
			argv, env, err := a.launchArgs(seat, Launch{ModelID: tc.model, Effort: "high"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(argv, []string{"--model", tc.model + ":high"}) || !reflect.DeepEqual(env, []string{"OMP_SKIP_SETUP=1"}) {
				t.Fatalf("argv %v env %v", argv, env)
			}
			for effort, r := range ompSeedRowsFor(t, tc.profile) {
				if r.ModelID != tc.model || r.Pool != tc.pool {
					t.Fatalf("%s@%s seed %s on %s, want %s on %s", tc.profile, effort, r.ModelID, r.Pool, tc.model, tc.pool)
				}
			}
		})
	}
}
