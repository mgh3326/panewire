package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 4212: fable and codex-astra hold worker seats at grade SS on the default
// gate, like any other profile. These tests run selection through the
// production gate (admissionQuota), so the role, purpose and alias rules are
// the gate's own; only the catalog, the account reading and the pool views
// are fixtures.

// t4212Quota answers selection's gate and launch questions from the production
// gate; pools stay fake (selFixture's PoolViews).
type t4212Quota struct {
	*fakeQuota
	production admissionQuota
}

func (q t4212Quota) Gate(ctx context.Context, r QuotaRequest) (string, error) {
	return q.production.Gate(ctx, r)
}

func (q t4212Quota) Launch(ctx context.Context, r QuotaRequest) (Launch, error) {
	return q.production.Launch(ctx, r)
}

func t4212Rows() []CatalogRow {
	return []CatalogRow{
		{Profile: "fable", Effort: "high", Grade: "SS", Pool: "claude", Gate: "default", ModelID: "claude-fable-5-2"},
		{Profile: "codex-astra", Effort: "high", Grade: "SS", Pool: "codex", Gate: "default", ModelID: "gpt-6-astra"},
		{Profile: "opus", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "claude-opus-5-5"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
		{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "gpt-6-luna"},
	}
}

func t4212Pools() map[string]PoolView {
	return map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(0)},
	}
}

func t4212Selection(t *testing.T, grade string) (*App, Spawn) {
	t.Helper()
	isolateConfigHome(t)
	t.Setenv("PACELEVER_CONFIG", "")
	t.Setenv("PACELEVER_E6_ARM", "")
	rows := t4212Rows()
	a, s, q := selFixture(t, grade, rows, t4212Pools(), []string{"claude", "codex"})
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return rows, nil }
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		now := a.Now()
		reset5h, reset7d := now.Add(3*time.Hour), now.Add(5*24*time.Hour)
		return shared.Latest{Observation: &shared.Observation{Kind: shared.KindAuto, MeasuredAt: now.Add(-time.Minute), Buckets: []shared.Bucket{
			{Window: "5h", UsedPct: 20, ResetsAt: &reset5h, Scope: "account"},
			{Window: "7d", UsedPct: 40, ResetsAt: &reset7d, Scope: "account"},
		}}}, nil
	}
	a.Quota = t4212Quota{fakeQuota: q, production: admissionQuota{CLIQuota{Runner: a.Runner, Binary: "pacelever"}, a}}
	return a, s
}

// (a) an SS worker task: fable@high and codex-astra@high hold the SS seat, and
// the S+ opus row is below the task's grade.
func TestSSWorkerTaskChoosesFableOrAstraAtSS(t *testing.T) {
	a, s := t4212Selection(t, "SS")
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("SS worker selection refused: %v", err)
	}
	if sel.chosen.row.Grade != "SS" || (sel.chosen.row.Profile != "fable" && sel.chosen.row.Profile != "codex-astra") {
		t.Fatalf("chosen %s@%s grade %s, want fable or codex-astra at SS", sel.chosen.row.Profile, sel.chosen.row.Effort, sel.chosen.row.Grade)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:grade_insufficient" {
		t.Fatalf("S+ row outcome = %v, want dropped:grade_insufficient", got)
	}
}

// (b) an S+ worker task on the same catalog takes the S+ row, not the SS rungs.
func TestSPlusWorkerTaskChoosesSPlusRowNotFableOrAstra(t *testing.T) {
	a, s := t4212Selection(t, "S+")
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("S+ worker selection refused: %v", err)
	}
	if sel.chosen.row.Profile != "opus" || sel.chosen.row.Grade != "S+" {
		t.Fatalf("chosen %s@%s grade %s, want opus at S+", sel.chosen.row.Profile, sel.chosen.row.Effort, sel.chosen.row.Grade)
	}
}

// Below SS neither new row is chosen automatically while a lower admitted row
// of its pool exists: the ordinary lowest-fitting-row rule, no special case.
func TestBelowSSWorkerTaskSkipsFableAndAstraForLowerRow(t *testing.T) {
	a, s := t4212Selection(t, "A")
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("A worker selection refused: %v", err)
	}
	if sel.chosen.row.Profile == "fable" || sel.chosen.row.Profile == "codex-astra" {
		t.Fatalf("A task chose %s@%s; a lower admitted row exists", sel.chosen.row.Profile, sel.chosen.row.Effort)
	}
	if sel.chosen.row.Grade != "A" {
		t.Fatalf("A task chose grade %s, want A", sel.chosen.row.Grade)
	}
}

// (c) pins on an S+ task with containment are admitted: fable@high by name and
// codex-astra@high with no --purpose.
func TestSPlusPinOfFableAndAstraIsAdmittedWithContainment(t *testing.T) {
	for _, profile := range []string{"fable", "codex-astra"} {
		t.Run(profile, func(t *testing.T) {
			a, s := t4212Selection(t, "S+")
			s.Profile, s.Effort, s.Containment, s.OverrideReason = profile, "high", "hk:4212", "fixture"
			if err := a.admitPinnedProfile(context.Background(), &s); err != nil {
				t.Fatalf("%s@high -m pin on an S+ task refused: %v", profile, err)
			}
			got := a.nativeGate(context.Background(), QuotaRequest{Profile: profile, Effort: "high", Role: "implementer"}, true)
			if got.Code != 0 || !strings.Contains(got.Out, `"ok":true`) {
				t.Fatalf("%s@high worker refused by the gate: code=%d out=%s", profile, got.Code, got.Out)
			}
		})
	}
}

// (d) the removed astra alias is still refused by the production gate, and
// another profile is admitted by the same gate with the same request shape.
func TestRemovedAstraAliasRefusedAndOtherProfilesAdmitted(t *testing.T) {
	a, _ := t4212Selection(t, "A")
	rows := append(t4212Rows(), CatalogRow{Profile: "test-astra", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "gpt-6-astra"})
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return rows, nil }
	ctx := context.Background()

	alias := a.nativeGate(ctx, QuotaRequest{Profile: "test-astra", Effort: "high", Role: "implementer"}, true)
	if alias.Code == 0 || !strings.Contains(alias.Out, "role_restricted: removed astra seat alias") {
		t.Fatalf("removed astra alias admitted or wrong reason: code=%d out=%s", alias.Code, alias.Out)
	}
	for _, profile := range []string{"opus", "sonnet"} {
		got := a.nativeGate(ctx, QuotaRequest{Profile: profile, Effort: "high", Role: "implementer"}, true)
		if got.Code != 0 || !strings.Contains(got.Out, `"ok":true`) {
			t.Fatalf("%s worker refused by the gate: code=%d out=%s", profile, got.Code, got.Out)
		}
	}
}
