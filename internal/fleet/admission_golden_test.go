package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// goldenVerdict is one gate verdict as testdata/admission_golden records it.
// Receipt is false when pacelever refused before deciding (an unusable stored
// reading, a mismatched account key): only its exit status and message exist.
type goldenVerdict struct {
	OK           bool     `json:"ok"`
	ExitCode     int      `json:"exit_code"`
	Reason       string   `json:"reason"`
	Pool         string   `json:"provider_id"`
	Basis        string   `json:"gate_basis"`
	Missing      []string `json:"missing_windows"`
	Alternatives []string `json:"alternatives"`
	Receipt      *bool    `json:"receipt"`
}

type goldenScenario struct {
	Name         string            `json:"name"`
	Now          time.Time         `json:"now"`
	Request      map[string]string `json:"request"`
	Config       string            `json:"config"`
	Latest       shared.Latest     `json:"latest"`
	Pacelever    goldenVerdict     `json:"pacelever"`
	Native       *goldenVerdict    `json:"native"`
	Disagreement string            `json:"disagreement"`
}

func loadGoldenScenarios(t *testing.T) ([]CatalogRow, []goldenScenario) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "admission_golden", "verdicts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Catalog   []CatalogRow     `json:"catalog"`
		Scenarios []goldenScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Scenarios) == 0 {
		t.Fatal("no golden scenarios")
	}
	return doc.Catalog, doc.Scenarios
}

// goldenGate runs panewire's own gate over one recorded scenario.
func goldenGate(t *testing.T, catalog []CatalogRow, s goldenScenario) (Result, goldenVerdict) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, []byte(s.Config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", config)
	t.Setenv("PACELEVER_E6_ARM", s.Request["e6_arm"])
	a := &App{Home: t.TempDir(), Now: func() time.Time { return s.Now }, Runner: &fakeRunner{}, Err: os.Stderr}
	a.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) {
		if key != s.Request["account_key"] {
			t.Errorf("%s: read account %s, want %s", s.Name, key, s.Request["account_key"])
		}
		return s.Latest, nil
	}
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return catalog, nil }
	r := s.Request
	x := a.nativeGate(context.Background(), QuotaRequest{Profile: r["profile"], Effort: r["effort"], Role: r["role"], Purpose: r["purpose"], OperatorRequest: r["operator_request"], RequestedBy: r["requested_by"], AccountKey: r["account_key"]}, true)
	var got goldenVerdict
	if err := json.Unmarshal([]byte(x.Out), &got); err != nil {
		t.Fatalf("%s: native receipt is not JSON: %v\n%s", s.Name, err, x.Out)
	}
	if got.ExitCode != x.Code {
		t.Fatalf("%s: receipt exit_code %d, gate exit %d", s.Name, got.ExitCode, x.Code)
	}
	return x, got
}

// sameVerdict compares the fields a verdict is judged on. A refusal pacelever
// made before deciding has only its exit status and message.
func sameVerdict(want, got goldenVerdict) bool {
	if want.OK != got.OK || want.ExitCode != got.ExitCode || want.Reason != got.Reason {
		return false
	}
	if want.Receipt != nil && !*want.Receipt {
		return true
	}
	norm := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		return s
	}
	return want.Pool == got.Pool && want.Basis == got.Basis && reflect.DeepEqual(norm(want.Missing), norm(got.Missing)) && reflect.DeepEqual(norm(want.Alternatives), norm(got.Alternatives))
}

// TestAdmissionGoldenVerdicts holds panewire's gate to pacelever's: each
// scenario's inputs were run through pacelever v0.3.28's own gate (the CLI's
// --account-key flow) and its verdict recorded. A scenario with a native
// verdict is a listed disagreement; its note says why panewire differs.
func TestAdmissionGoldenVerdicts(t *testing.T) {
	catalog, scenarios := loadGoldenScenarios(t)
	for _, s := range scenarios {
		t.Run(s.Name, func(t *testing.T) {
			_, got := goldenGate(t, catalog, s)
			want := s.Pacelever
			if s.Native != nil {
				if s.Disagreement == "" {
					t.Fatal("a native verdict needs its disagreement note")
				}
				if sameVerdict(s.Pacelever, *s.Native) {
					t.Fatal("listed disagreement no longer differs from pacelever; drop it")
				}
				want = *s.Native
			}
			if !sameVerdict(want, got) {
				t.Fatalf("verdict differs\nwant %+v\ngot  %+v", want, got)
			}
		})
	}
}

// TestAdmissionGoldenCoversEveryPoolAndRule keeps the golden set from
// shrinking below the rules the ticket names.
func TestAdmissionGoldenCoversEveryPoolAndRule(t *testing.T) {
	_, scenarios := loadGoldenScenarios(t)
	names := map[string]bool{}
	for _, s := range scenarios {
		names[s.Name] = true
	}
	for _, name := range []string{
		"claude-5h-exhausted", "claude-preserve-cutoff-90", "claude-stale-reading", "claude-malformed-scope",
		"codex-plus-missing-5h", "codex-free-missing-30d", "codex-prolite-7d-only-admits",
		"commandcode-30d-missing", "commandcode-30d-exhausted", "cmd-dsflash41-reviewer-denied", "cmd-dsflash41-builder-denied",
		"pi-reviewer-denied", "pi-implementer-manual-credit-admit", "devin-swe2-free-exhausted",
		"agy-group-window-gates-its-group", "grok-excluded-pool", "fusion-billing-fp-differs",
		"devin-ds41-paid-exhausted-alternatives", "claude-escalation-operator-override",
	} {
		if !names[name] {
			t.Errorf("golden scenario %s is missing", name)
		}
	}
}
