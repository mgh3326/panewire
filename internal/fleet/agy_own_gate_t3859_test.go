package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 3859: Gemini 3.7 (agy-flash) is retired. Every agy profile is judged by
// its own catalog row, so retiring the agy-flash row cannot refuse the 3.8
// rungs, and agy-sonnet46 / agy-opus46 meter against the 3p group.

var agyOwnGateNow = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

const agyOwnGateKey = "agy:0123456789abcdef"

var agyOwnGateProfiles = []string{"agy-flash38-high", "agy-flash38-medium", "agy-flash38-low", "agy-sonnet46", "agy-opus46", "agy-oss120"}

// agyOwnGateCatalog is the live catalog's agy rows as of 2026-11-10 with the
// agy-flash row retired, which is where panewire-design takes the hk catalog.
func agyOwnGateCatalog(withFlash37 bool) []CatalogRow {
	retired := agyOwnGateNow.Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	rows := []CatalogRow{
		{Profile: "agy-flash38-high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-high"},
		{Profile: "agy-flash38-medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-flash38-low", Grade: "C", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-low"},
		{Profile: "agy-sonnet46", Grade: "B", Pool: "agy", Gate: "default", ModelID: "claude-sonnet-4-6"},
		{Profile: "agy-opus46", Grade: gradeUnrated, Pool: "agy", Gate: "escalation", ModelID: "claude-opus-4-6-thinking"},
	}
	if withFlash37 {
		rows = append(rows, CatalogRow{Profile: "agy-flash", Grade: "A+", Pool: "agy", Gate: "default", ModelID: "gemini-3.7-flash", RetiredAt: &retired})
	}
	return rows
}

// agyOwnGateReading is a stored agy reading with one used-percent per group
// (5h and 7d windows, group-scoped buckets).
func agyOwnGateReading(gemini, thirdParty float64) shared.Latest {
	reset := agyOwnGateNow.Add(3 * time.Hour)
	bucket := func(window, group string, used float64) shared.Bucket {
		return shared.Bucket{Window: window, UsedPct: used, Scope: "group", Model: group, ResetsAt: &reset}
	}
	return shared.Latest{Observation: &shared.Observation{ID: 7, Kind: "auto", MeasuredAt: agyOwnGateNow.Add(-time.Minute), Buckets: []shared.Bucket{
		bucket("5h", "gemini", gemini), bucket("7d", "gemini", gemini), bucket("5h", "3p", thirdParty), bucket("7d", "3p", thirdParty),
	}}}
}

func agyOwnGateGate(t *testing.T, catalog []CatalogRow, latest shared.Latest, profile string) (Result, map[string]any) {
	t.Helper()
	return agyOwnGateGateKey(t, catalog, latest, profile, agyOwnGateKey)
}

func agyOwnGateGateKey(t *testing.T, catalog []CatalogRow, latest shared.Latest, profile, accountKey string) (Result, map[string]any) {
	t.Helper()
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", config)
	t.Setenv("PACELEVER_E6_ARM", "")
	a := &App{Home: t.TempDir(), Now: func() time.Time { return agyOwnGateNow }, Runner: &fakeRunner{}, Err: os.Stderr}
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) { return latest, nil }
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return catalog, nil }
	// A gate is asked by the profile's gate name, as spawn asks it.
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	name := profile
	if seat, ok := p.Profiles[profile]; ok {
		name = seat.gateName()
	}
	x := a.nativeGate(context.Background(), QuotaRequest{Profile: profile, GateProfile: name, Role: "implementer", AccountKey: accountKey}, true)
	var receipt map[string]any
	if err := json.Unmarshal([]byte(x.Out), &receipt); err != nil {
		t.Fatalf("%s: receipt is not JSON: %v\n%s", profile, err, x.Out)
	}
	return x, receipt
}

// (a) With the agy-flash row retired, the 3.8 rungs are admitted under their
// own rows. On the base they are gated as "agy-flash" and refused
// `profile "agy-flash" is unknown or retired`.
func TestAgyRungsAreAdmittedWithFlash37RowRetired(t *testing.T) {
	for _, withRow := range []bool{true, false} {
		for _, profile := range []string{"agy-flash38-high", "agy-flash38-medium", "agy-flash38-low", "agy-sonnet46"} {
			x, receipt := agyOwnGateGate(t, agyOwnGateCatalog(withRow), agyOwnGateReading(10, 10), profile)
			if x.Code != 0 || receipt["ok"] != true || receipt["profile"] != profile || receipt["provider_id"] != "agy" {
				t.Errorf("%s (retired agy-flash row present=%v): code %d receipt %v", profile, withRow, x.Code, receipt)
			}
		}
	}
}

// (b) A verdict reports the grade of the profile's own row, not agy-flash's A+.
func TestAgyVerdictReportsOwnGrade(t *testing.T) {
	for profile, want := range map[string]string{"agy-flash38-high": "A", "agy-flash38-medium": "B", "agy-flash38-low": "C", "agy-sonnet46": "B"} {
		_, receipt := agyOwnGateGate(t, agyOwnGateCatalog(true), agyOwnGateReading(10, 10), profile)
		if receipt["grade"] != want {
			t.Errorf("%s: grade %v, want %s (receipt %v)", profile, receipt["grade"], want, receipt)
		}
	}
	// agy-opus46 is its own unrated escalation row, not an ordinary A+ rung:
	// its verdict names that row rather than the 3.7 one.
	_, receipt := agyOwnGateGate(t, agyOwnGateCatalog(true), agyOwnGateReading(10, 10), "agy-opus46")
	if receipt["profile"] != "agy-opus46" || receipt["grade"] == "A+" {
		t.Errorf("agy-opus46 must be judged by its own row: %v", receipt)
	}
}

// (c) Pool, account key and metering group of the changed profiles. The pool
// and account key are what the base gave; the metering group is gemini for the
// 3.8 rungs and 3p for the Claude rungs (gemini for both Claude rungs on the base).
func TestAgyProfilesKeepPoolAndAccountAndMeterOwnGroup(t *testing.T) {
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]admissionRow{}
	for _, r := range admissionRows(agyOwnGateCatalog(false)) {
		rows[r.Profile] = r
	}
	wantGroup := map[string]string{"agy-flash38-high": "gemini", "agy-flash38-medium": "gemini", "agy-flash38-low": "gemini", "agy-sonnet46": "3p", "agy-opus46": "3p"}
	for profile, group := range wantGroup {
		seat := p.Profiles[profile]
		if seat.gateName() != profile || seat.Gate != "" {
			t.Errorf("%s: gate name %q (Gate %q), want its own canon", profile, seat.gateName(), seat.Gate)
		}
		if seat.Kind != "agy" || rows[profile].Pool != "agy" {
			t.Errorf("%s: kind %q pool %q, want agy/agy", profile, seat.Kind, rows[profile].Pool)
		}
		if rows[profile].Group != group {
			t.Errorf("%s: metering group %q, want %s", profile, rows[profile].Group, group)
		}
	}
	// The group is what the verdict meters: with the 3p group exhausted the
	// Claude rung is refused while the Gemini rung is admitted, and the reverse.
	for _, c := range []struct {
		gemini, thirdParty float64
		sonnetOK, flashOK  bool
	}{{10, 99.5, false, true}, {99.5, 10, true, false}} {
		for profile, want := range map[string]bool{"agy-sonnet46": c.sonnetOK, "agy-flash38-medium": c.flashOK} {
			x, receipt := agyOwnGateGate(t, agyOwnGateCatalog(true), agyOwnGateReading(c.gemini, c.thirdParty), profile)
			if (x.Code == 0) != want || receipt["provider_id"] != "agy" || receipt["account_key"] != agyOwnGateKey {
				t.Errorf("%s with gemini=%v 3p=%v: code %d receipt %v, want admitted=%v on pool agy for %s", profile, c.gemini, c.thirdParty, x.Code, receipt, want, agyOwnGateKey)
			}
		}
	}
}

// (d) agy-flash is retired: the spawn seat is unknown and the live catalog
// verdict refuses it as unknown.
func TestAgyFlash37IsUnknown(t *testing.T) {
	p, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"agy-flash", "agy-flash36"} {
		if _, ok := p.Profiles[name]; ok {
			t.Errorf("%s must not be a policy profile", name)
		}
	}
	if rows := overlayLaunchCatalog(agyOwnGateCatalog(true), false).profileRows("agy-flash"); len(rows) != 0 {
		t.Errorf("retired live catalog row remained launchable: %+v", rows)
	}
	a, s, _, _ := cmdFixture(t)
	s.Profile = "agy-flash"
	if e := a.Spawn(context.Background(), s); e == nil || !strings.Contains(e.Error(), "missing seat policy for agy-flash") {
		t.Fatalf("-m agy-flash must be refused as an unknown profile: %v", e)
	}
	x, receipt := agyOwnGateGateKey(t, agyOwnGateCatalog(true), agyOwnGateReading(10, 10), "agy-flash", "")
	if x.Code == 0 || !strings.Contains(receipt["reason"].(string), `profile "agy-flash" is unknown or retired`) {
		t.Fatalf("the gate must refuse agy-flash as unknown: code %d receipt %v", x.Code, receipt)
	}
}
