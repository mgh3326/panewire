package fleet

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// poolCapRows is a free devin rung that ranks first and a ranked claude rung
// that takes the spawn when devin is full.
func poolCapRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A+", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// poolCapFixture builds a selection over poolCapRows with the devin cap set and
// the live count answered by live. calls counts reads of the count.
func poolCapFixture(t *testing.T, rows []CatalogRow, caps map[string]any, live func() (map[string]int, error)) (*App, Spawn, *int) {
	t.Helper()
	pools := map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "devin-swe2": "devin"})
	if caps != nil {
		a.Config.Quota = map[string]any{"pool_max_concurrent": caps}
	}
	calls := new(int)
	a.LiveSpawnProfiles = func(context.Context, string) (map[string]int, error) {
		*calls++
		return live()
	}
	return a, s, calls
}

func liveProfiles(counts map[string]int) func() (map[string]int, error) {
	return func() (map[string]int, error) { return counts, nil }
}

func chosenSeat(t *testing.T, a *App, s Spawn) (*selPlan, string) {
	t.Helper()
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("selection refused: %v", err)
	}
	return sel, sel.chosen.seatName
}

// (a) Two live devin jobs fill the cap: devin is dropped pool_at_cap and the
// next pool is chosen.
func TestPoolCapDropsDevinAtCapAndChoosesNextPool(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"devin": int64(2)}, liveProfiles(map[string]int{"devin-swe2": 2}))
	sel, seat := chosenSeat(t, a, s)
	if seat != "sonnet" {
		t.Fatalf("chosen = %s, want sonnet (devin is at its cap)", seat)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:pool_at_cap" {
		t.Fatalf("devin outcome = %v, want dropped:pool_at_cap", got)
	}
}

// (b) One live devin job leaves a slot: devin is admitted.
func TestPoolCapAdmitsBelowCap(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"devin": int64(2)}, liveProfiles(map[string]int{"devin-swe2": 1}))
	if _, seat := chosenSeat(t, a, s); seat != "devin-swe2" {
		t.Fatalf("chosen = %s, want devin-swe2", seat)
	}
}

// (c) The count is of live jobs only. The hub leaves a job its nodes no longer
// list (completed, lost) out of the profiles it returns, so a count of zero is
// what a fleet whose devin jobs have all ended reports. The hub's side of this
// is TestLiveSpawnProfilesSkipsEndedJobs.
func TestPoolCapIgnoresEndedJobs(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"devin": int64(2)}, liveProfiles(map[string]int{}))
	if _, seat := chosenSeat(t, a, s); seat != "devin-swe2" {
		t.Fatalf("chosen = %s, want devin-swe2", seat)
	}
}

// (d) Without [quota].pool_max_concurrent nothing is capped, and the count is
// never read.
func TestPoolCapAbsentConfigMeansNoCap(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), nil, liveProfiles(map[string]int{"devin-swe2": 50}))
	if _, seat := chosenSeat(t, a, s); seat != "devin-swe2" {
		t.Fatalf("chosen = %s, want devin-swe2", seat)
	}
	if *calls != 0 {
		t.Fatalf("count read %d times with no cap configured", *calls)
	}
}

// A pool the table does not name is uncapped, and the count is not read for it.
func TestPoolCapOnlyCapsNamedPools(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), map[string]any{"codex": int64(1)}, liveProfiles(map[string]int{"devin-swe2": 50}))
	if _, seat := chosenSeat(t, a, s); seat != "devin-swe2" {
		t.Fatalf("chosen = %s, want devin-swe2", seat)
	}
	if *calls != 0 {
		t.Fatalf("count read %d times though no candidate's pool is capped", *calls)
	}
}

// (e) A count that cannot be read lifts the cap, says so in the selection
// record, and logs the reason.
func TestPoolCapUnavailableCountMeansNoCap(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), map[string]any{"devin": int64(2)}, func() (map[string]int, error) {
		return nil, errors.New("hub down")
	})
	sel, seat := chosenSeat(t, a, s)
	if seat != "devin-swe2" {
		t.Fatalf("chosen = %s, want devin-swe2 (fail-open)", seat)
	}
	if *calls != 1 {
		t.Fatalf("count read %d times, want once per selection", *calls)
	}
	if !slices.Contains(sel.degraded, "pool_cap_count_unavailable") {
		t.Fatalf("selection record does not say the count was unavailable: %v", sel.degraded)
	}
	if logged := a.Err.(*bytes.Buffer).String(); !strings.Contains(logged, "pool_cap_count_unavailable") || !strings.Contains(logged, "hub down") {
		t.Fatalf("unavailable count not logged with its cause: %q", logged)
	}
}

// A seat alias counts toward the pool of the catalog profile it runs.
func TestPoolCapCountsSeatAliasesByPool(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"devin": int64(2)}, liveProfiles(map[string]int{"devin-swe2": 1, "builder-devin": 1}))
	if _, seat := chosenSeat(t, a, s); seat != "sonnet" {
		t.Fatalf("chosen = %s, want sonnet: a live devin-swe2 and a live builder-devin (its alias) fill a cap of 2", seat)
	}
}

// When devin is the only candidate and is full, the refusal names pool_at_cap.
func TestPoolCapRefusalSaysSo(t *testing.T) {
	rows := poolCapRows()[:1]
	a, s, _ := poolCapFixture(t, rows, map[string]any{"devin": int64(2)}, liveProfiles(map[string]int{"devin-swe2": 2}))
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil {
		t.Fatal("selection admitted a spawn on a full pool")
	}
	for _, want := range []string{"pool_at_cap", "devin=2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:pool_at_cap" {
		t.Fatalf("devin outcome = %v", got)
	}
}

// (f) An invalid pool_max_concurrent is refused at load, naming the key.
func TestPoolCapInvalidConfigRefusedAtLoad(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"valid", "pool_max_concurrent = { devin = 2 }", ""},
		{"not_a_table", "pool_max_concurrent = 2", "quota.pool_max_concurrent must map pool names to positive integers"},
		{"string_value", `pool_max_concurrent = { devin = "2" }`, "quota.pool_max_concurrent.devin must be a positive integer"},
		{"float_value", "pool_max_concurrent = { devin = 1.5 }", "quota.pool_max_concurrent.devin must be a positive integer"},
		{"zero", "pool_max_concurrent = { devin = 0 }", "quota.pool_max_concurrent.devin must be a positive integer"},
		{"negative", "pool_max_concurrent = { devin = -1 }", "quota.pool_max_concurrent.devin must be a positive integer"},
		{"invalid_pool_name", `pool_max_concurrent = { "bad pool" = 2 }`, "quota.pool_max_concurrent.bad pool must be a positive integer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hosts.toml")
			if err := os.WriteFile(path, []byte("[quota]\n"+tc.body+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if got := poolMaxConcurrent(cfg.Quota); got["devin"] != 2 {
					t.Fatalf("caps = %v, want devin=2", got)
				}
				return
			}
			if exitCode(err) != exitConfig || err == nil || err.Error() != tc.want {
				t.Fatalf("want exit %d %q, got %v", exitConfig, tc.want, err)
			}
		})
	}
}
