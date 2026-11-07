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

// poolCapPolicy keeps openai-api out of the credit tier so the test selection
// ranks it first on its own merits; the credit two-pass case has its own
// policy below. The cap itself is tier-independent.
const poolCapPolicy = `{"version": 2, "source_doc": "policy/quota-stop", "order": ["waste", "free", "ranked", "credit"], "free": [], "credit_pools": []}`

// poolCapCreditPolicy is the shipped ordering for the two credit pools.
const poolCapCreditPolicy = `{"version": 2, "source_doc": "policy/quota-stop", "order": ["waste", "free", "ranked", "credit"], "free": [], "credit_pools": ["anthropic-api", "openai-api"]}`

// poolCapRows is an openai-api rung that ranks first and a ranked claude rung
// that takes the spawn when openai-api is full.
func poolCapRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "omp-sol", Effort: "high", Grade: "A", Pool: "openai-api", Gate: "default", ModelID: "gpt-6.1-sol"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

func servePoolCapPolicy(t *testing.T, a *App, policy string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = path
}

// poolCapFixture builds a selection over poolCapRows with the openai-api cap set
// and the live count answered by live. calls counts reads of the count.
func poolCapFixture(t *testing.T, rows []CatalogRow, caps map[string]any, live func() (map[string]int, error)) (*App, Spawn, *int) {
	t.Helper()
	pools := map[string]PoolView{
		"openai-api": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
		"claude":     {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"openai-api", "claude"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "omp-sol": "openai-api"})
	servePoolCapPolicy(t, a, poolCapPolicy)
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

// (a) Two live openai-api jobs fill the cap: openai-api is dropped pool_at_cap
// and the next pool is chosen.
func TestPoolCapDropsOpenAIAtCapAndChoosesNextPool(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"openai-api": int64(2)}, liveProfiles(map[string]int{"omp-sol": 2}))
	sel, seat := chosenSeat(t, a, s)
	if seat != "sonnet" {
		t.Fatalf("chosen = %s, want sonnet (openai-api is at its cap)", seat)
	}
	if got := candOutcome(sel.record(), "omp-sol", "high")["outcome"]; got != "dropped:pool_at_cap" {
		t.Fatalf("openai-api outcome = %v, want dropped:pool_at_cap", got)
	}
}

// (b) One live openai-api job leaves a slot: openai-api is admitted.
func TestPoolCapAdmitsBelowCap(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"openai-api": int64(2)}, liveProfiles(map[string]int{"omp-sol": 1}))
	if _, seat := chosenSeat(t, a, s); seat != "omp-sol" {
		t.Fatalf("chosen = %s, want omp-sol", seat)
	}
}

// (c) The count is of live jobs only. The hub leaves a job its nodes no longer
// list (completed, lost) out of the profiles it returns, so a count of zero is
// what a fleet whose openai-api jobs have all ended reports. The hub's side of
// this is TestLiveSpawnProfilesSkipsEndedJobs.
func TestPoolCapIgnoresEndedJobs(t *testing.T) {
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"openai-api": int64(2)}, liveProfiles(map[string]int{}))
	if _, seat := chosenSeat(t, a, s); seat != "omp-sol" {
		t.Fatalf("chosen = %s, want omp-sol", seat)
	}
}

// (d) Without [quota].pool_max_concurrent nothing is capped, and the count is
// never read.
func TestPoolCapAbsentConfigMeansNoCap(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), nil, liveProfiles(map[string]int{"omp-sol": 50}))
	if _, seat := chosenSeat(t, a, s); seat != "omp-sol" {
		t.Fatalf("chosen = %s, want omp-sol", seat)
	}
	if *calls != 0 {
		t.Fatalf("count read %d times with no cap configured", *calls)
	}
}

// A pool the table does not name is uncapped, and the count is not read for it.
func TestPoolCapOnlyCapsNamedPools(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), map[string]any{"codex": int64(1)}, liveProfiles(map[string]int{"omp-sol": 50}))
	if _, seat := chosenSeat(t, a, s); seat != "omp-sol" {
		t.Fatalf("chosen = %s, want omp-sol", seat)
	}
	if *calls != 0 {
		t.Fatalf("count read %d times though no candidate's pool is capped", *calls)
	}
}

// (e) A count that cannot be read lifts the cap, says so in the selection
// record, and logs the reason.
func TestPoolCapUnavailableCountMeansNoCap(t *testing.T) {
	a, s, calls := poolCapFixture(t, poolCapRows(), map[string]any{"openai-api": int64(2)}, func() (map[string]int, error) {
		return nil, errors.New("hub down")
	})
	sel, seat := chosenSeat(t, a, s)
	if seat != "omp-sol" {
		t.Fatalf("chosen = %s, want omp-sol (fail-open)", seat)
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
	a, s, _ := poolCapFixture(t, poolCapRows(), map[string]any{"openai-api": int64(2)}, liveProfiles(map[string]int{"omp-sol": 1, "builder-sol": 1}))
	a.Policy.Profiles["builder-sol"] = Seat{Canon: "omp-sol"}
	if _, seat := chosenSeat(t, a, s); seat != "sonnet" {
		t.Fatalf("chosen = %s, want sonnet: a live omp-sol and a live builder-sol (its alias) fill a cap of 2", seat)
	}
}

// When openai-api is the only candidate and is full, the refusal names
// pool_at_cap.
func TestPoolCapRefusalSaysSo(t *testing.T) {
	rows := poolCapRows()[:1]
	a, s, _ := poolCapFixture(t, rows, map[string]any{"openai-api": int64(2)}, liveProfiles(map[string]int{"omp-sol": 2}))
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil {
		t.Fatal("selection admitted a spawn on a full pool")
	}
	for _, want := range []string{"pool_at_cap", "openai-api=2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err, want)
		}
	}
	if got := candOutcome(sel.record(), "omp-sol", "high")["outcome"]; got != "dropped:pool_at_cap" {
		t.Fatalf("openai-api outcome = %v", got)
	}
}

// (f) An invalid pool_max_concurrent is refused at load, naming the key.
func TestPoolCapInvalidConfigRefusedAtLoad(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"valid", `pool_max_concurrent = { "openai-api" = 2 }`, ""},
		{"not_a_table", "pool_max_concurrent = 2", "quota.pool_max_concurrent must map pool names to positive integers"},
		{"string_value", `pool_max_concurrent = { "openai-api" = "2" }`, "quota.pool_max_concurrent.openai-api must be a positive integer"},
		{"float_value", `pool_max_concurrent = { "openai-api" = 1.5 }`, "quota.pool_max_concurrent.openai-api must be a positive integer"},
		{"zero", `pool_max_concurrent = { "openai-api" = 0 }`, "quota.pool_max_concurrent.openai-api must be a positive integer"},
		{"negative", `pool_max_concurrent = { "openai-api" = -1 }`, "quota.pool_max_concurrent.openai-api must be a positive integer"},
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
				if got := poolMaxConcurrent(cfg.Quota); got["openai-api"] != 2 {
					t.Fatalf("caps = %v, want openai-api=2", got)
				}
				return
			}
			if exitCode(err) != exitConfig || err == nil || err.Error() != tc.want {
				t.Fatalf("want exit %d %q, got %v", exitConfig, tc.want, err)
			}
		})
	}
}

// (g) #290's fleet-wide credit last band runs two passes. A capped credit pool
// on the first host is dropped there, in pass 2, and the pass goes on to the
// next host's credit candidate: the capped pool is never counted as admitted.
func TestPoolCapDropsCappedCreditPoolInSecondPass(t *testing.T) {
	const openaiKey = "openai-api:00000000000000e1"
	rows := []CatalogRow{
		ompRow("omp-sol", "openai/gpt-6.1-sol", "openai-api"),
		ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"),
	}
	a, s, q, src := hostFirstFixture(t, "worker", "A", rows,
		map[string]string{"mac-b": openaiKey, "cloud-small": testCreditA},
		map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
	serveOMPRouting(t, a)
	servePoolCapPolicy(t, a, poolCapCreditPolicy)
	q.PoolViews["openai-api"] = PoolView{Status: "ok", Class: "credit"}
	q.PoolViews["anthropic-api"] = PoolView{Status: "ok", Class: "credit"}
	now := a.Now()
	src.accounts[openaiKey] = SharedAccount{Key: openaiKey, Observed: true, Fresh: true, MeasuredAt: now}
	src.accounts[testCreditA] = SharedAccount{Key: testCreditA, Observed: true, Fresh: true, MeasuredAt: now}
	q.GateDo = gatePools(map[string]string{"omp-sol": "openai-api", "omp-opus": "anthropic-api"})
	a.Config.Quota = map[string]any{"pool_max_concurrent": map[string]any{"openai-api": int64(2)}}
	a.LiveSpawnProfiles = func(context.Context, string) (map[string]int, error) {
		return map[string]int{"omp-sol": 2}, nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" || sel.chosen.host != "cloud-small" {
		t.Fatalf("chosen = %s on %s, want omp-opus on cloud-small", sel.chosen.seatName, sel.chosen.host)
	}
	if got := outcomeOf(t, sel, "omp-sol"); got != "dropped:pool_at_cap" {
		t.Fatalf("openai-api outcome = %q, want dropped:pool_at_cap", got)
	}
	if !slices.Contains(hostPasses(sel), "cloud-small/2") {
		t.Fatalf("the credit band is admitted in pass 2: %v", hostPasses(sel))
	}
	for _, c := range sel.cands {
		if c.row.Pool == "openai-api" && (c.outcome == "chosen" || c.outcome == "below_choice") {
			t.Fatalf("capped openai-api candidate counted as admitted: %+v", c)
		}
	}
}
