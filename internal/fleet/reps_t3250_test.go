package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hkBenchFake is a tiny hk: bench reps are keyed by origin id (insert-once,
// identical resend OK, different content 409) and quota observations record
// the exact JSON a manual write sent.
type hkBenchFake struct {
	mu           sync.Mutex
	reps         []map[string]any
	repBodies    []string
	observations []string
	manual       []shared.Observation
	leased       int
	next         int64
	repCanon     map[float64]string
	repIDs       map[float64]int64
}

func (f *hkBenchFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/v1/bench/reps":
		body, _ := io.ReadAll(r.Body)
		f.repBodies = append(f.repBodies, string(body))
		var input struct {
			Reps []map[string]any `json:"reps"`
		}
		if err := json.Unmarshal(body, &input); err != nil || len(input.Reps) != 1 {
			http.Error(w, `{"error":"invalid bench reps"}`, http.StatusBadRequest)
			return
		}
		rep := input.Reps[0]
		origin, _ := rep["origin_id"].(float64)
		canonical, _ := json.Marshal(rep)
		if f.repCanon == nil {
			f.repCanon = map[float64]string{}
			f.repIDs = map[float64]int64{}
		}
		if prior, ok := f.repCanon[origin]; ok {
			if prior != string(canonical) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"bench_rep_conflict"}`))
				return
			}
			writeJSONBody(w, http.StatusOK, map[string]any{"upserted": 1, "ids": []int64{f.repIDs[origin]}})
			return
		}
		f.next++
		f.repCanon[origin] = string(canonical)
		f.repIDs[origin] = f.next
		rep["id"] = f.next
		f.reps = append(f.reps, rep)
		writeJSONBody(w, http.StatusOK, map[string]any{"upserted": 1, "ids": []int64{f.next}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/bench/reps":
		writeJSONBody(w, http.StatusOK, map[string]any{"reps": f.reps})
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/lease"):
		f.leased++
		var input struct {
			Holder string `json:"holder"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		writeJSONBody(w, http.StatusOK, map[string]any{"holder": input.Holder, "expires_at": "2026-01-01T00:05:00Z", "acquired": true})
	case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/lease"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/observation"):
		body, _ := io.ReadAll(r.Body)
		f.observations = append(f.observations, string(body))
		var input struct {
			Observation shared.Observation `json:"observation"`
		}
		if err := json.Unmarshal(body, &input); err != nil {
			http.Error(w, `{"error":"invalid_quota_request"}`, http.StatusBadRequest)
			return
		}
		f.next++
		input.Observation.ID = f.next
		f.manual = append(f.manual, input.Observation)
		writeJSONBody(w, http.StatusCreated, input.Observation)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/latest"):
		writeJSONBody(w, http.StatusOK, map[string]any{"observation": nil, "manual": f.manual})
	default:
		http.Error(w, `{"error":"unexpected"}`, http.StatusNotImplemented)
	}
}

func writeJSONBody(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func benchFakeApp(t *testing.T, srv *httptest.Server) *App {
	t.Helper()
	a := testApp(t)
	// hosts.toml [quota] url overrides this endpoint.
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a.Config.Quota = map[string]any{
		"url":      srv.URL,
		"accounts": map[string]any{"anthropic-api": "0123456789abcdef"},
	}
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
		return []CatalogRow{{Profile: "omp-opus", Effort: "high", Grade: "B", ModelID: "anthropic/claude-opus-5-5"}}, nil
	}
	a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: a.Now}, now: a.Now, pendingPath: filepath.Join(a.Home, "reps.json")}
	return a
}

func TestRepWireMatchesPaceleverEnvelope(t *testing.T) {
	completed := 1
	r := Rep{
		OriginID: 123, Profile: "omp-opus", ModelID: "anthropic/claude-opus-5-5",
		TaskRef: "hk3250", Tier: "T2", Role: "impl", Rounds: 1, BlockersFound: 2,
		Completed: &completed, Notes: "source=mergewake", RecordedAt: "2026-11-07T09:00:00Z",
		Effort: "high", Grade: "B", TableGrade: "B",
	}
	got, err := json.Marshal(repWire(r))
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"blockers_found":2,"completed":1,"effort":"high","grade":"B","input_tokens":null,"model_id":"anthropic/claude-opus-5-5","notes":"source=mergewake","origin_id":123,"output_tokens":null,"profile":"omp-opus","recorded_at":"2026-11-07T09:00:00Z","role":"impl","rounds":1,"table_grade":"B","task_ref":"hk3250","tier":"T2"}`
	if string(got) != want {
		t.Fatalf("rep wire\n got %s\nwant %s", got, want)
	}
}

func TestRepNotesCarryOutcomesForOlderServers(t *testing.T) {
	green := true
	r := Rep{Notes: `{"source":"mergewake"}`, FirstCIGreen: &green, FindingsBySeverity: map[string]int{"low": 2}}
	if err := hydrateRepOutcomes(&r); err != nil {
		t.Fatal(err)
	}
	const want = `{"findings_by_severity":{"low":2},"first_ci_green":true,"source":"mergewake"}`
	if got := repNotesForWire(r); got != want {
		t.Fatalf("notes\n got %s\nwant %s", got, want)
	}
}

func TestRepsAddWritesPaceleverEnvelopeAndReceipt(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	out := &strings.Builder{}
	err := a.repsAdd(context.Background(), []string{
		"--profile", "omp-opus", "--model", "anthropic/claude-opus-5-5",
		"--effort", "high", "--task", "hk3250", "--tier", "T2", "--role", "impl",
		"--grade", "B", "--rounds", "1", "--blockers-found", "2", "--completed", "1",
		"--notes", "source=mergewake",
	}, out)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != "recorded rep id=srv:1\n" {
		t.Fatalf("receipt = %q", out.String())
	}
	if len(fake.repBodies) != 1 {
		t.Fatalf("PUT bodies = %d", len(fake.repBodies))
	}
	var input struct {
		Reps []map[string]any `json:"reps"`
	}
	if err := json.Unmarshal([]byte(fake.repBodies[0]), &input); err != nil || len(input.Reps) != 1 {
		t.Fatalf("body = %s", fake.repBodies[0])
	}
	rep := input.Reps[0]
	for key, want := range map[string]any{
		"profile": "omp-opus", "model_id": "anthropic/claude-opus-5-5", "task_ref": "hk3250",
		"tier": "T2", "role": "impl", "rounds": float64(1), "blockers_found": float64(2),
		"completed": float64(1), "effort": "high", "grade": "B", "table_grade": "B",
		"notes": "source=mergewake", "recorded_at": "2026-01-01T00:00:00Z",
	} {
		if rep[key] != want {
			t.Fatalf("%s = %v, want %v (body %s)", key, rep[key], want, fake.repBodies[0])
		}
	}
	if origin, _ := rep["origin_id"].(float64); origin <= 0 {
		t.Fatalf("origin_id = %v", rep["origin_id"])
	}
}

func TestGradeSSRepsListAndCompareReadTheSharedStore(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	completed := 1
	fake.reps = []map[string]any{
		{"id": float64(1), "origin_id": float64(10), "profile": "omp-opus", "model_id": "m",
			"task_ref": "hk1", "tier": "T1", "role": "impl", "rounds": float64(1),
			"blockers_found": float64(0), "completed": float64(1), "recorded_at": "2026-01-01T00:00:00Z",
			"effort": "high", "grade": "A", "table_grade": "A", "notes": ""},
		{"id": float64(2), "origin_id": float64(11), "profile": "omp-opus", "model_id": "m",
			"task_ref": "hk4044", "tier": "T2", "role": "impl", "rounds": float64(1),
			"blockers_found": float64(0), "completed": float64(1), "recorded_at": "2026-01-01T00:00:00Z",
			"effort": "high", "grade": "SS", "table_grade": "SS", "notes": ""},
	}
	_ = completed
	out := &strings.Builder{}
	if err := a.repsList(context.Background(), "list", []string{"--json"}, out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Reps   []Rep  `json:"reps"`
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(out.String()), &listed); err != nil || len(listed.Reps) != 2 || listed.Source != "handoffkeep" {
		t.Fatalf("list = %s (%v)", out.String(), err)
	}
	out.Reset()
	if err := a.repsList(context.Background(), "list", []string{"--grade", "SS", "--json"}, out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(out.String()), &listed); err != nil || len(listed.Reps) != 1 || listed.Reps[0].Grade != "SS" {
		t.Fatalf("SS reps filter = %s (%v)", out.String(), err)
	}
	out.Reset()
	if err := a.repsList(context.Background(), "compare", []string{"--grade", "SS"}, out, io.Discard); err != nil {
		t.Fatal(err)
	}
	var compared struct {
		Profiles map[string]map[string]any `json:"profiles"`
	}
	if err := json.Unmarshal([]byte(out.String()), &compared); err != nil {
		t.Fatal(err)
	}
	if got := compared.Profiles["omp-opus@high"]["count"]; got != float64(1) {
		t.Fatalf("compare = %s", out.String())
	}
}

func TestManualSetKeepsDollarsLocallyAndWritesPaceleverWire(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC) }
	out := &strings.Builder{}
	if err := a.manualSet(context.Background(), []string{
		"--pool", "anthropic-api", "--credit-balance", "150", "--credit-total", "200", "--expires", "2026-11-16",
	}, out); err != nil {
		t.Fatal(err)
	}
	var got ManualObservation
	if err := json.Unmarshal([]byte(out.String()), &got); err != nil {
		t.Fatal(err)
	}
	if got.Credit == nil || got.Credit.RemainingUSD != 150 || got.Credit.TotalUSD != 200 || got.Credit.ExpiresOn != "2026-11-16" {
		t.Fatalf("local credit = %+v", got.Credit)
	}
	if got.UsedPct == nil || *got.UsedPct != 25 {
		t.Fatalf("used_pct = %v", got.UsedPct)
	}
	if len(fake.observations) != 1 {
		t.Fatalf("observations = %d", len(fake.observations))
	}
	var wire struct {
		Holder      string             `json:"holder"`
		Observation shared.Observation `json:"observation"`
	}
	if err := json.Unmarshal([]byte(fake.observations[0]), &wire); err != nil {
		t.Fatal(err)
	}
	obs := wire.Observation
	if obs.ID != 0 || obs.Kind != "manual" || obs.Window != "credit" ||
		obs.MeasuredAt.Format(time.RFC3339Nano) != "2026-11-07T09:00:00Z" ||
		obs.ExpiresAt == nil || obs.ExpiresAt.Format(time.RFC3339Nano) != "2026-11-16T00:00:00Z" ||
		len(obs.Buckets) != 1 || obs.Buckets[0].Window != "credit" || obs.Buckets[0].UsedPct != 25 || obs.Buckets[0].Scope != "account" {
		t.Fatalf("wire = %s", fake.observations[0])
	}
	if strings.Contains(fake.observations[0], "remaining_usd") || strings.Contains(fake.observations[0], "total_usd") {
		t.Fatalf("hk wire must not carry dollar fields: %s", fake.observations[0])
	}
}

func TestManualListReadsSharedEntries(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC) }
	expires := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	fake.manual = []shared.Observation{{
		ID: 7, Kind: "manual", Window: "credit", MeasuredAt: a.Now(),
		ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 25, Scope: "account"}},
	}}
	out := &strings.Builder{}
	if err := a.manualList(context.Background(), []string{"anthropic-api"}, out); err != nil {
		t.Fatal(err)
	}
	var valid []ManualObservation
	if err := json.Unmarshal([]byte(out.String()), &valid); err != nil {
		t.Fatal(err)
	}
	if len(valid) != 1 || valid[0].ID != "hk-7" || valid[0].UsedPct == nil || *valid[0].UsedPct != 25 || valid[0].Credit != nil {
		t.Fatalf("manual list = %s", out.String())
	}
}

func TestManualClearPublishesTombstone(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	now := time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)
	a.Now = func() time.Time { return now }
	expires := now.Add(time.Hour)
	fake.manual = []shared.Observation{{
		ID: 7, Kind: "manual", Window: "credit", MeasuredAt: now,
		ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 25, Scope: "account"}},
	}}
	if err := a.manualClear(context.Background(), []string{"--pool", "anthropic-api"}); err != nil {
		t.Fatal(err)
	}
	if len(fake.observations) != 1 {
		t.Fatalf("observations = %d", len(fake.observations))
	}
	var wire struct {
		Observation shared.Observation `json:"observation"`
	}
	if err := json.Unmarshal([]byte(fake.observations[0]), &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Observation.Buckets) != 0 || wire.Observation.ExpiresAt == nil || !wire.Observation.ExpiresAt.Equal(now) {
		t.Fatalf("tombstone = %s", fake.observations[0])
	}
}

func TestCreditBalancesReadSharedManualEntries(t *testing.T) {
	now := time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)
	expires := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	q := CLIQuota{
		Now:            func() time.Time { return now },
		Accounts:       map[string]string{"anthropic-api": "0123456789abcdef"},
		CreditTotalUSD: map[string]float64{"anthropic-api": 200},
		SharedLatest: func(context.Context, string) (shared.Latest, error) {
			return shared.Latest{Manual: []shared.Observation{
				{ID: 1, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-25 * time.Hour), ExpiresAt: &expires,
					Buckets: []shared.Bucket{{Window: "credit", UsedPct: 30, Scope: "account"}}},
				{ID: 2, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-time.Hour), ExpiresAt: &expires,
					Buckets: []shared.Bucket{{Window: "credit", UsedPct: 42.35, Scope: "account"}}},
			}}, nil
		},
	}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := balances["anthropic-api"]
	if c == nil || c.LeftPct != 57.65 || c.BalanceUSD == nil || *c.BalanceUSD < 115.29 || *c.BalanceUSD > 115.31 {
		t.Fatalf("balance = %+v", c)
	}
	if c.BurnUSDPerH < 1.02 || c.BurnUSDPerH > 1.04 {
		t.Fatalf("burn = %v", c.BurnUSDPerH)
	}
}

func TestPaneSweepRecordsAMergedJobOutcome(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	f := newPaneSweepFixture(t)
	f.a.BenchStore = &BenchStore{hk: &hkQuota{base: srv.URL, token: "test-token", now: f.a.Now}, now: f.a.Now}
	f.job("j", "implementer", "42")
	putEvent(t, f.a, "j", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/9", "head": "head-9"})
	f.tasks["42"], f.category["42"] = "merged", "completed"
	f.prChanges["42"] = []PRChange{{Number: 9, State: "merged", Head: "head-9",
		OpenedAt: "2026-03-01T11:00:00Z", MergedAt: "2026-03-01T12:00:00Z"}}
	f.at(0)
	f.at(11)
	if len(fake.reps) != 1 {
		t.Fatalf("outcome rows = %d", len(fake.reps))
	}
	row := fake.reps[0]
	if row["profile"] != "cmd-dsflash41" || row["task_ref"] != "hk42" || row["merged"] != true {
		t.Fatalf("row = %v", row)
	}
}

func TestAutomaticOutcomeRecordsOneRow(t *testing.T) {
	fake := &hkBenchFake{}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	a := benchFakeApp(t, srv)
	a.Now = func() time.Time { return time.Date(2026, 11, 7, 8, 0, 0, 0, time.UTC) }
	putEvent(t, a, "pw-job", "job.claim", map[string]any{"task_id": "3250", "t_level": "T2", "role": "implementer"})
	putEvent(t, a, "pw-job", "job.spawned", map[string]any{
		"profile": "sonnet", "effort": "high", "model_id": "claude-sonnet-5-5",
		"harness_session": map[string]any{"kind": "claude", "id": "s1"}, "task_id": "3250",
	})
	putEvent(t, a, "pw-job", "job.joined", map[string]any{"pr": "https://github.com/mgh3326/panewire/pull/77", "head": "abc123"})
	putEvent(t, a, "pw-job", "job.completed", map[string]any{})
	if err := os.WriteFile(filepath.Join(a.Jobs, "pw-job", "findings.json"),
		[]byte(`{"findings":[{"severity":"high"},{"severity":"low"},{"severity":"low"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	j, err := a.job("pw-job")
	if err != nil {
		t.Fatal(err)
	}
	status := TaskStatus{State: "merged", Category: "completed", Title: "M4 reps (grade B, T2)"}
	change := PRChange{Number: 77, Repo: "mgh3326/panewire", State: "merged", Head: "abc123",
		OpenedAt: "2026-11-07T08:00:00Z", MergedAt: "2026-11-07T09:00:00Z"}
	if err := a.recordMergedOutcome(context.Background(), j, "3250", status, change); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("rows = %d", len(fake.reps))
	}
	row := fake.reps[0]
	if row["profile"] != "sonnet" || row["effort"] != "high" || row["model_id"] != "claude-sonnet-5-5" ||
		row["task_ref"] != "hk3250" || row["tier"] != "T2" || row["grade"] != "B" ||
		row["rounds"] != float64(1) || row["blockers_found"] != float64(1) ||
		row["completed"] != float64(1) || row["merged"] != true ||
		row["high_findings"] != float64(1) || row["critical_findings"] != float64(0) ||
		row["recorded_at"] != "2026-11-07T09:00:00Z" {
		t.Fatalf("row = %v", row)
	}
	if spawn, _ := row["spawn_to_merge_s"].(float64); spawn < 3599 || spawn > 3600 {
		t.Fatalf("spawn_to_merge_s = %v", row["spawn_to_merge_s"])
	}
	notes, _ := row["notes"].(string)
	for _, want := range []string{`"source":"panewire"`, `"job":"pw-job"`, `"task":"3250"`, `"harness":"claude"`,
		`"findings_by_severity":{"high":1,"low":2}`, `"time_to_merge_s":3600`} {
		if !strings.Contains(notes, want) {
			t.Fatalf("notes %s missing %s", notes, want)
		}
	}
	if err := a.recordMergedOutcome(context.Background(), j, "3250", status, change); err != nil {
		t.Fatal(err)
	}
	if len(fake.reps) != 1 {
		t.Fatalf("a second merged signal duplicated the row: %d", len(fake.reps))
	}
}
