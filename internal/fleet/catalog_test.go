package fleet

import (
	"bytes"
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
)

const catalogFixture = `{"catalog":[
 {"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S+","gate":"default","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator","deviation_ref":"hk:task/1"},
 {"profile":"sonnet","effort":"high","model_id":"claude-sonnet-5-5","pool":"claude","grade":"A","gate":"default","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator","deviation_ref":"hk:task/1"}
]}`

type catalogServer struct {
	mu    sync.Mutex
	calls int
	fail  bool
	body  string
}

func (s *catalogServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	fail := s.fail
	body := s.body
	s.mu.Unlock()
	if fail {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.URL.Path != "/v1/bench/catalog" || !strings.Contains(r.URL.RawQuery, "include_retired=1") {
		http.Error(w, "unexpected path", http.StatusNotFound)
		return
	}
	if r.Header.Get("Authorization") != "Bearer test-token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	_, _ = io.WriteString(w, body)
}

func (s *catalogServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *catalogServer) setFail(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = fail
}

func catalogFixtureClient(t *testing.T, server *httptest.Server, now func() time.Time) *CatalogClient {
	t.Helper()
	client, err := NewCatalogClient(CatalogClientConfig{URL: server.URL, Token: "test-token", CacheDir: t.TempDir(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// Acceptance 2: the catalog read never runs pacelever.
func TestCatalogReadsHandoffkeepWithoutPacelever(t *testing.T) {
	state := &catalogServer{body: catalogFixture}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	runner := &fakeRunner{Do: func(c Command) Result {
		t.Errorf("catalog ran %s %v", c.Name, c.Args)
		return Result{}
	}}
	rows, err := (CLIQuota{Runner: runner, Binary: "pacelever"}).Catalog(context.Background())
	if err != nil || len(rows) != 2 || rows[0].Profile != "opus" || rows[1].Grade != "A" {
		t.Fatalf("catalog rows=%+v err=%v", rows, err)
	}
	if len(runner.Calls) != 0 || state.count() != 1 {
		t.Fatalf("runner calls=%v hk calls=%d", runner.Calls, state.count())
	}
}

func TestCatalogCacheTTLAndStaleFallback(t *testing.T) {
	state := &catalogServer{body: catalogFixture}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	now := time.Date(2026, 11, 7, 3, 0, 0, 0, time.UTC)
	client := catalogFixtureClient(t, server, func() time.Time { return now })

	first, err := client.Read(context.Background(), false)
	if err != nil || first.Info.Source != catalogSourceServer || first.Info.Stale || state.count() != 1 {
		t.Fatalf("server read: %+v err=%v calls=%d", first.Info, err, state.count())
	}
	second, err := client.Read(context.Background(), false)
	if err != nil || second.Info.Source != catalogSourceCache || second.Info.Stale || state.count() != 1 {
		t.Fatalf("fresh cache: %+v err=%v calls=%d", second.Info, err, state.count())
	}
	now = now.Add(2 * time.Hour)
	state.setFail(true)
	third, err := client.Read(context.Background(), false)
	if err != nil || third.Info.Source != catalogSourceCache || third.Info.Stale || state.count() != 2 {
		t.Fatalf("last-good cache: %+v err=%v calls=%d", third.Info, err, state.count())
	}
	now = now.Add(48 * time.Hour)
	stale, err := client.Read(context.Background(), false)
	if err != nil || stale.Info.Source != catalogSourceCacheStale || !stale.Info.Stale || state.count() != 3 {
		t.Fatalf("stale cache: %+v err=%v calls=%d", stale.Info, err, state.count())
	}
	if _, err := (func() (CatalogData, error) {
		miss, missErr := NewCatalogClient(CatalogClientConfig{URL: server.URL, Token: "test-token", CacheDir: t.TempDir(), Now: func() time.Time { return now }})
		if missErr != nil {
			return CatalogData{}, missErr
		}
		return miss.Read(context.Background(), false)
	})(); err == nil {
		t.Fatal("failed read without a cache was accepted")
	}
}

func TestCatalogNoCacheSkipsFreshCache(t *testing.T) {
	state := &catalogServer{body: catalogFixture}
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	now := time.Date(2026, 11, 7, 3, 0, 0, 0, time.UTC)
	client := catalogFixtureClient(t, server, func() time.Time { return now })
	if _, err := client.Read(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if data, err := client.Read(context.Background(), true); err != nil || data.Info.Source != catalogSourceServer || state.count() != 2 {
		t.Fatalf("no-cache read: %+v err=%v calls=%d", data.Info, err, state.count())
	}
}

// Acceptance 3: dropping the unknown-pool check would include the unknown row
// here and fail this test.
func TestCatalogDecodeDropsUnknownPool(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default"}`),
		json.RawMessage(`{"profile":"mystery","effort":"high","model_id":"m","pool":"upstage","grade":"A","gate":"default"}`),
	}
	rows, warnings, keep, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 1 || len(keep) != 1 || rows[0].Profile != "opus" {
		t.Fatalf("unknown pool kept: rows=%+v keep=%v err=%v", rows, keep, err)
	}
	if len(warnings) != 1 || warnings[0] != "unknown pool [upstage] ignored" {
		t.Fatalf("warnings = %v", warnings)
	}
	if _, _, _, err := decodeCatalogRows(raw[1:]); err == nil || !strings.Contains(err.Error(), "no supported pool rows") {
		t.Fatalf("all-unknown catalog: %v", err)
	}
}

func TestCatalogDecodeSkipsMalformedRows(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default"}`),
		json.RawMessage(`{"profile":"p","effort":"","model_id":"","pool":"","grade":"","gate":""}`),
	}
	rows, warnings, keep, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 1 || rows[0].Profile != "opus" || len(keep) != 1 || keep[0] != 0 {
		t.Fatalf("malformed row affected valid rung: rows=%+v keep=%v err=%v", rows, keep, err)
	}
	if len(warnings) != 1 || warnings[0] != "catalog_row_invalid:p@" {
		t.Fatalf("warnings = %v", warnings)
	}
	if _, _, _, err := decodeCatalogRows(raw[1:]); err == nil || !strings.Contains(err.Error(), "no supported pool rows") {
		t.Fatalf("all-invalid catalog: %v", err)
	}
}

type catalogDetailsQuota struct {
	*fakeQuota
	data CatalogData
}

func (q catalogDetailsQuota) CatalogDetails(context.Context) (CatalogData, error) {
	return q.data, nil
}

func TestCatalogSelectionKeepsValidRowAndReportsMalformedRow(t *testing.T) {
	rows := []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"}}
	a, s, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(30, 8)}, []string{"claude"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude"})
	serveRouting(t, routingDocV1)
	route, err := a.routeFor(context.Background(), "implementer", "")
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	warning := "catalog_row_invalid:p@"
	a.Quota = catalogDetailsQuota{fakeQuota: q, data: CatalogData{
		Rows: []CatalogEntry{{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"}},
		Info: CatalogInfo{Source: catalogSourceServer}, Warnings: []string{warning},
	}}
	s.Role = "implementer"
	sel, err := a.selectFor(context.Background(), s, selReq{grade: "A", t: "T1", role: "implementer", route: route, urgency: urgencyNormal, urgencySource: "default"})
	if err != nil || sel == nil || sel.chosen.seat.Canon != "sonnet" {
		t.Fatalf("selection should use the valid row: selection=%+v err=%v", sel, err)
	}
	if !strings.Contains(strings.Join(sel.degraded, ","), warning) {
		t.Fatalf("degraded warnings = %v", sel.degraded)
	}
	if got := strings.Count(a.Err.(*bytes.Buffer).String(), "panewire: warning: "+warning); got != 1 {
		t.Fatalf("warning log count=%d output=%q", got, a.Err.(*bytes.Buffer).String())
	}
}

func TestCatalogDecodeKeepsCreditPools(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"profile":"omp-opus","effort":"high","model_id":"m","pool":"anthropic-api","grade":"A","gate":"default"}`),
		json.RawMessage(`{"profile":"omp-sol","effort":"high","model_id":"m","pool":"openai-api","grade":"A","gate":"default"}`),
	}
	rows, _, _, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 2 {
		t.Fatalf("credit pools dropped: rows=%+v err=%v", rows, err)
	}
}

func TestCatalogDecodeRejectsDuplicateAndNonMonotonic(t *testing.T) {
	duplicate := []json.RawMessage{
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default"}`),
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default"}`),
	}
	if _, _, _, err := decodeCatalogRows(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate catalog rung") {
		t.Fatalf("duplicate: %v", err)
	}
	nonMonotonic := []json.RawMessage{
		json.RawMessage(`{"profile":"opus","effort":"low","model_id":"m","pool":"claude","grade":"A","gate":"default"}`),
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"C","gate":"default"}`),
	}
	if _, _, _, err := decodeCatalogRows(nonMonotonic); err == nil || !strings.Contains(err.Error(), "not monotonic for opus") {
		t.Fatalf("non-monotonic: %v", err)
	}
}

func TestCatalogDecodeInfersAgyGroup(t *testing.T) {
	raw := []json.RawMessage{
		json.RawMessage(`{"profile":"agy-new","effort":"high","model_id":"gemini-9-flash-high","pool":"agy","grade":"B","gate":"default"}`),
		json.RawMessage(`{"profile":"agy-mystery","effort":"high","model_id":"mystery-model","pool":"agy","grade":"B","gate":"default"}`),
	}
	rows, _, _, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 2 || rows[0].Group != "gemini" || rows[1].Group != "" {
		t.Fatalf("agy groups: %+v err=%v", rows, err)
	}
}

// The push fixture from pacelever's bench_test.go: the hk payload is byte for
// byte the one pacelever sends, and local fields never reach it.
func TestCatalogWireMatchesPaceleverPushFixture(t *testing.T) {
	row := CatalogEntry{Profile: "example", ModelID: "example-model", Effort: "high", Pool: "claude", Grade: "A", Gate: "default", Group: "gemini", DecidedAt: "2026-10-28T00:00:00Z", DecidedBy: "operator", DeviationRef: "hk:task/1"}
	body, err := json.Marshal(map[string]any{"catalog": CatalogWire([]CatalogEntry{row})})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"catalog":[{"boundary_version":"","decided_at":"2026-10-28T00:00:00Z","decided_by":"operator","deviation_ref":"hk:task/1","effort":"high","gate":"default","gate_reason":null,"grade":"A","model_id":"example-model","pool":"claude","profile":"example","retired_at":null,"score":null}]}`
	if string(body) != want {
		t.Fatalf("push payload differs:\n got %s\nwant %s", body, want)
	}
}

func TestValidateCatalogRefusals(t *testing.T) {
	valid := CatalogEntry{Profile: "example", ModelID: "m", Effort: "high", Pool: "claude", Grade: "A", Gate: "default", DecidedAt: "2026-11-01T00:00:00Z", DecidedBy: "operator", DeviationRef: "hk:task/1"}
	for _, tc := range []struct {
		name string
		rows []CatalogEntry
		want string
	}{
		{"empty", nil, "non-empty catalog required"},
		{"grade", []CatalogEntry{func() CatalogEntry { r := valid; r.Grade = "unknown"; return r }()}, `invalid catalog grade "unknown"`},
		{"provenance", []CatalogEntry{func() CatalogEntry { r := valid; r.DecidedBy = ""; return r }()}, "catalog provenance required"},
		{"decided_at", []CatalogEntry{func() CatalogEntry { r := valid; r.DecidedAt = "2026-11-01 00:00:00"; return r }()}, "catalog decided_at must include timezone"},
		{"monotonic", []CatalogEntry{
			func() CatalogEntry { r := valid; r.Effort = "low"; r.Grade = "A"; return r }(),
			func() CatalogEntry { r := valid; r.Effort = "high"; r.Grade = "C"; return r }(),
		}, "not monotonic for example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateCatalog(tc.rows); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
	if err := ValidateCatalog([]CatalogEntry{valid}); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogReadRefusesStaleQuotaCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "offline", http.StatusServiceUnavailable) }))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	client, err := NewCatalogClient(CatalogClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cache := catalogCache{FetchedAt: "2026-11-05T00:00:00Z", EndpointID: client.endpointID(), Rows: []json.RawMessage{json.RawMessage(`{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default"}`)}}
	data, _ := json.Marshal(cache)
	cachePath := client.cachePath()
	if err := os.MkdirAll(filepath.Dir(cachePath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := (CLIQuota{}).Catalog(context.Background()); err == nil || !strings.Contains(err.Error(), "bench catalog is stale") {
		t.Fatalf("stale quota catalog: %v", err)
	}
}

func TestCatalogCredentialsSetCFHeaders(t *testing.T) {
	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = io.WriteString(w, catalogFixture)
	}))
	t.Cleanup(server.Close)
	client, err := NewCatalogClient(CatalogClientConfig{URL: server.URL, Token: "test-token", CFClientID: "cf-id", CFClientSecret: "cf-secret", CacheDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Read(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if got.Get("CF-Access-Client-Id") != "cf-id" || got.Get("CF-Access-Client-Secret") != "cf-secret" {
		t.Fatalf("CF headers = %v", got)
	}
}

func TestCatalogCredentialsIncompletePair(t *testing.T) {
	if _, err := NewCatalogClient(CatalogClientConfig{URL: "https://catalog.example.invalid", Token: "t", CFClientID: "only"}); err == nil || !strings.Contains(err.Error(), "pair incomplete") {
		t.Fatalf("incomplete pair: %v", err)
	}
	if _, err := NewCatalogClient(CatalogClientConfig{URL: "http://catalog.example.invalid", Token: "t"}); err == nil || !strings.Contains(err.Error(), "requires HTTPS") {
		t.Fatalf("plaintext remote: %v", err)
	}
}

// A ladder whose grade improves with effort is the normal shape (live
// 2026-11-07: codex-luna low C, medium B, xhigh A+). v0.0.167 refused it and
// every -m auto placement failed with POLICY_UNAVAILABLE.
func TestCatalogAcceptsGradeImprovingWithEffort(t *testing.T) {
	ladder := []json.RawMessage{
		json.RawMessage(`{"profile":"codex-luna","effort":"low","model_id":"m","pool":"codex","grade":"C","gate":"default"}`),
		json.RawMessage(`{"profile":"codex-luna","effort":"medium","model_id":"m","pool":"codex","grade":"B","gate":"default"}`),
		json.RawMessage(`{"profile":"codex-luna","effort":"high","model_id":"m","pool":"codex","grade":"unrated","gate":"default"}`),
		json.RawMessage(`{"profile":"codex-luna","effort":"xhigh","model_id":"m","pool":"codex","grade":"A+","gate":"default"}`),
	}
	if _, _, _, err := decodeCatalogRows(ladder); err != nil {
		t.Fatalf("improving ladder refused: %v", err)
	}
}
