package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/claude"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// claudeSecret is the OAuth token of the test login. It may appear only in the
// Authorization header of the one usage request.
const claudeSecret = "sk-ant-oat01-node-test-token-must-not-leak"

// claudeFPForTest is the fingerprint of the test login's accountUuid
// (00000000-0000-4000-8000-000000000001), computed with pacelever@f53bb01.
const claudeFPForTest = "e864f6030c9d151d"

var claudeKeyForTest = "claude:" + claudeFPForTest

var claudeTestNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

type claudeAPIFunc func(*http.Request) (*http.Response, error)

func (f claudeAPIFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func claudeAPIReply(status int, body string) claudeAPIFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

const claudeAPIBody = `{"five_hour":{"utilization":42,"resets_at":"2026-11-05T15:00:00Z"},"seven_day":{"utilization":10,"resets_at":"2026-11-09T00:00:00Z"}}`

// claudeHK is a small handoffkeep: one account's reading and lease.
type claudeHK struct {
	t           *testing.T
	server      *httptest.Server
	mu          sync.Mutex
	calls       []string
	latest      json.RawMessage
	holder      string
	failPublish bool
	// foreignHolder, when set, holds every lease for a minute: this node never
	// gets it.
	foreignHolder string
	pubs          []json.RawMessage
}

func newClaudeHK(t *testing.T) *claudeHK {
	h := &claudeHK{t: t}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.calls = append(h.calls, r.Method+" "+r.URL.Path)
		var body struct {
			Holder      string          `json:"holder"`
			Observation json.RawMessage `json:"observation"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		switch {
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/latest"):
			if h.latest == nil {
				_, _ = w.Write([]byte(`{"observation":null}`))
				return
			}
			_, _ = w.Write([]byte(`{"observation":` + string(h.latest) + `}`))
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/lease") && h.foreignHolder != "":
			_ = json.NewEncoder(w).Encode(shared.Lease{Holder: h.foreignHolder, ExpiresAt: time.Now().Add(time.Minute)})
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/lease"):
			h.holder = body.Holder
			_ = json.NewEncoder(w).Encode(shared.Lease{Holder: body.Holder, ExpiresAt: time.Now().Add(time.Minute), Acquired: true})
		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/observation"):
			if h.failPublish {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			h.pubs = append(h.pubs, body.Observation)
			h.latest = body.Observation
			w.WriteHeader(204)
		default:
			w.WriteHeader(204)
		}
	}))
	t.Cleanup(h.server.Close)
	return h
}

func (h *claudeHK) published() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pubs)
}

// claudeStore writes spawn.json and the mode-0600 handoffkeep env naming url.
func claudeStore(t *testing.T, url string) string {
	t.Helper()
	dir := t.TempDir()
	env := filepath.Join(dir, "hk.env")
	if err := os.WriteFile(env, []byte("HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN=hk-node-test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	spawn := filepath.Join(dir, "spawn.json")
	if err := os.WriteFile(spawn, []byte(`{"enabled":false,"handoffkeep_env":"`+env+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return spawn
}

func claudeLoginFiles(token string, expires time.Time) map[string]string {
	files := map[string]string{"/home/u/.claude.json": `{"oauthAccount":{"accountUuid":"00000000-0000-4000-8000-000000000001"}}`}
	if token != "" {
		files["/home/u/.claude/.credentials.json"] = `{"claudeAiOauth":{"accessToken":"` + token + `","expiresAt":` + strconv.FormatInt(expires.UnixMilli(), 10) + `}}`
	}
	return files
}

// claudeTestSource is a claude measurement over fakes: the login files, the
// usage API, no claude binary.
func claudeTestSource(spawnConfig string, files map[string]string, api claudeAPIFunc) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "claude", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return claudeTestNow }, Wait: 5 * time.Millisecond,
		Claude: claude.Config{
			HTTP: api, Home: "/home/u", GOOS: "linux", LookupEnv: func(string) (string, bool) { return "", false },
			ReadFile: func(path string) ([]byte, error) {
				if body, ok := files[path]; ok {
					return []byte(body), nil
				}
				return nil, os.ErrNotExist
			},
			RunCLI: func(context.Context, string, string, string) (string, error) { return "", errors.New("no claude here") },
		},
	}
}

// pacelever for every pool but claude: a call naming claude fails the test.
func paceleverNeverForClaude(t *testing.T, calls *[]string) relayCommandRunner {
	return func(_ context.Context, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		for _, arg := range args {
			if arg == "claude" {
				t.Errorf("pacelever was run for claude: %v", args)
			}
		}
		return nil, errors.New("pacelever unavailable")
	}
}

// Acceptance 4: the fleet-state loop measures claude itself — the usage API,
// the shared store, the hub publish — and never execs pacelever for it.
func TestFleetQuotaClaudeIsMeasuredNatively(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	var requests []*http.Request
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": claudeTestSource(client.spawnConfigPath, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r)
		return claudeAPIReply(200, claudeAPIBody)(r)
	})}
	var pacelever []string
	var warnings []string
	client.warn = func(m string) { warnings = append(warnings, m) }

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "claude", sent)

	if len(pacelever) != 0 {
		t.Fatalf("pacelever was run for claude: %v", pacelever)
	}
	if len(requests) != 1 || requests[0].Header.Get("Authorization") != "Bearer "+claudeSecret {
		t.Fatalf("usage requests: %v", requests)
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	select {
	case event := <-client.events:
		if event.Kind != "state" {
			t.Fatalf("event %s", event.Kind)
		}
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil {
			t.Fatal(err)
		}
		if publish.Key != claudeKeyForTest || publish.Quota == nil || publish.Quota.Pool != "claude" || publish.Quota.Source != "handoffkeep" || !publish.MeasuredAt.Equal(claudeTestNow) {
			t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
		}
		if len(publish.Quota.Buckets) != 2 || *publish.Quota.Buckets[0].UsedPct != 42 || publish.Quota.Buckets[0].Window != "5h" || publish.Quota.Buckets[0].ResetsAt != "2026-11-05T15:00:00Z" {
			t.Fatalf("buckets %+v", publish.Quota.Buckets)
		}
		if strings.Contains(string(event.Payload), claudeSecret) {
			t.Fatal("token in the published state")
		}
	default:
		t.Fatal("nothing was published to the hub")
	}
	if got := poolLogin(t, client, "claude"); got.Login != fleet.ReadinessLoginPresent || got.Account != claudeKeyForTest {
		t.Fatalf("login %+v", got)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings %q", warnings)
	}

	// The next round reads the shared reading: no second API request.
	client.refreshFleetQuota(t.Context(), "claude", sent)
	if len(requests) != 1 {
		t.Fatalf("a fresh shared reading was measured again: %d requests", len(requests))
	}
}

// The same loop still runs pacelever for a pool it measures.
func TestFleetQuotaOtherPoolIsUnmeasurable(t *testing.T) {
	client := quotaClient(t)
	var calls []string
	client.refreshFleetQuota(t.Context(), "grok", map[string]fleetQuotaSent{})
	if len(calls) != 0 {
		t.Fatalf("unmeasurable pool executed commands: %v", calls)
	}
	if got := poolLogin(t, client, "grok"); got.Login == fleet.ReadinessLoginMissing {
		t.Fatalf("login %+v", got)
	}
}

func TestFleetQuotaClaudeLoginClassification(t *testing.T) {
	expired := claudeLoginFiles(claudeSecret, claudeTestNow.Add(-time.Hour))
	valid := claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour))
	for _, test := range []struct {
		name  string
		files map[string]string
		api   claudeAPIFunc
		want  string
		kind  string
	}{
		{"no credentials", claudeLoginFiles("", time.Time{}), claudeAPIReply(200, claudeAPIBody), fleet.ReadinessLoginMissing, "credentials"},
		{"token expired", expired, claudeAPIReply(200, claudeAPIBody), fleet.ReadinessLoginMissing, "token_expired"},
		{"refused token", valid, claudeAPIReply(401, "no"), fleet.ReadinessLoginMissing, "auth"},
		{"forbidden token", valid, claudeAPIReply(403, "no"), fleet.ReadinessLoginMissing, "auth"},
		{"rate limited", valid, claudeAPIReply(429, "slow"), fleet.ReadinessLoginUnknown, "rate_limited"},
		{"server error", valid, claudeAPIReply(500, "boom"), fleet.ReadinessLoginUnknown, "http"},
		{"network", valid, func(*http.Request) (*http.Response, error) { return nil, errors.New("dial " + claudeSecret) }, fleet.ReadinessLoginUnknown, "network"},
		{"unparsable", valid, claudeAPIReply(200, "{}"), fleet.ReadinessLoginUnknown, "parse_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hk := newClaudeHK(t)
			client := quotaClient(t)
			client.spawnConfigPath = claudeStore(t, hk.server.URL)
			client.nativeQuota = map[string]*nativeQuotaSource{"claude": claudeTestSource(client.spawnConfigPath, test.files, test.api)}
			var pacelever []string
			var warnings []string
			client.warn = func(m string) { warnings = append(warnings, m) }
			client.refreshFleetQuota(t.Context(), "claude", map[string]fleetQuotaSent{})
			if got := poolLogin(t, client, "claude").Login; got != test.want {
				t.Fatalf("login = %q, want %q", got, test.want)
			}
			if len(pacelever) != 0 {
				t.Fatalf("pacelever was run: %v", pacelever)
			}
			if len(warnings) != 1 || !strings.Contains(warnings[0], "error_kind "+test.kind) || strings.Contains(warnings[0], claudeSecret) {
				t.Fatalf("warnings %q, want kind %s", warnings, test.kind)
			}
			if hk.published() != 0 {
				t.Fatal("a failed measurement was published")
			}
		})
	}
}

// Acceptance 4: the quota.refresh node op measures claude itself and still
// execs pacelever for another pool.
func TestNodeQuotaRefreshClaudeIsNative(t *testing.T) {
	hk := newClaudeHK(t)
	client := &HubClient{}
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": claudeTestSource(client.spawnConfigPath, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), claudeAPIReply(200, claudeAPIBody))}
	var pacelever []string
	result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "claude"})
	if code != "" || result["outcome"] != nodeQuotaRefreshed {
		t.Fatalf("claude refresh: result=%v code=%q", result, code)
	}
	if len(pacelever) != 0 || hk.published() != 1 {
		t.Fatalf("pacelever=%v published=%d", pacelever, hk.published())
	}
	if result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "kimi"}); code != "refresh_failed" || result != nil || len(pacelever) != 0 {
		t.Fatalf("unmeasurable refresh: result=%v code=%q external=%v", result, code, pacelever)
	}

	failing := &HubClient{nativeQuota: map[string]*nativeQuotaSource{"claude": claudeTestSource("/nonexistent", claudeLoginFiles("", time.Time{}), claudeAPIReply(200, claudeAPIBody))}}
	if _, code := nodeRun(t, failing, nodeCommandQuotaRefresh, map[string]any{"pool": "claude"}); code != "refresh_failed" {
		t.Fatalf("failing claude refresh code=%q", code)
	}
}

// A node with no handoffkeep_env measures directly and publishes to no store;
// one whose store is configured but unreadable refuses rather than measuring
// around the lease.
func TestClaudeQuotaStoreConfiguration(t *testing.T) {
	files := claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour))
	api := claudeAPIReply(200, claudeAPIBody)

	dir := t.TempDir()
	noStore := filepath.Join(dir, "spawn.json")
	if err := os.WriteFile(noStore, []byte(`{"enabled":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{noStore, filepath.Join(dir, "missing.json")} {
		reading, err := claudeTestSource(path, files, api).Refresh(t.Context())
		if err != nil || reading.Key != claudeKeyForTest || reading.Source != quota.SourceUsageAPI || len(reading.Buckets) != 2 {
			t.Fatalf("direct measurement %q: %+v err=%v", path, reading, err)
		}
	}

	badEnv := filepath.Join(dir, "spawn-bad.json")
	if err := os.WriteFile(badEnv, []byte(`{"enabled":false,"handoffkeep_env":"`+filepath.Join(dir, "nope.env")+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := claudeTestSource(badEnv, files, api).Refresh(t.Context()); quota.KindOf(err) != quota.ErrKindSharedUnavailable {
		t.Fatalf("unreadable store: %v", err)
	}

	hk := newClaudeHK(t)
	noAccount := map[string]string{"/home/u/.claude/.credentials.json": files["/home/u/.claude/.credentials.json"]}
	if _, err := claudeTestSource(claudeStore(t, hk.server.URL), noAccount, api).Refresh(t.Context()); quota.KindOf(err) != quota.ErrKindAccountUnknown {
		t.Fatalf("no account identity: %v", err)
	}
}

// fleetQuotaFromReading refuses what the shared cache would not accept and
// carries a later confirmed_at.
func TestFleetQuotaFromReading(t *testing.T) {
	reset := claudeTestNow.Add(time.Hour)
	confirmed := claudeTestNow.Add(5 * time.Minute)
	reading := shared.Reading{Key: claudeKeyForTest, Source: "handoffkeep", MeasuredAt: claudeTestNow, ConfirmedAt: &confirmed, Buckets: []shared.Bucket{{Window: "5h", UsedPct: 42, ResetsAt: &reset, Scope: "account"}}}
	publish, ok := fleetQuotaFromReading("claude", reading, claudeTestNow.Add(10*time.Minute))
	if !ok || publish.ConfirmedAt == nil || !publish.ConfirmedAt.Equal(confirmed) || publish.Quota.Buckets[0].ResetsAt != "2026-11-05T13:00:00Z" {
		t.Fatalf("publish %+v ok=%v", publish, ok)
	}
	for name, mutate := range map[string]func(*shared.Reading){
		"no key":            func(r *shared.Reading) { r.Key = "" },
		"other pool":        func(r *shared.Reading) { r.Key = "codex:0123456789abcdef" },
		"no buckets":        func(r *shared.Reading) { r.Buckets = nil },
		"confirmed earlier": func(r *shared.Reading) { c := claudeTestNow.Add(-time.Minute); r.ConfirmedAt = &c },
		"confirmed future":  func(r *shared.Reading) { c := claudeTestNow.Add(time.Hour); r.ConfirmedAt = &c },
	} {
		changed := reading
		mutate(&changed)
		got, ok := fleetQuotaFromReading("claude", changed, claudeTestNow.Add(10*time.Minute))
		switch name {
		case "confirmed earlier", "confirmed future":
			if !ok || got.ConfirmedAt != nil {
				t.Errorf("%s: a confirmed_at that is not later than, or ahead of, the clock must be dropped: %+v ok=%v", name, got.ConfirmedAt, ok)
			}
		default:
			if ok {
				t.Errorf("%s reading accepted", name)
			}
		}
	}
}

// `panewire quota refresh claude` measures through the same source.
func TestQuotaCLIRefreshClaude(t *testing.T) {
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	quotaCLISource = func(string) *nativeQuotaSource {
		return claudeTestSource(spawn, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), claudeAPIReply(200, claudeAPIBody))
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "claude"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var reading quotaReading
	if err := json.Unmarshal(out.Bytes(), &reading); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if reading.Pool != "claude" || reading.Account != claudeFPForTest || reading.Source != "handoffkeep" || reading.Stale || len(reading.Buckets) != 2 || *reading.Buckets[0].UsedPct != 42 || reading.Buckets[0].Scope.Kind != "account" {
		t.Fatalf("reading %+v", reading)
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}

	// A fresh shared reading needs no login; an empty store does.
	empty := claudeStore(t, newClaudeHK(t).server.URL)
	quotaCLISource = func(string) *nativeQuotaSource {
		return claudeTestSource(empty, claudeLoginFiles("", time.Time{}), claudeAPIReply(200, claudeAPIBody))
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "claude"}, &out, &errOut); code == 0 || errOut.String() != "quota refresh: credentials\n" || out.Len() != 0 {
		t.Fatalf("failing refresh: exit %d out=%q err=%q", code, out.String(), errOut.String())
	}
}

// The token never reaches the places a reading or a failure is reported: not
// the warning, the hub state, the CLI output or any returned error.
func TestClaudeTokenNeverLeaves(t *testing.T) {
	leaky := claudeAPIFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Get https://api.anthropic.com: Authorization: Bearer " + claudeSecret)
	})
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	source := claudeTestSource(spawn, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), leaky)
	if _, err := source.Refresh(t.Context()); err == nil || strings.Contains(err.Error(), claudeSecret) {
		t.Fatalf("err = %v", err)
	}
	client := quotaClient(t)
	client.spawnConfigPath = spawn
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": source}
	var warnings []string
	client.warn = func(m string) { warnings = append(warnings, m) }
	client.refreshFleetQuota(t.Context(), "claude", map[string]fleetQuotaSent{})
	if strings.Contains(strings.Join(warnings, "\n"), claudeSecret) {
		t.Fatal("token in a warning")
	}
	quotaCLIPrevious := quotaCLISource
	t.Cleanup(func() { quotaCLISource = quotaCLIPrevious })
	quotaCLISource = func(string) *nativeQuotaSource { return source }
	var out, errOut bytes.Buffer
	runQuotaCLI([]string{"refresh", "claude"}, &out, &errOut)
	if strings.Contains(out.String()+errOut.String(), claudeSecret) {
		t.Fatal("token in the CLI output")
	}
}
