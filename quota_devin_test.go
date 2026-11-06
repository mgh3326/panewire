package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// devinSecret is the bearer token of the test login. It may appear only in the
// Authorization header of the one web request.
const devinSecret = "devin-node-test-token-must-not-leak"

// devinFPForTest is the fingerprint of organization "org-fixture", computed
// with pacelever@f7a1825 (fingerprint("devin-account", "org-fixture")).
const devinFPForTest = "9651d01012552948"

var devinKeyForTest = "devin:" + devinFPForTest

func devinWebBody(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("internal", "quota", "devin", "testdata", "web_flat.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func devinLoginFiles(login bool) map[string]string {
	files := map[string]string{"/home/u/.config/pacelever/config.toml": "[pools.devin]\norg_id = 'org-fixture'\n"}
	if login {
		files["/home/u/.local/share/devin/credentials.toml"] = "windsurf_api_key = 'devin-api-key-must-not-leak'\n"
		files["/home/u/.config/devin/config.json"] = `{"devin":{"org_id":"org-fixture"}}`
	}
	return files
}

// devinTestSource is a devin measurement over fakes: the login files, the web
// endpoint, and a probe that must not run. The devin binary is never found.
func devinTestSource(spawnConfig string, files map[string]string, api claudeAPIFunc) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "devin", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return claudeTestNow }, Wait: 5 * time.Millisecond,
		Devin: devin.Config{
			HTTP: api, Home: "/home/u",
			LookupEnv: func(name string) (string, bool) {
				if name == "DEVIN_BEARER_TOKEN" {
					return devinSecret, true
				}
				return "", false
			},
			LookPath: func(string) (string, error) { return "", os.ErrNotExist },
			ReadFile: func(path string) ([]byte, error) {
				if body, ok := files[path]; ok {
					return []byte(body), nil
				}
				return nil, os.ErrNotExist
			},
			Hostname: func() (string, error) { return "host-a", nil }, UID: func() int { return 1000 },
			StartProbe: func(context.Context, string, string, []string) (devin.Terminal, error) {
				return nil, errors.New("no devin binary here")
			},
		},
	}
}

// isolateDevinShadow gives a client whose test uses devin as a pacelever pool a
// devin measurement over fakes with no login. In shadow the node measures the
// pool beside pacelever, and the default source would read this host's real
// login and start its real devin: every test that refreshes devin through
// pacelever must set a source first.
func isolateDevinShadow(client *HubClient) {
	if client.nativeQuota == nil {
		client.nativeQuota = map[string]*nativeQuotaSource{}
	}
	client.nativeQuota["devin"] = devinTestSource("", devinLoginFiles(false), func(*http.Request) (*http.Response, error) {
		return nil, errors.New("no network in tests")
	})
}

// devinSpawnConfig writes spawn.json for the store at url, with devin listed
// in quota_native when native.
func devinSpawnConfig(t *testing.T, url string, native bool) string {
	t.Helper()
	dir := t.TempDir()
	env := filepath.Join(dir, "hk.env")
	if err := os.WriteFile(env, []byte("HANDOFFKEEP_URL="+url+"\nHANDOFFKEEP_TOKEN=hk-node-test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	listed := ""
	if native {
		listed = `,"quota_native":["devin"]`
	}
	spawn := filepath.Join(dir, "spawn.json")
	if err := os.WriteFile(spawn, []byte(`{"enabled":false,"handoffkeep_env":"`+env+`"`+listed+`}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return spawn
}

// devinPaceleverOutput is `pacelever refresh devin` for the web_flat fixture:
// what pacelever answers for the same account.
func devinPaceleverOutput(used1d, used7d float64) []byte {
	return []byte(fmt.Sprintf(`{"id":"devin","source":"usage-api","fetched_at":%d,"publish_fp":%q,"publish_fp_kind":"account","buckets":[`+
		`{"window":"1d","used_pct":%v,"resets_at":"2026-11-07T00:00:00Z","scope":{"kind":"account"}},`+
		`{"window":"7d","used_pct":%v,"resets_at":"2026-11-12T16:13:20Z","scope":{"kind":"account"}}]}`, claudeTestNow.Unix(), devinFPForTest, used1d, used7d))
}

// devinPaceleverOutputWithFreeTag is the same with the per-model Free tag
// pacelever adds from `devin models list`: a bucket with no window, which the
// hub publish refuses but the comparison must read past.
func devinPaceleverOutputWithFreeTag() []byte {
	out := string(devinPaceleverOutput(25, 55))
	return []byte(strings.TrimSuffix(out, "]}") + `,{"window":"","used_pct":0,"scope":{"kind":"model","name":"swe-2-high"}}]}`)
}

// paceleverCalls is a pacelever fake that records its calls and answers refresh
// with output.
func paceleverCalls(calls *[]string, mu *sync.Mutex, output []byte, err error) relayCommandRunner {
	return func(_ context.Context, args ...string) ([]byte, error) {
		mu.Lock()
		*calls = append(*calls, strings.Join(args, " "))
		mu.Unlock()
		return output, err
	}
}

// logLines captures slog output for the test and returns it on demand.
type logLines struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func captureSlog(t *testing.T) *logLines {
	t.Helper()
	lines := &logLines{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(lines, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return lines
}

func shadowIdle(client *HubClient) bool {
	client.shadowMu.Lock()
	defer client.shadowMu.Unlock()
	return len(client.shadowRunning) == 0
}

func compareLine(lines *logLines) string {
	for _, line := range strings.Split(lines.String(), "\n") {
		if strings.Contains(line, "quota shadow compare") {
			return line
		}
	}
	return ""
}

func TestQuotaSwitchedNative(t *testing.T) {
	write := func(t *testing.T, body string) string {
		path := filepath.Join(t.TempDir(), "spawn.json")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, test := range []struct {
		name, body, pool string
		want             bool
	}{
		{"listed", `{"enabled":true,"quota_native":["devin"]}`, "devin", true},
		{"listed among others", `{"quota_native":["kimi","devin"]}`, "devin", true},
		{"absent key is shadow", `{"enabled":true}`, "devin", false},
		{"empty list is shadow", `{"quota_native":[]}`, "devin", false},
		{"another pool listed", `{"quota_native":["claude"]}`, "devin", false},
		{"malformed is shadow", `{"quota_native":["devin"`, "devin", false},
		{"wrong type is shadow", `{"quota_native":"devin"}`, "devin", false},
		{"a pool without a switch cannot be flipped", `{"quota_native":["kimi"]}`, "kimi", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := quotaSwitchedNative(write(t, test.body), test.pool); got != test.want {
				t.Fatalf("quotaSwitchedNative = %t, want %t", got, test.want)
			}
		})
	}
	if quotaSwitchedNative("", "devin") || quotaSwitchedNative(filepath.Join(t.TempDir(), "missing.json"), "devin") {
		t.Fatal("a missing spawn.json flipped the switch")
	}
	if quotaSwitchedNative(t.TempDir(), "devin") {
		t.Fatal("a directory flipped the switch")
	}
}

// The strict spawn.json loader knows the switch, so listing devin there does
// not disable hub spawns.
func TestSpawnConfigAcceptsQuotaNative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spawn.json")
	body := `{"enabled":true,"workspace":"w","herdr_session":"s","quota_native":["devin"]}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := loadHubSpawnConfig(path)
	if err != nil || len(config.QuotaNative) != 1 || config.QuotaNative[0] != "devin" {
		t.Fatalf("config %+v err=%v", config, err)
	}
}

// Acceptance 2, shadow: the fleet-state loop still runs pacelever for devin and
// publishes its reading; the node's own measurement runs beside it, writes one
// comparison line, and touches neither the store nor the hub.
func TestFleetQuotaDevinShadowKeepsPaceleverAndLogsTheComparison(t *testing.T) {
	lines := captureSlog(t)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, hk.server.URL, false)
	var web []*http.Request
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), func(r *http.Request) (*http.Response, error) {
		web = append(web, r)
		return claudeAPIReply(200, devinWebBody(t))(r)
	})}
	var pacelever []string
	var mu sync.Mutex
	client.nodePacelever = paceleverCalls(&pacelever, &mu, devinPaceleverOutput(25, 55), nil)

	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
	eventually(t, 5*time.Second, "the shadow comparison line", func() bool { return compareLine(lines) != "" })
	eventually(t, 5*time.Second, "the shadow comparison to finish", func() bool { return shadowIdle(client) })

	mu.Lock()
	if len(pacelever) == 0 || pacelever[0] != "refresh devin" {
		t.Fatalf("pacelever calls %v, want refresh devin", pacelever)
	}
	mu.Unlock()
	line := compareLine(lines)
	for _, want := range []string{"verdict=match", "account=same", "pool=devin", "native_source=usage-api",
		`native_buckets="1d=25@2026-11-07T00:00:00Z,7d=55@2026-11-12T16:13:20Z"`, `pacelever_buckets="1d=25@2026-11-07T00:00:00Z,7d=55@2026-11-12T16:13:20Z"`} {
		if !strings.Contains(line, want) {
			t.Errorf("comparison line misses %q:\n%s", want, line)
		}
	}
	if len(web) != 1 || web[0].Header.Get("Authorization") != "Bearer "+devinSecret {
		t.Fatalf("web requests %v", web)
	}
	hk.mu.Lock()
	calls := hk.calls
	hk.mu.Unlock()
	if len(calls) != 0 {
		t.Fatalf("shadow touched the shared store: %v", calls)
	}
	// pacelever's reading is what the hub got.
	select {
	case event := <-client.events:
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil || publish.Key != devinKeyForTest || publish.Quota.Source != "usage-api" {
			t.Fatalf("publish %+v err=%v", publish, err)
		}
	default:
		t.Fatal("pacelever's reading was not published")
	}
	if strings.Contains(lines.String(), devinSecret) {
		t.Fatal("token in the log")
	}
}

// pacelever's reading with its per-model Free tag is refused by the hub publish
// (a bucket without a window), yet the comparison still judges the paid
// buckets next to it.
func TestDevinShadowReadsPastTheFreeTag(t *testing.T) {
	lines := captureSlog(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, "http://127.0.0.1:1", false)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))}
	var pacelever []string
	var mu sync.Mutex
	client.nodePacelever = paceleverCalls(&pacelever, &mu, devinPaceleverOutputWithFreeTag(), nil)
	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
	eventually(t, 5*time.Second, "the shadow comparison line", func() bool { return compareLine(lines) != "" })
	if line := compareLine(lines); !strings.Contains(line, "verdict=match") || strings.Contains(line, "swe-2-high") {
		t.Fatalf("comparison line:\n%s", line)
	}
}

// The same in shadow through the quota.refresh node command: a spawn's refresh
// answers from pacelever and is compared in the background.
func TestNodeQuotaRefreshDevinShadowRunsPacelever(t *testing.T) {
	lines := captureSlog(t)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, hk.server.URL, false)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))}
	var pacelever []string
	var mu sync.Mutex
	client.nodePacelever = paceleverCalls(&pacelever, &mu, devinPaceleverOutput(25, 70), nil)

	result, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"})
	if failure != "" || result == nil {
		t.Fatalf("result %v failure %q", result, failure)
	}
	eventually(t, 5*time.Second, "the shadow comparison line", func() bool { return compareLine(lines) != "" })
	mu.Lock()
	if len(pacelever) != 1 || pacelever[0] != "refresh devin" {
		t.Fatalf("pacelever calls %v", pacelever)
	}
	mu.Unlock()
	if line := compareLine(lines); !strings.Contains(line, "verdict=differ") || !strings.Contains(line, "diffs=used:account//7d") {
		t.Fatalf("a 7d reading of 70 against 55 must differ:\n%s", line)
	}
}

// A failed pacelever refresh in shadow still answers refresh_failed, and the
// comparison names pacelever's error kind (asked of its status off the caller's
// path) next to the node's measurement.
func TestNodeQuotaRefreshDevinShadowWithFailingPacelever(t *testing.T) {
	lines := captureSlog(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, "http://127.0.0.1:1", false)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))}
	client.nodePacelever = func(_ context.Context, args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "--json" {
			return []byte(`{"providers":[{"id":"devin","error_kind":"probe_error"}]}`), nil
		}
		return nil, errors.New("exit status 1")
	}
	if _, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"}); failure != "refresh_failed" {
		t.Fatalf("failure %q", failure)
	}
	eventually(t, 5*time.Second, "the shadow comparison line", func() bool { return compareLine(lines) != "" })
	line := compareLine(lines)
	if !strings.Contains(line, "verdict=pacelever_error") || !strings.Contains(line, "pacelever_error_kind=probe_error") || !strings.Contains(line, "native_buckets=") {
		t.Fatalf("comparison line:\n%s", line)
	}
}

// A host without a devin login is not measured in shadow either: no web
// request, no probe, no comparison line.
func TestDevinShadowSkipsAHostWithoutALogin(t *testing.T) {
	lines := captureSlog(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, "http://127.0.0.1:1", false)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(false), func(*http.Request) (*http.Response, error) {
		t.Error("a web request was sent from a host without a login")
		return nil, errors.New("unexpected")
	})}
	var pacelever []string
	var mu sync.Mutex
	client.nodePacelever = paceleverCalls(&pacelever, &mu, nil, errors.New("no login"))
	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
	eventually(t, 5*time.Second, "the shadow comparison to finish", func() bool { return shadowIdle(client) })
	if compareLine(lines) != "" {
		t.Fatalf("a comparison line without a login:\n%s", lines.String())
	}
	if !strings.Contains(lines.String(), "quota shadow skipped") {
		t.Fatalf("the skip is not logged:\n%s", lines.String())
	}
}

// One comparison per pool runs at a time.
func TestDevinShadowRunsOneComparisonAtATime(t *testing.T) {
	captureSlog(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, "http://127.0.0.1:1", false)
	release := make(chan struct{})
	var requests int
	var mu sync.Mutex
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		requests++
		mu.Unlock()
		<-release
		return claudeAPIReply(200, devinWebBody(t))(r)
	})}
	pace := shadowPacelever{ok: true}
	client.shadowQuota("devin", pace)
	eventually(t, 5*time.Second, "the first comparison to start", func() bool { mu.Lock(); defer mu.Unlock(); return requests == 1 })
	client.shadowQuota("devin", pace) // dropped: one is in flight
	close(release)
	eventually(t, 5*time.Second, "the comparison to finish", func() bool { return shadowIdle(client) })
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("%d measurements for two requests, want the second dropped", requests)
	}
}

// Acceptance 2, native: spawn.json lists devin, so the loop measures it in this
// process — the shared store, the hub publish — and never execs pacelever.
func TestFleetQuotaDevinNativeRunsNoPacelever(t *testing.T) {
	lines := captureSlog(t)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, hk.server.URL, true)
	var web []*http.Request
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), func(r *http.Request) (*http.Response, error) {
		web = append(web, r)
		return claudeAPIReply(200, devinWebBody(t))(r)
	})}
	var pacelever []string
	client.nodePacelever = func(_ context.Context, args ...string) ([]byte, error) {
		pacelever = append(pacelever, strings.Join(args, " "))
		t.Errorf("pacelever was run in native mode: %v", args)
		return nil, errors.New("unexpected")
	}

	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})

	if len(pacelever) != 0 || len(web) != 1 {
		t.Fatalf("pacelever=%v web=%d", pacelever, len(web))
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	hk.mu.Lock()
	var observation struct {
		Kind       string `json:"kind"`
		Plan       string `json:"plan"`
		FreeBucket struct {
			State string `json:"state"`
		} `json:"free_bucket"`
		Buckets []struct {
			Window  string  `json:"window"`
			UsedPct float64 `json:"used_pct"`
		} `json:"buckets"`
	}
	err := json.Unmarshal(hk.pubs[0], &observation)
	calls := strings.Join(hk.calls, "\n")
	hk.mu.Unlock()
	if err != nil || observation.Kind != "auto" || observation.Plan != "Teams" || len(observation.Buckets) != 2 || observation.FreeBucket.State != "available" {
		t.Fatalf("stored observation %+v err=%v", observation, err)
	}
	if !strings.Contains(calls, "/v1/quota/accounts/"+devinKeyForTest+"/") {
		t.Fatalf("store calls\n%s", calls)
	}
	select {
	case event := <-client.events:
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil {
			t.Fatal(err)
		}
		if publish.Key != devinKeyForTest || publish.Quota.Pool != "devin" || publish.Quota.Source != "handoffkeep" || len(publish.Quota.Buckets) != 2 {
			t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
		}
		if strings.Contains(string(event.Payload), devinSecret) {
			t.Fatal("token in the published state")
		}
	default:
		t.Fatal("nothing was published to the hub")
	}
	if got := poolLogin(t, client, "devin"); got.Login != fleet.ReadinessLoginPresent || got.Account != devinKeyForTest {
		t.Fatalf("login %+v", got)
	}
	if strings.Contains(lines.String(), "quota shadow") {
		t.Fatalf("native mode compared with pacelever:\n%s", lines.String())
	}
	// The next round reads the shared reading: nothing is measured again.
	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
	if len(web) != 1 {
		t.Fatalf("a fresh shared reading was measured again: %d requests", len(web))
	}
}

// Native through the quota.refresh node command: no pacelever, the store
// flow's reading.
func TestNodeQuotaRefreshDevinNativeRunsNoPacelever(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, hk.server.URL, true)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))}
	client.nodePacelever = func(_ context.Context, args ...string) ([]byte, error) {
		t.Errorf("pacelever was run in native mode: %v", args)
		return nil, errors.New("unexpected")
	}
	if result, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"}); failure != "" || result == nil {
		t.Fatalf("result %v failure %q", result, failure)
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}
}

// Acceptance 6: a host without a devin login — shadow or native — takes no
// lease, starts no probe and sends no request; its refresh reports credentials.
func TestDevinNativeHostWithoutALoginTakesNoLease(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = devinSpawnConfig(t, hk.server.URL, true)
	source := devinTestSource(client.spawnConfigPath, devinLoginFiles(false), func(*http.Request) (*http.Response, error) {
		t.Error("a web request was sent from a host without a login")
		return nil, errors.New("unexpected")
	})
	probes := 0
	source.Devin.StartProbe = func(context.Context, string, string, []string) (devin.Terminal, error) {
		probes++
		return nil, errors.New("unexpected")
	}
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": source}
	client.nodePacelever = func(_ context.Context, args ...string) ([]byte, error) {
		t.Errorf("pacelever was run in native mode: %v", args)
		return nil, errors.New("unexpected")
	}
	if _, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"}); failure != "refresh_failed" {
		t.Fatalf("failure %q", failure)
	}
	publish, ok, kind := client.measureFleetQuota(t.Context(), t.Context(), "devin")
	if ok || kind != quota.ErrKindCredentials || publish.Key != "" {
		t.Fatalf("publish %+v ok=%t kind=%q, want credentials", publish, ok, kind)
	}
	hk.mu.Lock()
	calls := hk.calls
	hk.mu.Unlock()
	if len(calls) != 0 || probes != 0 {
		t.Fatalf("store calls %v, probes %d: a host without a login must not touch the store or the TUI", calls, probes)
	}
	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})
	if got := poolLogin(t, client, "devin"); got.Login != fleet.ReadinessLoginMissing {
		t.Fatalf("login %+v, want missing", got)
	}
}

// The lease wait of a devin refresh is bounded: with another host holding the
// lease, the node's refresh gives up with shared_pending at devinLeaseWait
// instead of the flow's two lease windows.
func TestDevinNativeRefreshDoesNotWaitPastTheBound(t *testing.T) {
	hk := newClaudeHK(t)
	hk.foreignHolder = "other-host"
	previous := devinLeaseWait
	devinLeaseWait = 40 * time.Millisecond
	t.Cleanup(func() { devinLeaseWait = previous })
	source := devinTestSource(devinSpawnConfig(t, hk.server.URL, true), devinLoginFiles(true), func(*http.Request) (*http.Response, error) {
		t.Error("measured although another host holds the lease")
		return nil, errors.New("unexpected")
	})
	start := time.Now()
	_, err := source.Refresh(t.Context())
	if quota.KindOf(err) != quota.ErrKindSharedPending {
		t.Fatalf("err = %v, want shared_pending", err)
	}
	// An upper bound only: the lease is held for a minute, the bound is 40 ms.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("refresh waited %s past a 40 ms bound", elapsed)
	}
	if hk.published() != 0 {
		t.Fatal("published although another host held the lease")
	}
}

// Free-tier state travels with the reading: the store keeps what this host
// published and a second refresh reuses it.
func TestDevinFreeBucketIsPublishedAndRead(t *testing.T) {
	hk := newClaudeHK(t)
	source := devinTestSource(devinSpawnConfig(t, hk.server.URL, true), devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))
	reading, err := source.Refresh(t.Context())
	if err != nil || reading.Free == nil || reading.Free.State != shared.FreeAvailable || len(reading.Buckets) != 2 {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
	if !strings.Contains(string(hk.pubs[0]), `"free_bucket":{"state":"available"}`) {
		t.Fatalf("published %s", hk.pubs[0])
	}
}

// Acceptance 2 at the CLI, which every spawn path outside the node daemon runs
// (`panewire quota refresh devin`, locally or over ssh): in shadow it runs
// pacelever; in native it runs no pacelever and answers from the node's own
// measurement.
func TestQuotaCLIRefreshDevinShadowRunsPacelever(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pacelever-ran")
	quotaFake(t, `echo "$@" > `+marker+`; echo refreshed`)
	hk := newClaudeHK(t)
	spawn := devinSpawnConfig(t, hk.server.URL, false)
	previous := quotaCLISource
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return devinTestSource(spawn, devinLoginFiles(true), func(*http.Request) (*http.Response, error) {
			t.Error("the CLI measured natively in shadow")
			return nil, errors.New("unexpected")
		})
	}
	t.Cleanup(func() { quotaCLISource = previous })
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "devin"}, &out, &errout); code != 0 || out.String() != "refreshed\n" {
		t.Fatalf("refresh %d %q %s", code, out.String(), errout.String())
	}
	ran, err := os.ReadFile(marker)
	if err != nil || strings.TrimSpace(string(ran)) != "refresh devin" {
		t.Fatalf("pacelever ran with %q (%v)", ran, err)
	}
	if hk.published() != 0 {
		t.Fatal("the shadow CLI published to the store")
	}
}

func TestQuotaCLIRefreshDevinNativeRunsNoPacelever(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "pacelever-ran")
	quotaFake(t, `echo "$@" > `+marker+`; echo refreshed`)
	hk := newClaudeHK(t)
	spawn := devinSpawnConfig(t, hk.server.URL, true)
	previous := quotaCLISource
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return devinTestSource(spawn, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))
	}
	t.Cleanup(func() { quotaCLISource = previous })
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "devin"}, &out, &errout); code != 0 {
		t.Fatalf("refresh %d %q %s", code, out.String(), errout.String())
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("pacelever ran in native mode")
	}
	var reading quotaReading
	if err := json.Unmarshal(out.Bytes(), &reading); err != nil || reading.Pool != "devin" || reading.Account != devinFPForTest || len(reading.Buckets) != 2 {
		t.Fatalf("reading %+v err=%v out=%s", reading, err, out.String())
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}
}

// A CLI on a host without a devin login says why instead of waiting.
func TestQuotaCLIRefreshDevinWithoutALoginReportsCredentials(t *testing.T) {
	quotaFake(t, `echo pacelever must not run; exit 9`)
	hk := newClaudeHK(t)
	spawn := devinSpawnConfig(t, hk.server.URL, true)
	previous := quotaCLISource
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return devinTestSource(spawn, devinLoginFiles(false), func(*http.Request) (*http.Response, error) {
			t.Error("a request was sent without a login")
			return nil, errors.New("unexpected")
		})
	}
	t.Cleanup(func() { quotaCLISource = previous })
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "devin"}, &out, &errout); code == 0 || !strings.Contains(errout.String(), "quota refresh: credentials") {
		t.Fatalf("refresh %d out=%q err=%q", code, out.String(), errout.String())
	}
	hk.mu.Lock()
	calls := hk.calls
	hk.mu.Unlock()
	if len(calls) != 0 {
		t.Fatalf("store calls %v", calls)
	}
}

func TestCompareQuotaShadow(t *testing.T) {
	paceBuckets := func(used7d float64, reset string) shadowPacelever {
		return shadowPacelever{ok: true, publish: FleetStatePublish{Key: devinKeyForTest, Quota: &FleetStateQuota{Buckets: []FleetStateBucket{
			{Window: "7d", UsedPct: &used7d, ResetsAt: reset, Scope: "account"},
			{Window: "", UsedPct: &used7d, Scope: "model", Model: "swe-2-high"}, // a free tag
		}}}}
	}
	reset := time.Date(2026, 11, 12, 16, 0, 0, 0, time.UTC)
	native := func(key string, used float64, at *time.Time) shared.Reading {
		return shared.Reading{Key: key, Buckets: []shared.Bucket{{Window: "7d", UsedPct: used, ResetsAt: at, Scope: "account"}}}
	}
	for _, test := range []struct {
		name       string
		pace       shadowPacelever
		native     shared.Reading
		nativeErr  error
		verdict    string
		diffs      string
		accountVal string
	}{
		{"same", paceBuckets(62, "2026-11-12T16:00:00Z"), native(devinKeyForTest, 62, &reset), nil, "match", "", "same"},
		{"within tolerance", paceBuckets(62, "2026-11-12T16:00:00Z"), native(devinKeyForTest, 62.9, ptrTime(reset.Add(4*time.Minute))), nil, "match", "", "same"},
		{"used differs", paceBuckets(62, "2026-11-12T16:00:00Z"), native(devinKeyForTest, 64, &reset), nil, "differ", "used:account//7d", "same"},
		{"reset differs", paceBuckets(62, "2026-11-12T16:00:00Z"), native(devinKeyForTest, 62, ptrTime(reset.Add(time.Hour))), nil, "differ", "reset:account//7d", "same"},
		{"reset missing on one side", paceBuckets(62, ""), native(devinKeyForTest, 62, &reset), nil, "differ", "reset:account//7d", "same"},
		{"other account", paceBuckets(62, "2026-11-12T16:00:00Z"), native("devin:0000000000000000", 62, &reset), nil, "differ", "account", "different"},
		{"native error", paceBuckets(62, ""), shared.Reading{}, quota.Fail(quota.ErrKindProbe, "x"), "native_error", "", "unknown"},
		{"pacelever error", shadowPacelever{}, native(devinKeyForTest, 62, &reset), nil, "pacelever_error", "", "unknown"},
		{"both error", shadowPacelever{}, shared.Reading{}, quota.Fail(quota.ErrKindProbe, "x"), "both_error", "", "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := compareQuotaShadow(test.pace, test.native, test.nativeErr)
			if got.Verdict != test.verdict || strings.Join(got.Diffs, ",") != test.diffs || got.Account != test.accountVal {
				t.Fatalf("comparison %+v, want %s %q %s", got, test.verdict, test.diffs, test.accountVal)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
