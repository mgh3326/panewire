package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// agy is in shadow too: its default source would read this host's real
	// fleet config and, with an account configured there, its real app and
	// login.
	client.nativeQuota["agy"] = &nativeQuotaSource{Pool: "agy", AgyAccount: func() (string, bool) { return "", false }}
}

// setDevinSwitch writes this host's fleet config with devin listed (native) or
// not (shadow) in `[quota].native_pools` and points the node at it.
func setDevinSwitch(t *testing.T, native bool) {
	t.Helper()
	body := "[quota]\nfresh_ttl_s = 300\n"
	if native {
		body = "[quota]\nnative_pools = [\"devin\"]\n"
	}
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", path)
}

// devinPacelever is a pacelever fake that answers `refresh devin` with output
// (or fails) and `--json` with status, and records every call.
type devinPacelever struct {
	mu     sync.Mutex
	calls  []string
	output []byte
	status string
}

func (p *devinPacelever) run(_ context.Context, args ...string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, strings.Join(args, " "))
	switch {
	case len(args) == 2 && args[0] == "refresh" && args[1] == "devin" && p.output != nil:
		return p.output, nil
	case len(args) == 1 && args[0] == "--json":
		return []byte(p.status), nil
	}
	return nil, errors.New("exit status 2")
}

func (p *devinPacelever) called() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
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

// devinShadowLine waits for the one devin comparison line.
func devinShadowLine(t *testing.T, w *warnLog) string {
	t.Helper()
	line := func() []string {
		var lines []string
		for _, m := range w.all() {
			if strings.HasPrefix(m, "quota shadow devin:") {
				lines = append(lines, m)
			}
		}
		return lines
	}
	eventually(t, 5*time.Second, "the devin shadow comparison line", func() bool { return len(line()) > 0 })
	lines := line()
	if len(lines) != 1 {
		t.Fatalf("shadow lines %q", lines)
	}
	return lines[0]
}

func noStoreCalls(t *testing.T, hk *claudeHK) {
	t.Helper()
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if len(hk.calls) != 0 {
		t.Fatalf("the shared store was touched: %v", hk.calls)
	}
}

// Acceptance 2, shadow: the fleet-state loop still runs pacelever for devin and
// publishes its reading; the node's own measurement runs beside it, writes one
// comparison line, and touches neither the store nor the hub.
func TestFleetQuotaDevinNativeRunsNoPacelever(t *testing.T) {
	setDevinSwitch(t, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	var web []*http.Request
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), func(r *http.Request) (*http.Response, error) {
		web = append(web, r)
		return claudeAPIReply(200, devinWebBody(t))(r)
	})}
	pacelever := &devinPacelever{}
	warned := newWarnLog(client)

	client.refreshFleetQuota(t.Context(), "devin", map[string]fleetQuotaSent{})

	if calls := pacelever.called(); len(calls) != 0 || len(web) != 1 {
		t.Fatalf("pacelever=%v web=%d", calls, len(web))
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
	if lines := warned.all(); len(lines) != 0 {
		t.Fatalf("native mode logged %q", lines)
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
	setDevinSwitch(t, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.nativeQuota = map[string]*nativeQuotaSource{"devin": devinTestSource(client.spawnConfigPath, devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))}
	pacelever := &devinPacelever{}
	if result, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"}); failure != "" || result == nil {
		t.Fatalf("result %v failure %q", result, failure)
	}
	if calls := pacelever.called(); len(calls) != 0 || hk.published() != 1 {
		t.Fatalf("pacelever=%v published=%d", calls, hk.published())
	}
}

// Acceptance 6: a native host without a devin login takes no lease, starts no
// probe and sends no request; its refresh reports credentials.
func TestDevinNativeHostWithoutALoginTakesNoLease(t *testing.T) {
	setDevinSwitch(t, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
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
	pacelever := &devinPacelever{}
	if _, failure := client.nodeQuotaRefresh(t.Context(), nodeQuotaRefreshArgs{Pool: "devin"}); failure != "refresh_failed" {
		t.Fatalf("failure %q", failure)
	}
	publish, ok, kind := client.measureFleetQuota(t.Context(), t.Context(), "devin")
	if ok || kind != quota.ErrKindCredentials || publish.Key != "" {
		t.Fatalf("publish %+v ok=%t kind=%q, want credentials", publish, ok, kind)
	}
	noStoreCalls(t, hk)
	if probes != 0 || len(pacelever.called()) != 0 {
		t.Fatalf("probes %d, pacelever %v", probes, pacelever.called())
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
	source := devinTestSource(claudeStore(t, hk.server.URL), devinLoginFiles(true), func(*http.Request) (*http.Response, error) {
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
	source := devinTestSource(claudeStore(t, hk.server.URL), devinLoginFiles(true), claudeAPIReply(200, devinWebBody(t)))
	reading, err := source.Refresh(t.Context())
	if err != nil || reading.Free == nil || reading.Free.State != shared.FreeAvailable || len(reading.Buckets) != 2 {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
	if !strings.Contains(string(hk.pubs[0]), `"free_bucket":{"state":"available"}`) {
		t.Fatalf("published %s", hk.pubs[0])
	}
}

// Acceptance 2 at the CLI, which every spawn path outside the node daemon runs
// (`panewire quota refresh devin`, locally or over ssh): in shadow it is
// pacelever's; flipped, it runs no pacelever and answers from the node's own
// measurement.
func TestQuotaCLIRefreshDevinWithoutALoginReportsCredentials(t *testing.T) {
	setDevinSwitch(t, true)
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	quotaCLISource = func(string) *nativeQuotaSource {
		return devinTestSource(spawn, devinLoginFiles(false), func(*http.Request) (*http.Response, error) {
			t.Error("a request was sent without a login")
			return nil, errors.New("unexpected")
		})
	}
	var out, errout bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "devin"}, &out, &errout); code == 0 || !strings.Contains(errout.String(), "quota refresh: credentials") {
		t.Fatalf("refresh %d out=%q err=%q", code, out.String(), errout.String())
	}
	noStoreCalls(t, hk)
}
