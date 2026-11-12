package panewire

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/commandcode"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// commandcodeSecret is the API key of the test login. It may appear only in
// the Authorization header of a request.
const commandcodeSecret = "commandcode-node-test-key-must-not-leak"

// commandcodeFPForTest is hex(sha256("commandcode-account|user_fixture_1"))[:16],
// the fingerprint of pacelever's accountFP("commandcode", "user_fixture_1")
// (account.go: fingerprint(pool+"-account", account)), computed with sha256sum.
const commandcodeFPForTest = "71430d9ff98e30c3"

var commandcodeKeyForTest = "commandcode:" + commandcodeFPForTest

// The 2026-11-06 response shapes (secrets and ids removed): plan individual-goat,
// 6.52 of 70 USD spent this billing period, weekly 0.45 of 35, 5h 0 of 14.
func commandcodeReplies(now time.Time) map[string]string {
	return map[string]string{
		"/alpha/whoami":                `{"success":true,"data":{"user":{"id":"user_fixture_1"},"org":{"id":"org_fixture_1"}}}`,
		"/alpha/billing/credits":       fmt.Sprintf(`{"success":true,"data":{"windowLimits":{"fiveHour":{"used":0,"cap":14,"resetAt":%d},"weekly":{"used":0.45,"cap":35,"resetAt":%d}},"credits":{"monthlyCredits":63.48,"purchasedCredits":0,"freeCredits":0}}}`, now.Add(4*time.Hour).UnixMilli(), now.Add(6*24*time.Hour).UnixMilli()),
		"/alpha/usage/summary":         `{"success":true,"data":{"totalMonthlyCredits":6.52,"periodBasis":"billing-period"}}`,
		"/alpha/billing/subscriptions": `{"success":true,"data":{"planId":"individual-goat","status":"active","currentPeriodEnd":"2026-11-27T07:38:00.000Z"}}`,
	}
}

// commandcodeAPI serves replies by path and counts the requests.
type commandcodeAPI struct {
	mu       sync.Mutex
	replies  map[string]string
	requests []string
	headers  []http.Header
}

func newCommandcodeAPI() *commandcodeAPI {
	return &commandcodeAPI{replies: commandcodeReplies(claudeTestNow)}
}

func (a *commandcodeAPI) Do(r *http.Request) (*http.Response, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, r.URL.Path)
	a.headers = append(a.headers, r.Header.Clone())
	body, ok := a.replies[r.URL.Path]
	if !ok {
		return nil, errors.New("no reply for " + r.URL.Path)
	}
	return claudeAPIReply(200, body)(r)
}

func (a *commandcodeAPI) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests)
}

func commandcodeLoginFiles(key string) map[string]string {
	if key == "" {
		return map[string]string{}
	}
	return map[string]string{"/home/u/.commandcode/auth.json": `{"apiKey":"` + key + `"}`}
}

// commandcodeTestSource is a commandcode measurement over fakes: the login
// file and the API.
func commandcodeTestSource(spawnConfig string, files map[string]string, api commandcode.Doer) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "commandcode", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return claudeTestNow }, Wait: 5 * time.Millisecond,
		Commandcode: commandcode.Config{
			HTTP: api, Home: "/home/u", LookupEnv: func(string) (string, bool) { return "", false },
			ReadFile: func(path string) ([]byte, error) {
				if body, ok := files[path]; ok {
					return []byte(body), nil
				}
				return nil, os.ErrNotExist
			},
		},
	}
}

// setCommandcodeSwitch writes this host's fleet config with commandcode listed
// (native) or not (shadow) and points the node at it.
func setCommandcodeSwitch(t *testing.T, native bool) {
	t.Helper()
	body := "[quota]\nfresh_ttl_s = 300\n"
	if native {
		body = "[quota]\nnative_pools = [\"commandcode\"]\n"
	}
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", path)
}

// paceleverRunnerFake answers `pacelever refresh commandcode` with output and
// `pacelever --json` with status, and records every call.
type paceleverRunnerFake struct {
	mu     sync.Mutex
	calls  []string
	output string
	status string
	// pool is the pool whose refresh it answers; empty is commandcode.
	pool string
}

func (p *paceleverRunnerFake) run(_ context.Context, args ...string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, strings.Join(args, " "))
	switch {
	case len(args) == 2 && args[0] == "refresh" && args[1] == cmp.Or(p.pool, "commandcode") && p.output != "":
		return []byte(p.output), nil
	case len(args) == 1 && args[0] == "--json":
		return []byte(p.status), nil
	}
	return nil, errors.New("exit status 2")
}

func (p *paceleverRunnerFake) called() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// paceleverCommandcodeOutput is pacelever's reading of the same account on the
// same day: it reads 5h and 7d and, with no planId in the response, no 30d.
func paceleverCommandcodeOutput() string {
	return fmt.Sprintf(`{"id":"commandcode","buckets":[{"label":"5h","window":"5h","used_pct":0,"resets_at":%q,"scope":{"kind":"account"}},{"label":"7d","window":"7d","used_pct":1.2857142857142856,"resets_at":%q,"scope":{"kind":"account"}},{"label":"credits","window":"credit","used_pct":null,"scope":{"kind":"account"}}],"source":"usage-api","stale":false,"fetched_at":%d,"account_fp":%q,"account_fp_kind":"account"}`,
		claudeTestNow.Add(4*time.Hour).UTC().Format(time.RFC3339Nano), claudeTestNow.Add(6*24*time.Hour).UTC().Format(time.RFC3339Nano), claudeTestNow.Unix(), commandcodeFPForTest)
}

func paceleverCommandcodeStatus(kind string) string {
	return `{"providers":[{"id":"commandcode","status":"error","error_kind":"` + kind + `"}]}`
}

// warnLog collects a client's warnings; the shadow comparison writes its line
// from a background goroutine.
type warnLog struct {
	mu  sync.Mutex
	log []string
}

func newWarnLog(client *HubClient) *warnLog {
	w := &warnLog{}
	client.warn = func(m string) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.log = append(w.log, m)
	}
	return w
}

func (w *warnLog) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.log...)
}

// shadowLine waits for the one comparison line of a refresh and returns it.
func shadowLine(t *testing.T, w *warnLog) string {
	t.Helper()
	eventually(t, 5*time.Second, "the shadow comparison line", func() bool { return len(shadowLines(w.all())) > 0 })
	lines := shadowLines(w.all())
	if len(lines) != 1 {
		t.Fatalf("shadow lines %q (all %q)", lines, w.all())
	}
	return lines[0]
}

func shadowLines(warnings []string) []string {
	var lines []string
	for _, w := range warnings {
		if strings.HasPrefix(w, "quota shadow commandcode:") {
			lines = append(lines, w)
		}
	}
	return lines
}

// Native mode, acceptance 2: with the switch flipped the fleet-state loop
// measures commandcode itself — shared store, hub publish, 30d included — and
// never runs pacelever for it.
func TestFleetQuotaCommandcodeNativeNeverRunsPacelever(t *testing.T) {
	setCommandcodeSwitch(t, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	api := newCommandcodeAPI()
	source := commandcodeTestSource(client.spawnConfigPath, commandcodeLoginFiles(commandcodeSecret), api)
	client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": source}
	pacelever := &paceleverRunnerFake{}
	warned := newWarnLog(client)

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "commandcode", sent)

	if calls := pacelever.called(); len(calls) != 0 {
		t.Fatalf("pacelever was run for commandcode in native mode: %v", calls)
	}
	if got := warned.all(); len(got) != 0 {
		t.Fatalf("warnings %q", got)
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	hk.mu.Lock()
	var observation struct {
		Kind    string `json:"kind"`
		Plan    string `json:"plan"`
		Buckets []struct {
			Window   string  `json:"window"`
			UsedPct  float64 `json:"used_pct"`
			ResetsAt string  `json:"resets_at"`
		} `json:"buckets"`
	}
	err := json.Unmarshal(hk.pubs[0], &observation)
	calls := strings.Join(hk.calls, "\n")
	hk.mu.Unlock()
	if err != nil || observation.Kind != "auto" || observation.Plan != "individual-goat" || len(observation.Buckets) != 3 {
		t.Fatalf("stored observation %+v err=%v", observation, err)
	}
	// Order: 5h, 7d, 30d. 30d resets at currentPeriodEnd.
	got := map[string]float64{}
	for _, b := range observation.Buckets {
		got[b.Window] = b.UsedPct
	}
	if got["5h"] != 0 || int(got["7d"]*10) != 12 || int(got["30d"]*10) != 93 || observation.Buckets[2].Window != "30d" || observation.Buckets[2].ResetsAt != "2026-11-27T07:38:00Z" {
		t.Fatalf("buckets %+v", observation.Buckets)
	}
	if !strings.Contains(calls, "/v1/quota/accounts/"+commandcodeKeyForTest+"/") || !strings.Contains(calls, "DELETE /v1/quota/accounts/"+commandcodeKeyForTest+"/lease") {
		t.Fatalf("the lease was not released under the account key:\n%s", calls)
	}
	select {
	case event := <-client.events:
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil {
			t.Fatal(err)
		}
		if publish.Key != commandcodeKeyForTest || publish.Quota == nil || publish.Quota.Pool != "commandcode" || publish.Quota.Source != "handoffkeep" || len(publish.Quota.Buckets) != 3 {
			t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
		}
		if strings.Contains(string(event.Payload), commandcodeSecret) {
			t.Fatal("key in the published state")
		}
	default:
		t.Fatal("nothing was published to the hub")
	}

	// The next round reads the shared reading: no new measurement. whoami is
	// remembered, so a fresh round costs no request at all.
	before := api.count()
	client.refreshFleetQuota(t.Context(), "commandcode", sent)
	if api.count() != before {
		t.Fatalf("a fresh shared reading cost %d requests", api.count()-before)
	}
	for _, header := range api.headers {
		if header.Get("Authorization") != "Bearer "+commandcodeSecret || header.Get("User-Agent") != "cmd/1.88.1" {
			t.Fatalf("request headers %v", header)
		}
	}
}

// hk 3320: mac-dev ran commandcode natively with a working login and was never
// offered the account, because presence followed only a published reading. The
// loop on a host whose measurement fails (here a 5h window the API reports as
// already over) still tells the store that this host holds the account, so
// placement can try cmd-dsflash41 there and the quota gate re-measures it.
func TestFleetQuotaCommandcodeNativePublishesPresenceWhenMeasurementFails(t *testing.T) {
	setCommandcodeSwitch(t, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.machineID = "node-a"
	api := newCommandcodeAPI()
	api.replies["/alpha/billing/credits"] = fmt.Sprintf(`{"success":true,"data":{"windowLimits":{"fiveHour":{"used":0,"cap":14,"resetAt":%d},"weekly":{"used":0.45,"cap":35,"resetAt":%d}},"credits":{"monthlyCredits":63.48}}}`,
		claudeTestNow.Add(-6*time.Hour).UnixMilli(), claudeTestNow.Add(6*24*time.Hour).UnixMilli())
	source := commandcodeTestSource(client.spawnConfigPath, commandcodeLoginFiles(commandcodeSecret), api)
	client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": source}
	warned := newWarnLog(client)

	client.refreshFleetQuota(t.Context(), "commandcode", map[string]fleetQuotaSent{})

	if hk.published() != 0 {
		t.Fatalf("a failed measurement was published")
	}
	if got := warned.all(); len(got) != 1 || !strings.Contains(got[0], "pool commandcode is unavailable (error_kind probe_failed)") {
		t.Fatalf("warnings %q", got)
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if !slices.Contains(hk.calls, "PUT /v1/quota/presence") {
		t.Fatalf("no presence for the commandcode account:\n%s", strings.Join(hk.calls, "\n"))
	}
}

// Shadow mode, acceptance 2: with the switch unflipped (the default) the loop
// still runs pacelever for commandcode, publishes pacelever's reading, and the
// node logs one comparison line from its own unshared measurement.
func TestCommandcodeSharedReadingFreshnessAndPublish(t *testing.T) {
	hk := newClaudeHK(t)
	api := newCommandcodeAPI()
	source := commandcodeTestSource(claudeStore(t, hk.server.URL), commandcodeLoginFiles(commandcodeSecret), api)
	now := claudeTestNow
	source.Now = func() time.Time { return now }
	for _, step := range []struct {
		after     time.Duration
		published int
	}{{0, 1}, {3 * time.Minute, 1}, {4 * time.Minute, 2}} {
		now = claudeTestNow.Add(step.after)
		reading, err := source.Refresh(t.Context())
		if err != nil {
			t.Fatalf("after %v: %v", step.after, err)
		}
		if hk.published() != step.published || reading.Key != commandcodeKeyForTest || len(reading.Buckets) != 3 {
			t.Fatalf("after %v: published %d, reading %+v", step.after, hk.published(), reading)
		}
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	var leases, releases, presence int
	for _, call := range hk.calls {
		switch {
		case strings.HasPrefix(call, "POST") && strings.HasSuffix(call, "/lease"):
			leases++
		case strings.HasPrefix(call, "DELETE") && strings.HasSuffix(call, "/lease"):
			releases++
		case strings.HasSuffix(call, "/presence"):
			presence++
		}
	}
	if leases != 2 || releases != 2 || presence != 1 {
		t.Fatalf("leases=%d releases=%d presence=%d\n%s", leases, releases, presence, strings.Join(hk.calls, "\n"))
	}
}

// A lease held by another host never blocks the node past its context: the
// refresh ends with a shared failure, having measured nothing.
func TestCommandcodeLeaseHeldElsewhereIsBounded(t *testing.T) {
	var mu sync.Mutex
	var leaseCalls int
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/latest"):
			_, _ = w.Write([]byte(`{"observation":null}`))
		case strings.HasSuffix(r.URL.Path, "/lease") && r.Method == "POST":
			leaseCalls++
			_ = json.NewEncoder(w).Encode(shared.Lease{Holder: "another-host", ExpiresAt: time.Now().Add(time.Hour), Acquired: false})
		default:
			w.WriteHeader(204)
		}
	}))
	t.Cleanup(store.Close)
	api := newCommandcodeAPI()
	source := commandcodeTestSource(claudeStore(t, store.URL), commandcodeLoginFiles(commandcodeSecret), api)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := source.Refresh(ctx)
	// The deadline lands either in a poll's sleep (shared_wait) or in a store
	// request (shared_unavailable); both end the refresh at the bound.
	if kind := quota.KindOf(err); kind != quota.ErrKindSharedWait && kind != quota.ErrKindSharedUnavailable {
		t.Fatalf("err = %v, want a shared failure", err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatalf("the wait ran %v past its context", time.Since(started))
	}
	mu.Lock()
	defer mu.Unlock()
	if leaseCalls == 0 {
		t.Fatal("the lease was never asked for")
	}
	// Only the fingerprint's whoami was asked; no measurement ran.
	if api.count() != 1 {
		t.Fatalf("API requests %v", api.requests)
	}
}

// A login that names no account has no shared key: a configured store refuses
// (account_unknown) and a node with no store measures directly.
func TestCommandcodeQuotaWithoutAccount(t *testing.T) {
	api := newCommandcodeAPI()
	api.replies["/alpha/whoami"] = `{"data":{}}`
	hk := newClaudeHK(t)
	if _, err := commandcodeTestSource(claudeStore(t, hk.server.URL), commandcodeLoginFiles(commandcodeSecret), api).Refresh(t.Context()); quota.KindOf(err) != quota.ErrKindAccountUnknown {
		t.Fatalf("err = %v", err)
	}
	reading, err := commandcodeTestSource("/nonexistent", commandcodeLoginFiles(commandcodeSecret), api).Refresh(t.Context())
	if err != nil || reading.Key != "" || len(reading.Buckets) != 3 {
		t.Fatalf("direct measurement %+v err=%v", reading, err)
	}
	// No login at all is credentials, not an unknown account.
	if _, err := commandcodeTestSource(claudeStore(t, hk.server.URL), commandcodeLoginFiles(""), api).Refresh(t.Context()); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("err = %v", err)
	}
}

// A native failure is reported under the kind that says why: the fleet-state
// warning names it, and pacelever is never asked.
func TestFleetQuotaCommandcodeNativeFailureKinds(t *testing.T) {
	setCommandcodeSwitch(t, true)
	for _, test := range []struct {
		name   string
		files  map[string]string
		status int
		kind   string
	}{
		{"no credentials", commandcodeLoginFiles(""), 200, "credentials"},
		{"refused key", commandcodeLoginFiles(commandcodeSecret), 401, "auth_failed"},
		{"forbidden", commandcodeLoginFiles(commandcodeSecret), 403, "auth_failed"},
		{"rate limited", commandcodeLoginFiles(commandcodeSecret), 429, "rate_limited"},
		{"server error", commandcodeLoginFiles(commandcodeSecret), 500, "http"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := quotaClient(t)
			client.spawnConfigPath = "/nonexistent"
			api := claudeAPIFunc(func(r *http.Request) (*http.Response, error) {
				return claudeAPIReply(test.status, `{"error":"x"}`)(r)
			})
			client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": commandcodeTestSource("/nonexistent", test.files, api)}
			pacelever := &paceleverRunnerFake{}
			warned := newWarnLog(client)
			client.refreshFleetQuota(t.Context(), "commandcode", map[string]fleetQuotaSent{})
			if len(pacelever.called()) != 0 {
				t.Fatalf("pacelever was run: %v", pacelever.called())
			}
			if got := warned.all(); len(got) != 1 || !strings.Contains(got[0], "error_kind "+test.kind) || strings.Contains(got[0], commandcodeSecret) {
				t.Fatalf("warnings %q, want kind %s", got, test.kind)
			}
		})
	}
}

// `panewire quota refresh commandcode` measures through the same source once
// native; in shadow it is not a native pool, so the CLI runs pacelever.
func TestQuotaCLIRefreshCommandcode(t *testing.T) {
	setCommandcodeSwitch(t, true)
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	var pools []string
	quotaCLISource = func(pool string) *nativeQuotaSource {
		pools = append(pools, pool)
		return commandcodeTestSource(spawn, commandcodeLoginFiles(commandcodeSecret), newCommandcodeAPI())
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "commandcode"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var reading quotaReading
	if err := json.Unmarshal(out.Bytes(), &reading); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if len(pools) != 1 || reading.Pool != "commandcode" || reading.Account != commandcodeFPForTest || reading.Source != "handoffkeep" || len(reading.Buckets) != 3 || reading.Buckets[2].Window != "30d" {
		t.Fatalf("pools %v reading %+v", pools, reading)
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}
	if strings.Contains(out.String()+errOut.String(), commandcodeSecret) {
		t.Fatal("key in the CLI output")
	}

	// Retired pool switches are ignored: commandcode stays native.
	setCommandcodeSwitch(t, false)
	pools = nil
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "commandcode"}, &out, &errOut); code != 0 || len(pools) != 1 {
		t.Fatalf("native CLI refresh: exit %d pools %v out %q err %q", code, pools, out.String(), errOut.String())
	}
}

// The key never reaches the places a reading or a failure is reported.
func TestCommandcodeKeyNeverLeaves(t *testing.T) {
	setCommandcodeSwitch(t, true)
	leaky := claudeAPIFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Get https://api.commandcode.ai: Authorization: Bearer " + commandcodeSecret)
	})
	source := commandcodeTestSource("/nonexistent", commandcodeLoginFiles(commandcodeSecret), leaky)
	if _, err := source.Refresh(t.Context()); err == nil || strings.Contains(err.Error(), commandcodeSecret) {
		t.Fatalf("err = %v", err)
	}
	client := quotaClient(t)
	client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": source}
	warned := newWarnLog(client)
	client.refreshFleetQuota(t.Context(), "commandcode", map[string]fleetQuotaSent{})
	if got := warned.all(); len(got) != 1 || strings.Contains(strings.Join(got, "\n"), commandcodeSecret) {
		t.Fatalf("warnings %q", got)
	}
}

// A slow shadow measurement never holds up the refresh it rides on: quota.refresh
// answers as soon as pacelever has, and the comparison line follows once the
// native measurement ends. A second refresh while one is in flight is skipped,
// not queued.
