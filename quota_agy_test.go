package panewire

import (
	"bytes"
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

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/agy"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// agyToken is the OAuth token of the test login. It may appear only in the
// Authorization header of a cloud request.
const agyToken = "agy-node-test-token-must-not-leak"

// agyFPForTest is the configured account fingerprint: agy has no login to
// derive one from, so the host's `[quota.accounts]` names it (pacelever's rule).
const agyFPForTest = "0123456789abcdef"

var agyKeyForTest = "agy:" + agyFPForTest

// The cloud summary shape (ids removed): the gemini group has a 5h window, the
// 3p group (Claude and GPT) a 5h and a weekly window, with claudeTestNow as the
// clock.
const agySummaryReply = `{"response":{"groups":[` +
	`{"displayName":"Gemini Models","buckets":[{"bucketId":"gemini-5h","window":"5h","remainingFraction":0.8,"resetTime":"2026-11-05T15:00:00Z"}]},` +
	`{"displayName":"Claude and GPT models","buckets":[{"bucketId":"3p-5h","window":"5h","remainingFraction":0.5,"resetTime":"2026-11-05T15:00:00Z"},{"bucketId":"3p-weekly","window":"weekly","remainingFraction":0.4,"resetTime":"2026-11-09T01:00:00Z"}]}]},` +
	`"planDaily":{"paidTier":{"name":"Google AI Pro"}}}`

// agyAPI serves the cloud summary and counts the requests.
type agyAPI struct {
	mu       sync.Mutex
	replies  map[string]string
	requests []*http.Request
}

func newAgyAPI() *agyAPI {
	return &agyAPI{replies: map[string]string{
		"/v1internal:retrieveUserQuotaSummary": agySummaryReply,
		"/v1internal:fetchAvailableModels":     `{"models":{}}`,
	}}
}

func (a *agyAPI) Do(r *http.Request) (*http.Response, error) {
	a.mu.Lock()
	a.requests = append(a.requests, r)
	body, ok := a.replies[r.URL.Path]
	a.mu.Unlock()
	if !ok {
		return nil, errors.New("no reply for " + r.URL.Path)
	}
	return claudeAPIReply(200, body)(r)
}

func (a *agyAPI) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests)
}

func agyLoginFiles(access string) map[string]string {
	if access == "" {
		return map[string]string{}
	}
	return map[string]string{"/home/u/.gemini/antigravity-cli/antigravity-oauth-token": `{"token":{"access_token":"` + access + `"}}`}
}

// agyTestSource is an agy measurement over fakes: the login file, the cloud API,
// no app process and no keychain, secret service or agy binary. account is the
// host's configured fingerprint and whether it may measure it.
func agyTestSource(spawnConfig string, files map[string]string, api agy.Doer, fp string, measurable bool) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "agy", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return claudeTestNow }, Wait: 5 * time.Millisecond,
		AgyAccount: func() (string, bool) { return fp, measurable },
		Agy: agy.Config{
			HTTP: api, Home: "/home/u", GOOS: "linux", LookupEnv: func(string) (string, bool) { return "", false },
			ReadFile: func(path string) ([]byte, error) {
				if body, ok := files[path]; ok {
					return []byte(body), nil
				}
				return nil, os.ErrNotExist
			},
			SecretService: func(context.Context) (string, error) { return "", errors.New("no secret service") },
			LookPath:      func(string) (string, error) { return "", errors.New("not found") },
			Processes:     func(context.Context) []agy.Proc { return nil },
			ListenPorts:   func(context.Context, int) []int { return nil },
			Locked:        func(_ string, fn func() error) error { return fn() },
		},
	}
}

// setAgySwitch writes this host's fleet config with agy listed (native) or not
// (shadow), and its account configured and measurable (or not), and points the
// node at it.
func setAgySwitch(t *testing.T, native, configured bool) {
	t.Helper()
	body := "[quota]\nfresh_ttl_s = 300\n"
	if native {
		body += "native_pools = [\"agy\"]\n"
	}
	if configured {
		body += "measurable_accounts = [\"" + agyKeyForTest + "\"]\n[quota.accounts]\nagy = \"" + agyFPForTest + "\"\n"
	}
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", path)
}

// paceleverAgyOutput is pacelever's reading of the same account and day: the
// group buckets with their scope.
func paceleverAgyOutput() string {
	reset5h := claudeTestNow.Add(3 * time.Hour).UTC().Format(time.RFC3339Nano)
	reset7d := time.Date(2026, 11, 9, 1, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	group := func(name, window string, used float64, reset string) string {
		return fmt.Sprintf(`{"label":%q,"window":%q,"used_pct":%g,"resets_at":%q,"scope":{"kind":"group","name":%q}}`, window, window, used, reset, name)
	}
	return fmt.Sprintf(`{"id":"agy","buckets":[%s,%s,%s],"source":"usage-api","stale":false,"fetched_at":%d,"account_fp":%q,"account_fp_kind":"account"}`,
		group("gemini", "5h", 20, reset5h), group("3p", "5h", 50, reset5h), group("3p", "7d", 60, reset7d), claudeTestNow.Unix(), agyFPForTest)
}

func TestFleetQuotaAgyNativeNeverRunsPacelever(t *testing.T) {
	setAgySwitch(t, true, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	api := newAgyAPI()
	source := agyTestSource(client.spawnConfigPath, agyLoginFiles(agyToken), api, agyFPForTest, true)
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": source}
	pacelever := &paceleverRunnerFake{pool: "agy"}
	warned := newWarnLog(client)

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "agy", sent)

	if calls := pacelever.called(); len(calls) != 0 {
		t.Fatalf("pacelever was run for agy in native mode: %v", calls)
	}
	if got := warned.all(); len(got) != 0 {
		t.Fatalf("warnings %q", got)
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	hk.mu.Lock()
	var observation shared.Observation
	err := json.Unmarshal(hk.pubs[0], &observation)
	calls := strings.Join(hk.calls, "\n")
	hk.mu.Unlock()
	if err != nil || observation.Kind != "auto" || observation.Plan != "GOOGLE AI PRO" || len(observation.Buckets) != 3 {
		t.Fatalf("stored observation %+v err=%v", observation, err)
	}
	type bucket struct {
		scope, group, window string
		used                 float64
	}
	var got []bucket
	for _, b := range observation.Buckets {
		got = append(got, bucket{b.Scope, b.Model, b.Window, float64(int(b.UsedPct*10+0.5)) / 10})
	}
	want := []bucket{{"group", "gemini", "5h", 20}, {"group", "3p", "5h", 50}, {"group", "3p", "7d", 60}}
	if !slices.Equal(got, want) {
		t.Fatalf("buckets %v, want %v", got, want)
	}
	if !strings.Contains(calls, "/v1/quota/accounts/"+agyKeyForTest+"/") || !strings.Contains(calls, "DELETE /v1/quota/accounts/"+agyKeyForTest+"/lease") || !slices.Contains(strings.Split(calls, "\n"), "PUT /v1/quota/presence") {
		t.Fatalf("the lease was not taken and released under the configured key, or no presence:\n%s", calls)
	}
	select {
	case event := <-client.events:
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil {
			t.Fatal(err)
		}
		if publish.Key != agyKeyForTest || publish.Quota == nil || publish.Quota.Pool != "agy" || publish.Quota.Source != "handoffkeep" || len(publish.Quota.Buckets) != 3 {
			t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
		}
		for _, b := range publish.Quota.Buckets {
			if b.Scope != "group" || (b.Model != "gemini" && b.Model != "3p") {
				t.Fatalf("a published bucket lost its group scope: %+v", b)
			}
		}
		if strings.Contains(string(event.Payload), agyToken) {
			t.Fatal("token in the published state")
		}
	default:
		t.Fatal("nothing was published to the hub")
	}

	// The next round reads the shared reading: no new measurement.
	before := api.count()
	client.refreshFleetQuota(t.Context(), "agy", sent)
	if api.count() != before {
		t.Fatalf("a fresh shared reading cost %d requests", api.count()-before)
	}
	for _, r := range api.requests {
		if r.Header.Get("Authorization") != "Bearer "+agyToken {
			t.Fatalf("request auth %q", r.Header.Get("Authorization"))
		}
	}
}

// Linux: agy's login lives in the secret service when the CLI keeps no login
// file, so the quota path reports the host logged in instead of login_missing.
func TestFleetQuotaAgyLinuxSecretServiceLogin(t *testing.T) {
	setAgySwitch(t, true, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	const secret = "agy-secret-service-token-must-not-leak"
	api := claudeAPIFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer "+secret {
			return claudeAPIReply(401, `{}`)(r)
		}
		if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
			return claudeAPIReply(200, `{"models":{}}`)(r)
		}
		return claudeAPIReply(200, agySummaryReply)(r)
	})
	source := agyTestSource(client.spawnConfigPath, agyLoginFiles(""), api, agyFPForTest, true)
	source.Agy.SecretService = func(context.Context) (string, error) {
		return `{"token":{"access_token":"` + secret + `"}}`, nil
	}
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": source}
	warned := newWarnLog(client)

	client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})

	if got := warned.all(); len(got) != 0 {
		t.Fatalf("warnings %q", got)
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	login := poolLogin(t, client, "agy")
	if login.Login != fleet.ReadinessLoginPresent || login.Account != agyKeyForTest {
		t.Fatalf("login %+v, want present under %s", login, agyKeyForTest)
	}
	hk.mu.Lock()
	published := string(hk.pubs[0])
	hk.mu.Unlock()
	if strings.Contains(published, secret) {
		t.Fatal("the secret service token is in the published reading")
	}
}

// A host with no configured agy account, or one measurable_accounts does not
// list, does not measure agy and does not claim the account: one log line, no
// request, no lease, no presence; the failure kind is credentials, as a host
// with no login reports.
func TestFleetQuotaAgyNativeWithoutAMeasurableAccountDoesNotMeasure(t *testing.T) {
	for name, c := range map[string]struct {
		fp         string
		measurable bool
	}{
		"not configured":      {"", false},
		"not measurable here": {agyFPForTest, false},
	} {
		t.Run(name, func(t *testing.T) {
			setAgySwitch(t, true, false)
			hk := newClaudeHK(t)
			client := quotaClient(t)
			client.spawnConfigPath = claudeStore(t, hk.server.URL)
			api := newAgyAPI()
			client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource(client.spawnConfigPath, agyLoginFiles(agyToken), api, c.fp, c.measurable)}
			warned := newWarnLog(client)
			client.nativeQuota["agy"].Warn = client.warnMessage

			for range 3 {
				client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})
			}
			if api.count() != 0 {
				t.Fatalf("agy was measured without a measurable account: %d requests", api.count())
			}
			hk.mu.Lock()
			calls := strings.Join(hk.calls, "\n")
			hk.mu.Unlock()
			if calls != "" {
				t.Fatalf("the store was touched:\n%s", calls)
			}
			var notices, unavailable int
			for _, line := range warned.all() {
				switch {
				case strings.HasPrefix(line, "quota agy:"):
					notices++
				case strings.Contains(line, "error_kind credentials"):
					unavailable++
				}
			}
			if notices != 1 || unavailable == 0 {
				t.Fatalf("warnings %q: want exactly one agy notice (%d) and the credentials failure (%d)", warned.all(), notices, unavailable)
			}
		})
	}
}

// Presence follows the account the host may measure, not a published reading: a
// measurement that fails still tells the store this host holds the account.
func TestFleetQuotaAgyNativePublishesPresenceWhenMeasurementFails(t *testing.T) {
	setAgySwitch(t, true, true)
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.machineID = "node-a"
	api := newAgyAPI()
	api.replies["/v1internal:retrieveUserQuotaSummary"] = `{"groups":[]}`
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource(client.spawnConfigPath, agyLoginFiles(agyToken), api, agyFPForTest, true)}
	warned := newWarnLog(client)

	client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})

	if hk.published() != 0 {
		t.Fatal("a failed measurement was published")
	}
	if got := warned.all(); len(got) != 1 || !strings.Contains(got[0], "pool agy is unavailable (error_kind parse_error)") {
		t.Fatalf("warnings %q", got)
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	if !slices.Contains(hk.calls, "PUT /v1/quota/presence") {
		t.Fatalf("no presence for the agy account:\n%s", strings.Join(hk.calls, "\n"))
	}
}

// Shadow mode: with the switch unflipped (the default) the loop still runs
// pacelever for agy, publishes pacelever's reading, and the node logs one
// comparison line from its own unshared measurement — per group, since gemini
// and 3p both have a 5h window.
func TestNodeQuotaRefreshAgyWithoutAnAccountFails(t *testing.T) {
	setAgySwitch(t, true, false)
	api := newAgyAPI()
	client := &HubClient{spawnConfigPath: "/nonexistent"}
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource("/nonexistent", agyLoginFiles(agyToken), api, "", false)}
	if _, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "agy"}); code != "refresh_failed" {
		t.Fatalf("code=%q", code)
	}
	if api.count() != 0 {
		t.Fatalf("%d provider requests", api.count())
	}
}

// Shared flow refreshes a reading with a two-minute margin before its
// 300-second reader TTL, publishing under the configured account key with the
// lease released.
func TestAgySharedReadingFreshnessAndPublish(t *testing.T) {
	hk := newClaudeHK(t)
	api := newAgyAPI()
	source := agyTestSource(claudeStore(t, hk.server.URL), agyLoginFiles(agyToken), api, agyFPForTest, true)
	now := claudeTestNow
	source.Now = func() time.Time { return now }
	api.replies["/v1internal:retrieveUserQuotaSummary"] = agySummaryReply
	for _, step := range []struct {
		after     time.Duration
		published int
	}{{0, 1}, {3 * time.Minute, 1}, {4 * time.Minute, 2}} {
		now = claudeTestNow.Add(step.after)
		reading, err := source.Refresh(t.Context())
		if err != nil {
			t.Fatalf("after %v: %v", step.after, err)
		}
		if hk.published() != step.published || reading.Key != agyKeyForTest || len(reading.Buckets) != 3 {
			t.Fatalf("after %v: published %d, reading %+v", step.after, hk.published(), reading)
		}
		for _, b := range reading.Buckets {
			if b.Scope != "group" || b.Model == "" {
				t.Fatalf("bucket without its group scope: %+v", b)
			}
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

// A reading another host published, with group buckets, is current here: the
// store flow accepts the group scope.
func TestAgySharedFlowAcceptsAnotherHostsGroupReading(t *testing.T) {
	hk := newClaudeHK(t)
	reset := claudeTestNow.Add(2 * time.Hour).UTC().Format(time.RFC3339)
	hk.latest = json.RawMessage(fmt.Sprintf(`{"kind":"auto","measured_at":%q,"buckets":[{"window":"5h","used_pct":100,"resets_at":%q,"scope":"group","model":"gemini"},{"window":"5h","used_pct":1,"resets_at":%q,"scope":"group","model":"3p"}]}`,
		claudeTestNow.Add(-time.Minute).UTC().Format(time.RFC3339), reset, reset))
	api := newAgyAPI()
	reading, err := agyTestSource(claudeStore(t, hk.server.URL), agyLoginFiles(agyToken), api, agyFPForTest, true).Refresh(t.Context())
	if err != nil || len(reading.Buckets) != 2 || api.count() != 0 {
		t.Fatalf("reading %+v err=%v requests=%d", reading, err, api.count())
	}
}

// `panewire quota refresh agy` measures through the same source once native
// and prints every group bucket with its scope; in shadow the CLI is
// pacelever's.
func TestQuotaCLIRefreshAgy(t *testing.T) {
	setAgySwitch(t, true, true)
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	var pools []string
	quotaCLISource = func(pool string) *nativeQuotaSource {
		pools = append(pools, pool)
		return agyTestSource(spawn, agyLoginFiles(agyToken), newAgyAPI(), agyFPForTest, true)
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "agy"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var reading quotaReading
	if err := json.Unmarshal(out.Bytes(), &reading); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if len(pools) != 1 || reading.Pool != "agy" || reading.Account != agyFPForTest || reading.Source != "handoffkeep" || len(reading.Buckets) != 3 {
		t.Fatalf("pools %v reading %+v", pools, reading)
	}
	for i, want := range []string{"gemini", "3p", "3p"} {
		if b := reading.Buckets[i]; b.Scope.Kind != "group" || b.Scope.Name != want {
			t.Fatalf("bucket %d scope %+v, want group %s", i, b.Scope, want)
		}
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}
	if strings.Contains(out.String()+errOut.String(), agyToken) {
		t.Fatal("token in the CLI output")
	}

	// A host that may not measure agy: the CLI says credentials.
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return agyTestSource(spawn, agyLoginFiles(agyToken), newAgyAPI(), "", false)
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "agy"}, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "credentials") {
		t.Fatalf("no account: exit %d out %q err %q", code, out.String(), errOut.String())
	}

	// Retired pool switches are ignored: agy stays native and reports its source result.
	setAgySwitch(t, false, true)
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "agy"}, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "credentials") {
		t.Fatalf("native CLI refresh: exit %d out %q err %q", code, out.String(), errOut.String())
	}
}

// The token never reaches the places a reading or a failure is reported.
func TestAgyTokenNeverLeaves(t *testing.T) {
	setAgySwitch(t, true, true)
	leaky := claudeAPIFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Post https://cloudcode-pa.googleapis.com: Authorization: Bearer " + agyToken)
	})
	source := agyTestSource("/nonexistent", agyLoginFiles(agyToken), leaky, agyFPForTest, true)
	if _, err := source.Refresh(t.Context()); err == nil || strings.Contains(err.Error(), agyToken) {
		t.Fatalf("err = %v", err)
	}
	client := quotaClient(t)
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": source}
	warned := newWarnLog(client)
	client.refreshFleetQuota(t.Context(), "agy", map[string]fleetQuotaSent{})
	if got := warned.all(); len(got) != 1 || strings.Contains(strings.Join(got, "\n"), agyToken) {
		t.Fatalf("warnings %q", got)
	}
}

// A lease held by another host never blocks the node past its context.
func TestAgyLeaseHeldElsewhereIsBounded(t *testing.T) {
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
	api := newAgyAPI()
	source := agyTestSource(claudeStore(t, store.URL), agyLoginFiles(agyToken), api, agyFPForTest, true)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := source.Refresh(ctx)
	if kind := quota.KindOf(err); kind != quota.ErrKindSharedWait && kind != quota.ErrKindSharedUnavailable {
		t.Fatalf("err = %v, want a shared failure", err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatalf("the wait ran %v past its context", time.Since(started))
	}
	mu.Lock()
	defer mu.Unlock()
	if leaseCalls == 0 || api.count() != 0 {
		t.Fatalf("lease asked %d times, %d provider requests", leaseCalls, api.count())
	}
}
