package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/codex"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// codexSecret is the access token of the test login. It may appear only in the
// Authorization header of the one usage request.
const codexSecret = "codex-node-test-token-must-not-leak"

// codexFPForTest is the fingerprint of account_id "fixture-acct", computed
// with pacelever@f53bb01 (accountFP("codex", "fixture-acct")).
const codexFPForTest = "bfe97bcbaec7eb63"

var codexKeyForTest = "codex:" + codexFPForTest

// codexRPCFake replays the app-server's lines and counts its starts and
// closes, so a test can assert the process was always closed.
type codexRPCFake struct {
	starts, closes int
	lines          []string
	startErr       error
}

func (f *codexRPCFake) start(context.Context, string, string, string) (codex.Transport, error) {
	f.starts++
	if f.startErr != nil {
		return nil, f.startErr
	}
	return &codexRPCConn{fake: f, lines: f.lines}, nil
}

type codexRPCConn struct {
	fake  *codexRPCFake
	lines []string
}

func (c *codexRPCConn) Send([]byte) error { return nil }

func (c *codexRPCConn) Recv() ([]byte, error) {
	if len(c.lines) == 0 {
		return nil, errors.New("fixture exhausted")
	}
	line := c.lines[0]
	c.lines = c.lines[1:]
	return []byte(line), nil
}

func (c *codexRPCConn) Close() { c.fake.closes++ }

// codexRPCLines is a pro account's app-server exchange: a 5h window at 42%
// and a 7d window at 10%, both resetting after claudeTestNow.
func codexRPCLines() []string {
	return []string{
		`{"id":1,"result":{}}`,
		fmt.Sprintf(`{"id":2,"result":{"rateLimits":{"planType":"pro","primary":{"usedPercent":42,"resetsAt":%d,"windowDurationMins":300},"secondary":{"usedPercent":10,"resetsAt":%d,"windowDurationMins":10080}}}}`,
			claudeTestNow.Add(3*time.Hour).Unix(), claudeTestNow.Add(4*24*time.Hour).Unix()),
		`{"id":3,"result":{"account":null}}`,
	}
}

func codexLoginFiles(token, account string) map[string]string {
	if token == "" {
		return map[string]string{}
	}
	block := `{"tokens":{"access_token":"` + token + `"`
	if account != "" {
		block += `,"account_id":"` + account + `"`
	}
	return map[string]string{"/home/u/.codex/auth.json": block + `}}`}
}

const codexAPIBodyFmt = `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000,"reset_at":%d},"secondary_window":{"used_percent":8,"limit_window_seconds":604800,"reset_at":%d}}}`

func codexAPIBody() string {
	return fmt.Sprintf(codexAPIBodyFmt, claudeTestNow.Add(3*time.Hour).Unix(), claudeTestNow.Add(4*24*time.Hour).Unix())
}

// codexTestSource is a codex measurement over fakes: the login file, the
// app-server fixture, the wham usage API. The codex binary is "found" on PATH.
func codexTestSource(spawnConfig string, files map[string]string, rpc *codexRPCFake, api claudeAPIFunc) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "codex", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return claudeTestNow }, Wait: 5 * time.Millisecond,
		Codex: codex.Config{
			HTTP: api, Home: "/home/u", GOOS: "linux", LookupEnv: func(string) (string, bool) { return "", false },
			LookPath: func(string) (string, error) { return "/fake/bin/codex", nil },
			ReadFile: func(path string) ([]byte, error) {
				if body, ok := files[path]; ok {
					return []byte(body), nil
				}
				return nil, os.ErrNotExist
			},
			StartRPC: rpc.start,
		},
	}
}

// pacelever for every pool the node does not measure: a call naming claude or
// codex fails the test.
func paceleverNeverForNative(t *testing.T, calls *[]string) relayCommandRunner {
	return func(_ context.Context, args ...string) ([]byte, error) {
		*calls = append(*calls, strings.Join(args, " "))
		for _, arg := range args {
			if isNativeQuotaPool(arg) {
				t.Errorf("pacelever was run for %s: %v", arg, args)
			}
		}
		return nil, errors.New("pacelever unavailable")
	}
}

// Acceptance 3: the fleet-state loop measures codex itself — the app-server,
// the shared store, the hub publish — and never execs pacelever for it.
func TestFleetQuotaCodexIsMeasuredNatively(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	rpc := &codexRPCFake{lines: codexRPCLines()}
	api := func(*http.Request) (*http.Response, error) {
		t.Error("usage API called although the app-server answered")
		return nil, errors.New("unexpected")
	}
	client.nativeQuota = map[string]*nativeQuotaSource{"codex": codexTestSource(client.spawnConfigPath, codexLoginFiles(codexSecret, "fixture-acct"), rpc, api)}
	var pacelever []string
	var warnings []string
	client.warn = func(m string) { warnings = append(warnings, m) }

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "codex", sent)

	if len(pacelever) != 0 {
		t.Fatalf("pacelever was run for codex: %v", pacelever)
	}
	if rpc.starts != 1 || rpc.closes == 0 {
		t.Fatalf("app-server starts=%d closes=%d", rpc.starts, rpc.closes)
	}
	if hk.published() != 1 {
		t.Fatalf("the shared store got %d observations", hk.published())
	}
	hk.mu.Lock()
	var observation struct {
		Kind    string `json:"kind"`
		Plan    string `json:"plan"`
		Buckets []struct {
			Window  string  `json:"window"`
			UsedPct float64 `json:"used_pct"`
		} `json:"buckets"`
	}
	err := json.Unmarshal(hk.pubs[0], &observation)
	calls := strings.Join(hk.calls, "\n")
	hk.mu.Unlock()
	if err != nil || observation.Kind != "auto" || observation.Plan != "pro" || len(observation.Buckets) != 2 {
		t.Fatalf("stored observation %+v err=%v", observation, err)
	}
	if !strings.Contains(calls, "/v1/quota/accounts/"+codexKeyForTest+"/") {
		t.Fatalf("store calls\n%s", calls)
	}
	select {
	case event := <-client.events:
		var publish FleetStatePublish
		if err := json.Unmarshal(event.Payload, &publish); err != nil {
			t.Fatal(err)
		}
		if publish.Key != codexKeyForTest || publish.Quota == nil || publish.Quota.Pool != "codex" || publish.Quota.Source != "handoffkeep" || !publish.MeasuredAt.Equal(claudeTestNow) {
			t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
		}
		if len(publish.Quota.Buckets) != 2 || *publish.Quota.Buckets[0].UsedPct != 42 || publish.Quota.Buckets[0].Window != "5h" || *publish.Quota.Buckets[1].UsedPct != 10 || publish.Quota.Buckets[1].Window != "7d" {
			t.Fatalf("buckets %+v", publish.Quota.Buckets)
		}
		if strings.Contains(string(event.Payload), codexSecret) {
			t.Fatal("token in the published state")
		}
	default:
		t.Fatal("nothing was published to the hub")
	}
	if got := poolLogin(t, client, "codex"); got.Login != fleet.ReadinessLoginPresent || got.Account != codexKeyForTest {
		t.Fatalf("login %+v", got)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings %q", warnings)
	}

	// The next round reads the shared reading: the app-server is not started
	// again.
	client.refreshFleetQuota(t.Context(), "codex", sent)
	if rpc.starts != 1 {
		t.Fatalf("a fresh shared reading was measured again: %d starts", rpc.starts)
	}
}

// With the app-server gone the same loop falls back to the wham usage API,
// sending the token only in its Authorization header.
func TestFleetQuotaCodexFallsBackToTheAPI(t *testing.T) {
	hk := newClaudeHK(t)
	client := quotaClient(t)
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	rpc := &codexRPCFake{startErr: errors.New("no codex binary")}
	var requests []*http.Request
	client.nativeQuota = map[string]*nativeQuotaSource{"codex": codexTestSource(client.spawnConfigPath, codexLoginFiles(codexSecret, "fixture-acct"), rpc, func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r)
		return claudeAPIReply(200, codexAPIBody())(r)
	})}
	var pacelever []string
	client.refreshFleetQuota(t.Context(), "codex", map[string]fleetQuotaSent{})

	if len(requests) != 1 || requests[0].Header.Get("Authorization") != "Bearer "+codexSecret || requests[0].Header.Get("ChatGPT-Account-Id") != "fixture-acct" {
		t.Fatalf("usage requests: %v", requests)
	}
	if len(pacelever) != 0 || hk.published() != 1 {
		t.Fatalf("pacelever=%v published=%d", pacelever, hk.published())
	}
	if got := poolLogin(t, client, "codex"); got.Login != fleet.ReadinessLoginPresent || got.Account != codexKeyForTest {
		t.Fatalf("login %+v", got)
	}
}

// hk 3087's rule for the codex pool: Login missing only for
// auth|auth_failed|credentials|token_expired; every other failure leaves the
// login unknown. No failure reaches pacelever's status.
func TestFleetQuotaCodexLoginClassification(t *testing.T) {
	valid := codexLoginFiles(codexSecret, "fixture-acct")
	for _, test := range []struct {
		name  string
		files map[string]string
		api   claudeAPIFunc
		want  string
		kind  string
	}{
		{"no credentials", codexLoginFiles("", ""), claudeAPIReply(200, codexAPIBody()), fleet.ReadinessLoginMissing, "credentials"},
		{"refused token", valid, claudeAPIReply(401, "no"), fleet.ReadinessLoginMissing, "auth"},
		{"forbidden token", valid, claudeAPIReply(403, "no"), fleet.ReadinessLoginMissing, "auth"},
		{"rate limited", valid, claudeAPIReply(429, "slow"), fleet.ReadinessLoginUnknown, "rate_limited"},
		{"server error", valid, claudeAPIReply(500, "boom"), fleet.ReadinessLoginUnknown, "http"},
		{"network", valid, func(*http.Request) (*http.Response, error) { return nil, errors.New("dial " + codexSecret) }, fleet.ReadinessLoginUnknown, "network"},
		{"unparsable", valid, claudeAPIReply(200, "{}"), fleet.ReadinessLoginUnknown, "parse_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hk := newClaudeHK(t)
			client := quotaClient(t)
			client.spawnConfigPath = claudeStore(t, hk.server.URL)
			rpc := &codexRPCFake{startErr: errors.New("no codex binary")}
			client.nativeQuota = map[string]*nativeQuotaSource{"codex": codexTestSource(client.spawnConfigPath, test.files, rpc, test.api)}
			var pacelever []string
			var warnings []string
			client.warn = func(m string) { warnings = append(warnings, m) }
			client.refreshFleetQuota(t.Context(), "codex", map[string]fleetQuotaSent{})
			if got := poolLogin(t, client, "codex").Login; got != test.want {
				t.Fatalf("login = %q, want %q", got, test.want)
			}
			if len(pacelever) != 0 {
				t.Fatalf("pacelever was run: %v", pacelever)
			}
			if len(warnings) != 1 || !strings.Contains(warnings[0], "error_kind "+test.kind) || strings.Contains(warnings[0], codexSecret) {
				t.Fatalf("warnings %q, want kind %s", warnings, test.kind)
			}
			if hk.published() != 0 {
				t.Fatal("a failed measurement was published")
			}
		})
	}
}

// Acceptance 3: the quota.refresh node op measures codex itself, and still
// execs pacelever for a pool the node does not measure.
func TestNodeQuotaRefreshCodexIsNative(t *testing.T) {
	hk := newClaudeHK(t)
	client := &HubClient{}
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	rpc := &codexRPCFake{lines: codexRPCLines()}
	client.nativeQuota = map[string]*nativeQuotaSource{"codex": codexTestSource(client.spawnConfigPath, codexLoginFiles(codexSecret, "fixture-acct"), rpc, claudeAPIReply(500, "unused"))}
	var pacelever []string
	result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "codex"})
	if code != "" || result["outcome"] != nodeQuotaRefreshed {
		t.Fatalf("codex refresh: result=%v code=%q", result, code)
	}
	if len(pacelever) != 0 || hk.published() != 1 || rpc.starts != 1 || rpc.closes == 0 {
		t.Fatalf("pacelever=%v published=%d starts=%d closes=%d", pacelever, hk.published(), rpc.starts, rpc.closes)
	}
	if result, code := nodeRun(t, client, nodeCommandQuotaRefresh, map[string]any{"pool": "kimi"}); code != "refresh_failed" || result != nil || len(pacelever) != 0 {
		t.Fatalf("unmeasurable kimi refresh: code=%q calls=%v result=%v", code, pacelever, result)
	}

	failing := &HubClient{nativeQuota: map[string]*nativeQuotaSource{"codex": codexTestSource("/nonexistent", codexLoginFiles("", ""), &codexRPCFake{}, claudeAPIReply(200, codexAPIBody()))}}
	if _, code := nodeRun(t, failing, nodeCommandQuotaRefresh, map[string]any{"pool": "codex"}); code != "refresh_failed" {
		t.Fatalf("failing codex refresh code=%q", code)
	}
}

// The shared reading is refreshed with a two-minute margin before its
// 300-second reader TTL: three minutes later it is reused, four minutes later
// it is measured again.
func TestCodexSharedReadingFreshnessIsFiveMinutes(t *testing.T) {
	hk := newClaudeHK(t)
	rpc := &codexRPCFake{}
	rpc.lines = codexRPCLines()
	source := codexTestSource(claudeStore(t, hk.server.URL), codexLoginFiles(codexSecret, "fixture-acct"), rpc, claudeAPIReply(500, "unused"))
	now := claudeTestNow
	source.Now = func() time.Time { return now }
	for _, step := range []struct {
		after  time.Duration
		starts int
	}{{0, 1}, {3 * time.Minute, 1}, {4 * time.Minute, 2}} {
		now = claudeTestNow.Add(step.after)
		if _, err := source.Refresh(t.Context()); err != nil {
			t.Fatalf("after %v: %v", step.after, err)
		}
		if rpc.starts != step.starts {
			t.Fatalf("after %v: %d measurements, want %d", step.after, rpc.starts, step.starts)
		}
	}
}

func TestOtherPoolRefreshesBeforeReaderTTL(t *testing.T) {
	hk := newClaudeHK(t)
	rpc := &codexRPCFake{lines: codexRPCLines()}
	source := codexTestSource(claudeStore(t, hk.server.URL), codexLoginFiles(codexSecret, "fixture-acct"), rpc, claudeAPIReply(500, "unused"))
	now := claudeTestNow
	source.Now = func() time.Time { return now }
	_, _, readerTTL := source.poolMeasurement()
	if readerTTL != shared.OtherFreshTTL {
		t.Fatalf("reader TTL=%s, want %s", readerTTL, shared.OtherFreshTTL)
	}
	if got := source.refreshTTL(readerTTL); got != shared.OtherRefreshTTL {
		t.Fatalf("publisher refresh TTL=%s, want %s", got, shared.OtherRefreshTTL)
	}
	for elapsed := time.Duration(0); elapsed <= 10*time.Minute; elapsed += time.Second {
		now = claudeTestNow.Add(elapsed)
		if elapsed%time.Minute == 0 {
			if _, err := source.Refresh(t.Context()); err != nil {
				t.Fatalf("publisher refresh at %s: %v", elapsed, err)
			}
		}
		reading, err := source.stored(t.Context())
		if err != nil {
			t.Fatalf("stored reader at %s: %v", elapsed, err)
		}
		if reading.Stale {
			t.Fatalf("stored reader marked healthy publisher's reading stale at %s (age %v)", elapsed, reading.AgeSeconds)
		}
	}
	if rpc.starts != 3 {
		t.Fatalf("measurements=%d, want one at initial publish and one at each 4-minute refresh cycle", rpc.starts)
	}
}

// A login without an account_id has no shared key: a configured store refuses,
// and a node with no store measures directly and publishes to no one.
func TestCodexQuotaWithoutAccountID(t *testing.T) {
	files := codexLoginFiles(codexSecret, "")
	rpc := &codexRPCFake{lines: codexRPCLines()}
	hk := newClaudeHK(t)
	if _, err := codexTestSource(claudeStore(t, hk.server.URL), files, rpc, claudeAPIReply(500, "unused")).Refresh(t.Context()); quota.KindOf(err) != quota.ErrKindAccountUnknown || rpc.starts != 0 {
		t.Fatalf("err=%v starts=%d", err, rpc.starts)
	}
	reading, err := codexTestSource("/nonexistent", files, rpc, claudeAPIReply(500, "unused")).Refresh(t.Context())
	if err != nil || reading.Key != "" || reading.Source != quota.SourceUsageRPC || len(reading.Buckets) != 2 {
		t.Fatalf("direct measurement %+v err=%v", reading, err)
	}
}

// `panewire quota refresh codex` measures through the same source.
func TestQuotaCLIRefreshCodex(t *testing.T) {
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	var pools []string
	rpc := &codexRPCFake{lines: codexRPCLines()}
	quotaCLISource = func(pool string) *nativeQuotaSource {
		pools = append(pools, pool)
		return codexTestSource(spawn, codexLoginFiles(codexSecret, "fixture-acct"), rpc, claudeAPIReply(500, "unused"))
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "codex"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var reading quotaReading
	if err := json.Unmarshal(out.Bytes(), &reading); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if len(pools) != 1 || pools[0] != "codex" || reading.Pool != "codex" || reading.Account != codexFPForTest || reading.Source != "handoffkeep" || reading.Stale || len(reading.Buckets) != 2 || *reading.Buckets[0].UsedPct != 42 {
		t.Fatalf("pools %v reading %+v", pools, reading)
	}
	if hk.published() != 1 {
		t.Fatalf("published %d", hk.published())
	}

	quotaCLISource = func(pool string) *nativeQuotaSource {
		return codexTestSource(claudeStore(t, newClaudeHK(t).server.URL), codexLoginFiles("", ""), &codexRPCFake{}, claudeAPIReply(500, "unused"))
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"refresh", "codex"}, &out, &errOut); code == 0 || errOut.String() != "quota refresh: credentials\n" || out.Len() != 0 {
		t.Fatalf("failing refresh: exit %d out=%q err=%q", code, out.String(), errOut.String())
	}
}

// Acceptance 4: the token never reaches the places a reading or a failure is
// reported: not the warning, the hub state, the CLI output or any error.
func TestCodexTokenNeverLeaves(t *testing.T) {
	leaky := claudeAPIFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Get https://chatgpt.com: Authorization: Bearer " + codexSecret)
	})
	hk := newClaudeHK(t)
	spawn := claudeStore(t, hk.server.URL)
	rpc := &codexRPCFake{startErr: errors.New("spawn " + codexSecret)}
	source := codexTestSource(spawn, codexLoginFiles(codexSecret, "fixture-acct"), rpc, leaky)
	if _, err := source.Refresh(t.Context()); err == nil || strings.Contains(err.Error(), codexSecret) {
		t.Fatalf("err = %v", err)
	}
	client := quotaClient(t)
	client.spawnConfigPath = spawn
	client.nativeQuota = map[string]*nativeQuotaSource{"codex": source}
	var warnings []string
	client.warn = func(m string) { warnings = append(warnings, m) }
	client.refreshFleetQuota(t.Context(), "codex", map[string]fleetQuotaSent{})
	if len(warnings) != 1 || strings.Contains(strings.Join(warnings, "\n"), codexSecret) {
		t.Fatalf("warnings %q", warnings)
	}
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	quotaCLISource = func(string) *nativeQuotaSource { return source }
	var out, errOut bytes.Buffer
	runQuotaCLI([]string{"refresh", "codex"}, &out, &errOut)
	if strings.Contains(out.String()+errOut.String(), codexSecret) {
		t.Fatal("token in the CLI output")
	}
}
