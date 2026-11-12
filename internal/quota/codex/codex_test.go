package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// token is the access token of the test login. It may appear only in the
// Authorization header of the one usage request.
const token = "codex-access-token-must-not-leak"

// fixtureFP is hex(sha256("codex-account|fixture-acct"))[:16], computed with
// pacelever@f53bb01 (accountFP("codex", "fixture-acct")).
const fixtureFP = "bfe97bcbaec7eb63"

var testNow = time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type doFunc func(*http.Request) (*http.Response, error)

func (f doFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func reply(status int, body string, header http.Header) doFunc {
	return func(*http.Request) (*http.Response, error) {
		if header == nil {
			header = http.Header{}
		}
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

// authFiles is a login under /home/u/.codex; a blank account omits account_id.
func authFiles(account string) map[string]string {
	block := `{"tokens":{"access_token":"` + token + `"`
	if account != "" {
		block += `,"account_id":"` + account + `"`
	}
	return map[string]string{"/home/u/.codex/auth.json": block + `}}`}
}

// config is a measurement over fakes: the login files, a codex on PATH, no
// environment.
func config(files map[string]string, api Doer, start func(context.Context, string, string, string) (Transport, error)) Config {
	return Config{
		HTTP: api, Now: func() time.Time { return testNow }, Home: "/home/u", GOOS: "linux",
		LookupEnv: func(string) (string, bool) { return "", false },
		LookPath:  func(string) (string, error) { return "/fake/bin/codex", nil },
		ReadFile: func(path string) ([]byte, error) {
			if body, ok := files[path]; ok {
				return []byte(body), nil
			}
			return nil, os.ErrNotExist
		},
		StartRPC: start,
	}
}

// fakeRPC replays recorded JSON-RPC lines; no test spawns a real CLI.
type fakeRPC struct {
	lines  []string
	sent   []string
	recvAt int
	closed int
}

func (f *fakeRPC) Send(b []byte) error { f.sent = append(f.sent, string(b)); return nil }

func (f *fakeRPC) Recv() ([]byte, error) {
	if f.recvAt >= len(f.lines) {
		return nil, errors.New("fixture exhausted")
	}
	line := f.lines[f.recvAt]
	f.recvAt++
	return []byte(line), nil
}

func (f *fakeRPC) Close() { f.closed++ }

func startWith(f *fakeRPC, err error) func(context.Context, string, string, string) (Transport, error) {
	return func(context.Context, string, string, string) (Transport, error) {
		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

func lines(text string) []string { return strings.Split(strings.TrimSpace(text), "\n") }

// bucketRow is one bucket as the equivalence run prints it.
type bucketRow struct {
	Window string
	Used   float64
	Resets string
	Scope  string
	Name   string
}

func rows(t *testing.T, buckets []quota.Bucket) []bucketRow {
	t.Helper()
	var out []bucketRow
	for _, b := range buckets {
		if b.UsedPct == nil {
			t.Fatalf("unmeasured bucket %+v", b)
		}
		out = append(out, bucketRow{b.Window, *b.UsedPct, b.ResetsAt, b.Scope.Kind, b.Scope.Name})
	}
	return out
}

func equalRows(t *testing.T, got, want []bucketRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("buckets\n got %+v\nwant %+v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("bucket %d\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// Acceptance 2, RPC fixture: the buckets, plan, source and fingerprint
// pacelever@f53bb01 produces for the same exchange and login (fetchCodex with
// a file login naming account fixture-acct and the fixture transport).
func TestEquivalenceRPCFixture(t *testing.T) {
	f := &fakeRPC{lines: lines(fixture(t, "rpc_exchange.jsonl"))}
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), nil, startWith(f, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if observed.Source != "usage-rpc" || observed.Plan != "pro" || observed.AccountFP != fixtureFP {
		t.Fatalf("observation %+v", observed)
	}
	equalRows(t, rows(t, observed.Buckets), []bucketRow{
		{"7d", 94, "2026-09-27T09:20:39Z", "account", ""},
		{"5h", 12, "2026-09-21T08:40:00Z", "account", ""},
		{"5h", 40, "2026-09-21T08:40:00Z", "model", "Spark"},
		{"7d", 5, "", "model", "Spark"},
	})
	if len(f.sent) != 4 || !strings.Contains(f.sent[0], `"initialize"`) || !strings.Contains(f.sent[1], `"initialized"`) || !strings.Contains(f.sent[2], `"account/rateLimits/read"`) || !strings.Contains(f.sent[3], `"account/read"`) {
		t.Fatalf("exchange order %+v", f.sent)
	}
	if f.closed == 0 {
		t.Fatal("transport not closed")
	}
}

// Acceptance 2, HTTP fixture: pacelever@f53bb01's buckets for the wham body,
// with the primary header override, at a fixed clock.
func TestEquivalenceHTTPFixture(t *testing.T) {
	api := reply(200, fixture(t, "wham_usage.json"), http.Header{"X-Codex-Primary-Used-Percent": {"77"}})
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), api, startWith(nil, errors.New("no binary"))))
	if err != nil {
		t.Fatal(err)
	}
	if observed.Source != "usage-api" || observed.Plan != "plus" || observed.AccountFP != fixtureFP {
		t.Fatalf("observation %+v", observed)
	}
	equalRows(t, rows(t, observed.Buckets), []bucketRow{
		{"5h", 77, "2026-10-29T01:00:00Z", "account", ""},
		{"7d", 10, "2026-10-29T00:00:00Z", "account", ""},
		{"5h", 7, "2026-10-29T00:00:00Z", "model", "GPT-5.5-Codex-Spark"},
		{"7d", 3, "2026-10-29T02:00:00Z", "model", "GPT-5.5-Codex-Spark"},
		{"credit", 75, "", "account", ""},
	})
}

func TestRPCFailuresFallBackToTheAPI(t *testing.T) {
	for name, text := range map[string]string{
		"auth error":          `{"id":1,"result":{}}` + "\n" + `{"id":2,"error":{"code":-32600,"message":"codex account authentication required to read rate limits"}}`,
		"empty non-free plan": `{"id":1,"result":{}}` + "\n" + `{"id":2,"result":{"rateLimits":{"planType":"pro"}}}`,
		"no measured window":  `{"id":1,"result":{}}` + "\n" + `{"id":2,"result":{"rateLimits":{"primary":{"windowDurationMins":300}}}}`,
		"bad result":          `{"id":1,"result":{}}` + "\n" + `{"id":2,"result":"nope"}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeRPC{lines: lines(text)}
			calls := 0
			api := doFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return reply(200, `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000}}}`, nil)(r)
			})
			observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), api, startWith(f, nil)))
			if err != nil || calls != 1 || observed.Source != "usage-api" || *observed.Buckets[0].UsedPct != 25 {
				t.Fatalf("%s must fall back: %+v err=%v calls=%d", name, observed, err, calls)
			}
			if f.closed == 0 {
				t.Fatal("transport not closed")
			}
		})
	}
}

func TestRPCFreePlan(t *testing.T) {
	f := &fakeRPC{lines: lines(`{"id":1,"result":{}}
{"id":2,"result":{"rateLimits":{"planType":"free"}}}
{"id":3,"result":{"account":null,"requiresOpenaiAuth":false}}`)}
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), nil, startWith(f, nil)))
	if err != nil || observed.Plan != "free" || len(observed.Buckets) != 1 || observed.Buckets[0].Window != "free" || *observed.Buckets[0].UsedPct != 0 {
		t.Fatalf("free plan %+v err=%v", observed, err)
	}
}

// The RPC answers: the API is never called. The file login's account_id, not
// the RPC's email, names the account.
func TestRPCPreferredAndAPIFallback(t *testing.T) {
	calls := 0
	api := doFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return reply(200, `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000}}}`, nil)(r)
	})
	f := &fakeRPC{lines: lines(`{"id":1,"result":{}}
{"id":2,"result":{"rateLimits":{"primary":{"usedPercent":55,"windowDurationMins":300,"resetsAt":1789980000}}}}
{"id":3,"result":{"account":{"email":"fixture@example.invalid"}}}`)}
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), api, startWith(f, nil)))
	if err != nil || calls != 0 || observed.Source != "usage-rpc" || *observed.Buckets[0].UsedPct != 55 || observed.AccountFP != fixtureFP {
		t.Fatalf("rpc path %+v err=%v calls=%d", observed, err, calls)
	}
	observed, err = Measure(t.Context(), config(authFiles("fixture-acct"), api, startWith(nil, errors.New("no binary"))))
	if err != nil || calls != 1 || observed.Source != "usage-api" || *observed.Buckets[0].UsedPct != 25 {
		t.Fatalf("api fallback %+v err=%v calls=%d", observed, err, calls)
	}
}

// No codex binary at all is an RPC failure like any other.
func TestNoBinaryFallsBackToTheAPI(t *testing.T) {
	cfg := config(authFiles("fixture-acct"), reply(200, fixture(t, "wham_usage.json"), nil), func(context.Context, string, string, string) (Transport, error) {
		t.Fatal("started with no binary")
		return nil, nil
	})
	cfg.LookPath = func(string) (string, error) { return "", errors.New("not found") }
	observed, err := Measure(t.Context(), cfg)
	if err != nil || observed.Source != "usage-api" {
		t.Fatalf("%+v %v", observed, err)
	}
}

// Without a token nothing is spawned and nothing is sent.
func TestNoTokenNoSpawnNoRequest(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"no file":      {},
		"no tokens":    {"/home/u/.codex/auth.json": `{"OPENAI_API_KEY":"sk-x"}`},
		"blank token":  {"/home/u/.codex/auth.json": `{"tokens":{"access_token":"","account_id":"a"}}`},
		"bad tokens":   {"/home/u/.codex/auth.json": `{"tokens":"nope"}`},
		"bad json":     {"/home/u/.codex/auth.json": `{`},
		"wrong type":   {"/home/u/.codex/auth.json": `{"tokens":{"access_token":7}}`},
		"oversize":     {"/home/u/.codex/auth.json": `{"tokens":{"access_token":"` + strings.Repeat("x", authFileLimit) + `"}}`},
		"other home":   {"/elsewhere/auth.json": `{"tokens":{"access_token":"t"}}`},
		"login in dir": {"/home/u/.codex": `{"tokens":{"access_token":"t"}}`},
	} {
		t.Run(name, func(t *testing.T) {
			api := doFunc(func(*http.Request) (*http.Response, error) { t.Fatal("request sent without a login"); return nil, nil })
			_, err := Measure(t.Context(), config(files, api, func(context.Context, string, string, string) (Transport, error) {
				t.Fatal("app-server started without a login")
				return nil, nil
			}))
			if quota.KindOf(err) != quota.ErrKindCredentials {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestAPIRequestHeaders(t *testing.T) {
	var got *http.Request
	api := doFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		return reply(200, fixture(t, "wham_usage.json"), nil)(r)
	})
	if _, err := Measure(t.Context(), config(authFiles("fixture-acct"), api, startWith(nil, errors.New("x")))); err != nil {
		t.Fatal(err)
	}
	if got.Method != "GET" || got.URL.String() != UsageEndpoint || got.Header.Get("Authorization") != "Bearer "+token || got.Header.Get("ChatGPT-Account-Id") != "fixture-acct" || got.Header.Get("Accept") != "application/json" {
		t.Fatalf("request %s %s %v", got.Method, got.URL, got.Header)
	}
	// No account_id: the header is omitted, and the account is unknown.
	observed, err := Measure(t.Context(), config(authFiles(""), api, startWith(nil, errors.New("x"))))
	if err != nil || got.Header.Get("ChatGPT-Account-Id") != "" || observed.AccountFP != "" {
		t.Fatalf("no account_id: %+v err=%v header=%v", observed, err, got.Header)
	}
}

func TestAPIErrorKinds(t *testing.T) {
	date := testNow.Add(90 * time.Second).UTC().Format(http.TimeFormat)
	for _, test := range []struct {
		name       string
		api        doFunc
		kind       string
		retryAfter time.Duration
	}{
		{"401", reply(401, `{}`, nil), quota.ErrKindAuth, 0},
		{"403", reply(403, `{}`, nil), quota.ErrKindAuth, 0},
		{"500", reply(500, `{}`, nil), quota.ErrKindHTTP, 0},
		{"429 with no header has no default wait", reply(429, `{}`, nil), quota.ErrKindRateLimited, 0},
		{"429 seconds", reply(429, `{}`, http.Header{"Retry-After": {"42"}}), quota.ErrKindRateLimited, 42 * time.Second},
		{"429 capped at a day", reply(429, `{}`, http.Header{"Retry-After": {"999999"}}), quota.ErrKindRateLimited, 86400 * time.Second},
		{"429 http-date", reply(429, `{}`, http.Header{"Retry-After": {date}}), quota.ErrKindRateLimited, 90 * time.Second},
		{"429 nonsense", reply(429, `{}`, http.Header{"Retry-After": {"soon"}}), quota.ErrKindRateLimited, 0},
		{"transport", func(*http.Request) (*http.Response, error) { return nil, errors.New("dial failed") }, quota.ErrKindNetwork, 0},
		{"bad json", reply(200, `<html>`, nil), quota.ErrKindParse, 0},
		{"trailing data", reply(200, `{"rate_limit":{"primary_window":{"used_percent":1,"limit_window_seconds":18000}}} {}`, nil), quota.ErrKindParse, 0},
		{"no windows", reply(200, `{"plan_type":"plus"}`, nil), quota.ErrKindParse, 0},
		{"no measured window", reply(200, `{"rate_limit":{"primary_window":{"limit_window_seconds":18000}}}`, nil), quota.ErrKindParse, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Measure(t.Context(), config(authFiles("fixture-acct"), test.api, startWith(nil, errors.New("x"))))
			var failure *quota.Error
			if !errors.As(err, &failure) || failure.Kind != test.kind || failure.RetryAfter != test.retryAfter {
				t.Fatalf("err = %#v", err)
			}
		})
	}
}

func TestWindowLabels(t *testing.T) {
	for seconds, want := range map[float64]string{
		2592000: "30d", 28 * 86400: "30d", 604800: "7d", 6 * 86400: "7d", 86400: "1d", 20 * 3600: "1d",
		18000: "5h", 4 * 3600: "5h", 6 * 3600: "5h", 3600: "3600s", 0: "0s", 90: "90s", 12.5: "12.5s", 6*3600 + 1: "21601s",
	} {
		if got := windowLabel(seconds); got != want {
			t.Errorf("windowLabel(%v) = %q, want %q", seconds, got, want)
		}
	}
}

// pacelever's TestCodexWindowsBySeconds: the label follows the window length,
// not the position.
func TestAPIWindowsBySeconds(t *testing.T) {
	body := `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":604800,"reset_at":1793232000},"secondary_window":{"used_percent":10,"limit_window_seconds":18000,"reset_at":1793232000}}}`
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), reply(200, body, nil), startWith(nil, errors.New("x"))))
	if err != nil || len(observed.Buckets) != 2 || observed.Buckets[0].Window != "7d" || observed.Buckets[1].Window != "5h" {
		t.Fatalf("codex windows by length %+v err=%v", observed, err)
	}
}

func TestAPIHeadersAndCredits(t *testing.T) {
	body := `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000,"reset_after_seconds":3600},"secondary_window":{"used_percent":10,"limit_window_seconds":604800,"reset_at":1793232000}},"credits":{"has_credits":true,"balance":"250"}}`
	header := http.Header{"X-Codex-Primary-Used-Percent": {"77"}}
	observed, err := Measure(t.Context(), config(authFiles("fixture-acct"), reply(200, body, header), startWith(nil, errors.New("x"))))
	if err != nil || len(observed.Buckets) != 3 {
		t.Fatalf("codex api %+v err=%v", observed, err)
	}
	if *observed.Buckets[0].UsedPct != 77 {
		t.Fatalf("header percent must override the body %+v", observed.Buckets[0])
	}
	if observed.Buckets[0].ResetsAt != testNow.Add(time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("reset_after_seconds %q", observed.Buckets[0].ResetsAt)
	}
	if credits := observed.Buckets[2]; credits.Window != "credit" || *credits.UsedPct != 75 || credits.Scope.Kind != quota.ScopeAccount {
		t.Fatalf("credits bucket %+v", credits)
	}

	// The balance header wins over the body; a header outside 0..100 does not
	// override a percentage; a balance above the scale measures nothing.
	header = http.Header{"X-Codex-Credits-Balance": {"500"}, "X-Codex-Secondary-Used-Percent": {"250"}}
	observed, err = Measure(t.Context(), config(authFiles("fixture-acct"), reply(200, body, header), startWith(nil, errors.New("x"))))
	if err != nil || *observed.Buckets[2].UsedPct != 50 || *observed.Buckets[1].UsedPct != 10 {
		t.Fatalf("header precedence %+v err=%v", observed.Buckets, err)
	}
	over := strings.Replace(body, `"250"`, `5000`, 1)
	observed, err = Measure(t.Context(), config(authFiles("fixture-acct"), reply(200, over, nil), startWith(nil, errors.New("x"))))
	if err != nil || observed.Buckets[2].UsedPct != nil {
		t.Fatalf("balance over the scale %+v err=%v", observed.Buckets, err)
	}
}

// pacelever's TestCodexLimitNameShortInputs, plus a multibyte first letter.
func TestLimitName(t *testing.T) {
	for in, want := range map[string]string{
		"o3": "o3", "Codex": "Codex", "codex": "codex", "codex_": "codex_", "codex ": "codex ",
		"codex_spark": "Spark", "Codex Spark": "Spark", "CODEX_spark": "Spark", "codex_élan": "Élan", "": "",
	} {
		if got := limitName(in); got != want {
			t.Errorf("limitName(%q) = %q, want %q", in, got, want)
		}
	}
}

// pacelever's TestCodexHomePrecedence.
func TestCodexHomePrecedence(t *testing.T) {
	env := map[string]string{"CODEX_HOME": "/env/codex"}
	cfg := Config{Home: "/home/u", CodexHome: "/sel/codex", LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
	if got := cfg.codexHome(); got != "/sel/codex" {
		t.Fatalf("selector not preferred: %s", got)
	}
	cfg.CodexHome = ""
	if got := cfg.codexHome(); got != "/env/codex" {
		t.Fatalf("environment not used: %s", got)
	}
	env["CODEX_HOME"] = "  "
	if got := cfg.codexHome(); got != filepath.Join("/home/u", ".codex") {
		t.Fatalf("default not used: %s", got)
	}
}

// pacelever's TestLocalCodexAccountMatchesPublishedKey: the key read before
// any request is the one a measurement publishes, and its stored form is
// pinned.
func TestAccountFPMatchesTheMeasurement(t *testing.T) {
	cfg := config(authFiles("acct-fixture"), reply(200, fixture(t, "wham_usage.json"), nil), startWith(nil, errors.New("x")))
	observed, err := Measure(t.Context(), cfg)
	if err != nil || AccountFP(cfg) == "" || AccountFP(cfg) != observed.AccountFP {
		t.Fatalf("local key %q and published key %q differ (err %v)", AccountFP(cfg), observed.AccountFP, err)
	}
	if fingerprint("fixture-acct") != fixtureFP {
		t.Fatal("account fingerprint changed its stored form")
	}
	if !HasLogin(cfg) || !HasLogin(config(authFiles(""), nil, nil)) || HasLogin(config(map[string]string{}, nil, nil)) {
		t.Fatal("HasLogin must follow the access token alone")
	}
	if AccountFP(config(authFiles(""), nil, nil)) != "" || AccountFP(config(map[string]string{}, nil, nil)) != "" || fingerprint("") != "" {
		t.Fatal("an unnamed account must have no fingerprint")
	}
}

func TestBinaryResolution(t *testing.T) {
	env := map[string]string{}
	files := map[string]bool{}
	cfg := Config{
		Home: "/home/u", GOOS: "darwin",
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		LookPath:  func(string) (string, error) { return "", errors.New("none") },
		IsFile:    func(p string) bool { return files[p] },
	}
	if got := cfg.binary(); got != "" {
		t.Fatalf("nothing installed: %q", got)
	}
	files["/home/u/Applications/Codex.app/Contents/Resources/codex"] = true
	if got := cfg.binary(); got != "/home/u/Applications/Codex.app/Contents/Resources/codex" {
		t.Fatalf("user app: %q", got)
	}
	files["/Applications/Codex.app/Contents/Resources/codex"] = true
	if got := cfg.binary(); got != "/Applications/Codex.app/Contents/Resources/codex" {
		t.Fatalf("system app first: %q", got)
	}
	cfg.GOOS = "linux"
	if got := cfg.binary(); got != "" {
		t.Fatalf("the app is looked up on darwin only: %q", got)
	}
	cfg.LookPath = func(string) (string, error) { return "/usr/bin/codex", nil }
	if got := cfg.binary(); got != "/usr/bin/codex" {
		t.Fatalf("PATH: %q", got)
	}
	cfg.Binary = "~/bin/codex"
	if got := cfg.binary(); got != "/home/u/bin/codex" {
		t.Fatalf("config before PATH, ~ expanded: %q", got)
	}
	env[BinaryOverrideEnv] = " /opt/codex "
	if got := cfg.binary(); got != "/opt/codex" {
		t.Fatalf("environment first: %q", got)
	}
	env[legacyBinaryOverrideEnv] = "/old/codex"
	if got := cfg.binary(); got != "/opt/codex" {
		t.Fatalf("new environment override must win: %q", got)
	}
	delete(env, BinaryOverrideEnv)
	if got := cfg.binary(); got != "/old/codex" {
		t.Fatalf("legacy environment fallback: %q", got)
	}
}

// The token reaches only the Authorization header: not an error, not the
// transport's start arguments.
func TestTokenNeverLeaves(t *testing.T) {
	leaky := doFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Get https://chatgpt.com: Authorization: Bearer " + token)
	})
	var started []string
	start := func(_ context.Context, binary, dir, home string) (Transport, error) {
		started = append(started, binary, dir, home)
		return nil, errors.New("spawn failed for " + token)
	}
	_, err := Measure(t.Context(), config(authFiles("fixture-acct"), leaky, start))
	if err == nil || strings.Contains(err.Error(), token) || quota.KindOf(err) != quota.ErrKindNetwork {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(strings.Join(started, " "), token) || len(started) != 3 {
		t.Fatalf("start arguments %v", started)
	}
	if started[1] != "/home/u/.codex" || started[2] != "/home/u/.codex" {
		t.Fatalf("the app-server runs in the codex home: %v", started)
	}
	for _, body := range []string{`{"rate_limit":{"primary_window":{"used_percent":1,"limit_window_seconds":18000}}} ` + token, token} {
		_, err = Measure(t.Context(), config(authFiles("fixture-acct"), reply(200, body, nil), startWith(nil, errors.New("x"))))
		if err == nil || strings.Contains(err.Error(), token) {
			t.Fatalf("err = %v", err)
		}
	}
	if _, err = Measure(t.Context(), config(authFiles("fixture-acct"), reply(500, token, nil), startWith(nil, errors.New("x")))); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("err = %v", err)
	}
}

// A server that never answers is closed when the exchange times out.
func TestRPCTimeoutClosesTheTransport(t *testing.T) {
	hung := &blockingRPC{released: make(chan struct{})}
	cfg := config(authFiles("fixture-acct"), reply(200, fixture(t, "wham_usage.json"), nil), func(context.Context, string, string, string) (Transport, error) { return hung, nil })
	cfg.RPCTimeout = 50 * time.Millisecond // simulated latency: the server never replies
	observed, err := Measure(t.Context(), cfg)
	if err != nil || observed.Source != "usage-api" {
		t.Fatalf("%+v %v", observed, err)
	}
	if !hung.isClosed() {
		t.Fatal("the hung transport was not closed")
	}
}

// Whatever the outcome, an RPC that decoded as JSON is valid input.
func TestRPCMessageDecoding(t *testing.T) {
	var msg rpcMessage
	if err := json.Unmarshal([]byte(`{"id":2,"error":{"code":1,"message":"m"}}`), &msg); err != nil || msg.Error == nil || string(msg.ID) != "2" {
		t.Fatalf("%+v %v", msg, err)
	}
}
