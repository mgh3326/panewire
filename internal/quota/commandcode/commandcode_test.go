package commandcode

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// secret is the API key of the test login. It may appear only in the
// Authorization header of a request.
const secret = "commandcode-api-key-must-not-leak"

// fixtureFP is hex(sha256("commandcode-account|user_fixture_1"))[:16], the
// fingerprint of pacelever's accountFP("commandcode", "user_fixture_1")
// (account.go: fingerprint(pool+"-account", account)), computed with sha256sum.
const fixtureFP = "71430d9ff98e30c3"

var testNow = time.Date(2026, 11, 6, 5, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// api answers by request path (without the query) and records every request.
type api struct {
	mu       sync.Mutex
	replies  map[string]func() (*http.Response, error)
	requests []*http.Request
}

func (a *api) Do(r *http.Request) (*http.Response, error) {
	a.mu.Lock()
	a.requests = append(a.requests, r)
	reply := a.replies[r.URL.Path]
	a.mu.Unlock()
	if reply == nil {
		return nil, errors.New("no reply for " + r.URL.Path)
	}
	return reply()
}

func (a *api) paths() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var paths []string
	for _, r := range a.requests {
		paths = append(paths, r.URL.RequestURI())
	}
	return strings.Join(paths, " ")
}

func respond(status int, body string) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

// fixtureAPI serves the four 2026-11-06 fixture bodies.
func fixtureAPI(t *testing.T) *api {
	return &api{replies: map[string]func() (*http.Response, error){
		"/alpha/whoami":                respond(200, fixture(t, "whoami.json")),
		"/alpha/billing/credits":       respond(200, fixture(t, "credits.json")),
		"/alpha/usage/summary":         respond(200, fixture(t, "usage_summary.json")),
		"/alpha/billing/subscriptions": respond(200, fixture(t, "subscriptions.json")),
	}}
}

func config(a Doer, files map[string]string, env map[string]string) Config {
	return Config{
		HTTP: a, Now: func() time.Time { return testNow }, Home: "/home/u",
		LookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
		ReadFile: func(path string) ([]byte, error) {
			if body, ok := files[path]; ok {
				return []byte(body), nil
			}
			return nil, os.ErrNotExist
		},
	}
}

func login() map[string]string {
	return map[string]string{"/home/u/.commandcode/auth.json": `{"apiKey":"` + secret + `"}`}
}

func bucketOf(t *testing.T, o quota.Observation, window string) quota.Bucket {
	t.Helper()
	for _, b := range o.Buckets {
		if b.Window == window {
			return b
		}
	}
	t.Fatalf("no %s bucket in %+v", window, o.Buckets)
	return quota.Bucket{}
}

func hasBucket(o quota.Observation, window string) bool {
	for _, b := range o.Buckets {
		if b.Window == window {
			return true
		}
	}
	return false
}

// Acceptance 5: the 2026-11-06 shapes give 30d ≈ 9.3% resetting at
// currentPeriodEnd, 7d 1.3%, 5h 0%, under pacelever's account fingerprint.
func TestMeasureFixtureYieldsThirtyDayWindow(t *testing.T) {
	a := fixtureAPI(t)
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.Buckets) != 3 || observed.Source != quota.SourceUsageAPI || observed.Plan != "individual-goat" || observed.AccountFP != fixtureFP {
		t.Fatalf("observation %+v", observed)
	}
	month := bucketOf(t, observed, "30d")
	if want := 100 * 6.52 / 70; math.Abs(*month.UsedPct-want) > 1e-9 || math.Round(*month.UsedPct*10)/10 != 9.3 {
		t.Fatalf("30d = %v, want %v", *month.UsedPct, want)
	}
	if month.ResetsAt != "2026-11-27T07:38:00Z" || month.Scope.Kind != quota.ScopeAccount {
		t.Fatalf("30d bucket %+v", month)
	}
	// hk 4330: 7d rolling window carries no ResetsAt (unpaceable).
	if week := bucketOf(t, observed, "7d"); math.Round(*week.UsedPct*10)/10 != 1.3 || week.ResetsAt != "" {
		t.Fatalf("7d bucket %+v", week)
	}
	if five := bucketOf(t, observed, "5h"); *five.UsedPct != 0 || five.ResetsAt != "2026-11-06T09:00:00Z" {
		t.Fatalf("5h bucket %+v", five)
	}
	// whoami names the org the credits request is scoped to.
	if got := a.paths(); got != "/alpha/whoami?limits=1 /alpha/billing/credits?orgId=org_fixture_1 /alpha/usage/summary /alpha/billing/subscriptions" {
		t.Fatalf("requests: %s", got)
	}
	for _, r := range a.requests {
		if r.Header.Get("Authorization") != "Bearer "+secret || r.Header.Get("User-Agent") != "cmd/1.88.1" || r.Method != http.MethodGet {
			t.Fatalf("request %s headers %v", r.URL.RequestURI(), r.Header)
		}
	}
}

// Acceptance 6: no usage summary, no 30d bucket — the 5h and 7d windows stay.
func TestMeasureWithoutUsageSummaryHasNoThirtyDay(t *testing.T) {
	for name, reply := range map[string]func() (*http.Response, error){
		"not found":      respond(404, `{"error":"nope"}`),
		"server error":   respond(500, "boom"),
		"rate limited":   respond(429, `{}`),
		"unauthorized":   respond(401, `{}`),
		"malformed":      respond(200, `{"data":`),
		"network":        func() (*http.Response, error) { return nil, errors.New("dial " + secret) },
		"no total":       respond(200, `{"data":{"periodBasis":"billing-period"}}`),
		"negative total": respond(200, `{"data":{"totalMonthlyCredits":-1,"periodBasis":"billing-period"}}`),
		"string total":   respond(200, `{"data":{"totalMonthlyCredits":"6.52","periodBasis":"billing-period"}}`),
		"other basis":    respond(200, `{"data":{"totalMonthlyCredits":6.52,"periodBasis":"calendar-month"}}`),
	} {
		t.Run(name, func(t *testing.T) {
			a := fixtureAPI(t)
			a.replies["/alpha/usage/summary"] = reply
			observed, err := Measure(t.Context(), config(a, login(), nil))
			if err != nil {
				t.Fatal(err)
			}
			if hasBucket(observed, "30d") || len(observed.Buckets) != 2 || !hasBucket(observed, "5h") || !hasBucket(observed, "7d") {
				t.Fatalf("buckets %+v", observed.Buckets)
			}
		})
	}
	// The summary is not even asked for when the credits response gives no
	// monthly remainder to measure against.
	a := fixtureAPI(t)
	a.replies["/alpha/billing/credits"] = respond(200, `{"data":{"windowLimits":{"weekly":{"used":1,"cap":35}}}}`)
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil || hasBucket(observed, "30d") || len(observed.Buckets) != 1 || strings.Contains(a.paths(), "usage/summary") {
		t.Fatalf("observed %+v err=%v paths=%s", observed, err, a.paths())
	}
}

// A missing subscription keeps the 30d value and leaves its reset unknown,
// and the plan name too: nothing is made up.
func TestMeasureWithoutSubscriptionKeepsThirtyDayWithoutReset(t *testing.T) {
	a := fixtureAPI(t)
	a.replies["/alpha/billing/subscriptions"] = respond(500, "boom")
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if month := bucketOf(t, observed, "30d"); month.ResetsAt != "" || observed.Plan != "" || len(observed.Buckets) != 3 {
		t.Fatalf("observation %+v", observed)
	}
}

// Purchased and free credits are outside the plan's allowance: they change
// neither the 30d value nor its denominator.
func TestMeasureExcludesPurchasedAndFreeCredits(t *testing.T) {
	a := fixtureAPI(t)
	a.replies["/alpha/billing/credits"] = respond(200, strings.Replace(fixture(t, "credits.json"), `"purchasedCredits":0,"freeCredits":0`, `"purchasedCredits":500,"freeCredits":25`, 1))
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if want := 100 * 6.52 / 70; math.Abs(*bucketOf(t, observed, "30d").UsedPct-want) > 1e-9 {
		t.Fatalf("30d = %v, want %v", *bucketOf(t, observed, "30d").UsedPct, want)
	}
}

// An exhausted plan reads 100%, and a window past its cap is clamped to 100.
func TestMeasureExhaustedWindows(t *testing.T) {
	a := fixtureAPI(t)
	a.replies["/alpha/billing/credits"] = respond(200, `{"windowLimits":{"fiveHour":{"used":20,"cap":14},"weekly":{"used":35,"cap":35}},"credits":{"monthlyCredits":-3}}`)
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, window := range []string{"5h", "7d", "30d"} {
		if got := *bucketOf(t, observed, window).UsedPct; got != 100 {
			t.Fatalf("%s = %v", window, got)
		}
	}
}

// A window with a malformed value is left out; it never reads as unused.
func TestMeasureMalformedWindowsAreLeftOut(t *testing.T) {
	a := fixtureAPI(t)
	a.replies["/alpha/billing/credits"] = respond(200, `{"windowLimits":{"fiveHour":{"used":"0","cap":14},"weekly":{"used":-1,"cap":35}},"credits":{"monthlyCredits":"63.48"}}`)
	if _, err := Measure(t.Context(), config(a, login(), nil)); quota.KindOf(err) != quota.ErrKindParse {
		t.Fatalf("err = %v", err)
	}
}

// Acceptance 1: auth missing, refused, edge-rejected, rate limited, malformed,
// timed out — each a kind and a fixed sentence, none carrying the key.
func TestMeasureFailureKinds(t *testing.T) {
	retry := func() (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"90"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}
	for _, test := range []struct {
		name   string
		files  map[string]string
		env    map[string]string
		path   string
		reply  func() (*http.Response, error)
		kind   string
		sends  bool
		retain time.Duration
	}{
		{name: "no login", files: map[string]string{}, kind: quota.ErrKindCredentials},
		{name: "empty key", files: map[string]string{"/home/u/.commandcode/auth.json": `{"apiKey":"  "}`}, kind: quota.ErrKindCredentials},
		{name: "unreadable login", files: map[string]string{"/home/u/.commandcode/auth.json": `{`}, kind: quota.ErrKindCredentials},
		{name: "refused key", path: "/alpha/whoami", reply: respond(401, `{"error":"unauthorized"}`), kind: quota.ErrKindAuthFailed, sends: true},
		{name: "forbidden json", path: "/alpha/billing/credits", reply: respond(403, `{"error":"forbidden"}`), kind: quota.ErrKindAuthFailed, sends: true},
		{name: "edge rejection", path: "/alpha/whoami", reply: respond(403, `<html>blocked</html>`), kind: quota.ErrKindEdgeRejected, sends: true},
		{name: "rate limited", path: "/alpha/billing/credits", reply: retry, kind: quota.ErrKindRateLimited, sends: true, retain: 90 * time.Second},
		{name: "bare rate limit", path: "/alpha/whoami", reply: respond(429, `{}`), kind: quota.ErrKindRateLimited, sends: true},
		{name: "server error", path: "/alpha/billing/credits", reply: respond(500, "boom"), kind: quota.ErrKindHTTP, sends: true},
		{name: "rejected body", path: "/alpha/whoami", reply: respond(200, `{"success":false}`), kind: quota.ErrKindHTTP, sends: true},
		{name: "malformed whoami", path: "/alpha/whoami", reply: respond(200, `not json`), kind: quota.ErrKindParse, sends: true},
		{name: "malformed credits", path: "/alpha/billing/credits", reply: respond(200, `{"data":`), kind: quota.ErrKindParse, sends: true},
		{name: "no windows", path: "/alpha/billing/credits", reply: respond(200, `{"data":{}}`), kind: quota.ErrKindParse, sends: true},
		{name: "network", path: "/alpha/billing/credits", reply: func() (*http.Response, error) { return nil, errors.New("Get https://x: Bearer " + secret) }, kind: quota.ErrKindNetwork, sends: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := fixtureAPI(t)
			if test.path != "" {
				a.replies[test.path] = test.reply
			}
			files := test.files
			if files == nil {
				files = login()
			}
			_, err := Measure(t.Context(), config(a, files, test.env))
			var failure *quota.Error
			if !errors.As(err, &failure) || failure.Kind != test.kind {
				t.Fatalf("err = %v, want kind %s", err, test.kind)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "http") && test.kind != quota.ErrKindHTTP {
				t.Fatalf("err leaks: %v", err)
			}
			if failure.RetryAfter != test.retain {
				t.Fatalf("RetryAfter = %v, want %v", failure.RetryAfter, test.retain)
			}
			if !test.sends && len(a.requests) != 0 {
				t.Fatalf("a request was sent without a login: %s", a.paths())
			}
		})
	}
}

// A deadline that passes during a request is a timeout, and a deadline that
// passes during a secondary request does not publish a partial reading.
func TestMeasureTimeout(t *testing.T) {
	hang := func(ctx context.Context) func() (*http.Response, error) {
		return func() (*http.Response, error) {
			<-ctx.Done() // simulated latency: the server never answers
			return nil, ctx.Err()
		}
	}
	for _, path := range []string{"/alpha/whoami", "/alpha/billing/credits", "/alpha/usage/summary", "/alpha/billing/subscriptions"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			a := fixtureAPI(t)
			a.replies[path] = hang(ctx)
			if _, err := Measure(ctx, config(a, login(), nil)); quota.KindOf(err) != quota.ErrKindTimeout {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

// The key comes from pacelever's two environment names first, then auth.json.
func TestTokenSources(t *testing.T) {
	for _, test := range []struct {
		name  string
		files map[string]string
		env   map[string]string
		want  string
	}{
		{"file", login(), nil, secret},
		{"primary env wins", login(), map[string]string{"COMMAND_CODE_API_KEY": " env-one ", "COMMANDCODE_API_KEY": "env-two"}, "env-one"},
		{"alias env", login(), map[string]string{"COMMANDCODE_API_KEY": "env-two"}, "env-two"},
		{"env without file", map[string]string{}, map[string]string{"COMMAND_CODE_API_KEY": "env-one"}, "env-one"},
		{"none", map[string]string{}, nil, ""},
	} {
		if got := config(nil, test.files, test.env).token(); got != test.want {
			t.Errorf("%s: token = %q, want %q", test.name, got, test.want)
		}
	}
}

// The fingerprint read before a refresh asks whoami once per key, then comes
// from the cache; a different key asks again. It is the one a measurement
// attaches, so the shared key and the lease agree.
func TestAccountFPIsCachedPerKey(t *testing.T) {
	a := fixtureAPI(t)
	cfg := config(a, login(), nil)
	cfg.Identities = &IdentityCache{}
	for range 3 {
		if fp, err := AccountFP(t.Context(), cfg); err != nil || fp != fixtureFP {
			t.Fatalf("fp=%q err=%v", fp, err)
		}
	}
	if len(a.requests) != 1 {
		t.Fatalf("whoami asked %d times", len(a.requests))
	}
	observed, err := Measure(t.Context(), cfg)
	if err != nil || observed.AccountFP != fixtureFP {
		t.Fatalf("measurement fp=%q err=%v", observed.AccountFP, err)
	}
	cfg.ReadFile = func(string) ([]byte, error) { return []byte(`{"apiKey":"another-key"}`), nil }
	before := len(a.requests)
	if _, err := AccountFP(t.Context(), cfg); err != nil || len(a.requests) != before+1 {
		t.Fatalf("a new key was answered from the cache: err=%v requests %d→%d", err, before, len(a.requests))
	}

	if _, err := AccountFP(t.Context(), config(a, map[string]string{}, nil)); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("no login: %v", err)
	}
}

// With no user the org names the account, and with neither the observation
// carries no fingerprint (the node then measures but does not share it).
func TestAccountFallsBackToOrgThenNothing(t *testing.T) {
	a := fixtureAPI(t)
	a.replies["/alpha/whoami"] = respond(200, `{"data":{"org":{"id":"org_fixture_1"}}}`)
	observed, err := Measure(t.Context(), config(a, login(), nil))
	if err != nil || observed.AccountFP != fingerprint("org_fixture_1") || observed.AccountFP == "" {
		t.Fatalf("fp=%q err=%v", observed.AccountFP, err)
	}
	a.replies["/alpha/whoami"] = respond(200, `{"data":{}}`)
	observed, err = Measure(t.Context(), config(a, login(), nil))
	if err != nil || observed.AccountFP != "" {
		t.Fatalf("fp=%q err=%v", observed.AccountFP, err)
	}
	if strings.Contains(a.paths(), "orgId") && strings.Count(a.paths(), "orgId") != 1 {
		t.Fatalf("an org-less account sent an orgId: %s", a.paths())
	}
}

func TestResetTime(t *testing.T) {
	for _, test := range []struct {
		in   any
		want string
	}{
		{float64(1793955600), "2026-11-06T09:00:00Z"},
		{float64(1793955600000), "2026-11-06T09:00:00Z"},
		{float64(1793955600000000), "2026-11-06T09:00:00Z"},
		{"1793955600000", "2026-11-06T09:00:00Z"},
		{"2026-11-27T09:38:00+02:00", "2026-11-27T07:38:00Z"},
		{"Fri, 06 Nov 2026 09:00:00 GMT", "2026-11-06T09:00:00Z"},
		{"", ""},
		{"soon", ""},
		{float64(0), ""},
		{float64(5), ""},
		{math.NaN(), ""},
		{nil, ""},
		{true, ""},
		{map[string]any{}, ""},
	} {
		if got := resetTime(test.in); got != test.want {
			t.Errorf("resetTime(%v) = %q, want %q", test.in, got, test.want)
		}
	}
}

// A whoami that names no account is not remembered: the next fingerprint read
// asks again and picks the account up once the API names it, instead of
// answering "no account" for as long as the key stays the same.
func TestAccountFPRetriesAfterAnEmptyWhoami(t *testing.T) {
	a := fixtureAPI(t)
	good := a.replies["/alpha/whoami"]
	a.replies["/alpha/whoami"] = respond(200, `{"data":{}}`)
	cfg := config(a, login(), nil)
	cfg.Identities = &IdentityCache{}
	if fp, err := AccountFP(t.Context(), cfg); err != nil || fp != "" {
		t.Fatalf("fp=%q err=%v", fp, err)
	}
	a.replies["/alpha/whoami"] = good
	if fp, err := AccountFP(t.Context(), cfg); err != nil || fp != fixtureFP {
		t.Fatalf("after recovery fp=%q err=%v", fp, err)
	}
	before := len(a.requests)
	if fp, _ := AccountFP(t.Context(), cfg); fp != fixtureFP || len(a.requests) != before {
		t.Fatalf("a named account was not remembered: fp=%q requests %d→%d", fp, before, len(a.requests))
	}
}
