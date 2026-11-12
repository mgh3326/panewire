package agy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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

// token is the OAuth access token of the test login, and csrf the app's CSRF
// token. Each may appear only in a request header.
const (
	token = "agy-oauth-token-must-not-leak"
	csrf  = "agy-csrf-token-must-not-leak"
)

var testNow = time.Date(2026, 11, 6, 5, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// server answers by request path and records every request.
type server struct {
	mu       sync.Mutex
	replies  map[string]func(*http.Request) (*http.Response, error)
	requests []*http.Request
}

func (s *server) Do(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.requests = append(s.requests, r)
	reply := s.replies[r.URL.Path]
	s.mu.Unlock()
	if reply == nil {
		return nil, errors.New("no reply for " + r.URL.Path)
	}
	return reply(r)
}

func (s *server) urls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var urls []string
	for _, r := range s.requests {
		urls = append(urls, r.URL.Scheme+"://"+r.URL.Host+r.URL.Path)
	}
	return urls
}

func respond(status int, body string) func(*http.Request) (*http.Response, error) {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

func cloudServer(t *testing.T) *server {
	return &server{replies: map[string]func(*http.Request) (*http.Response, error){
		"/v1internal:retrieveUserQuotaSummary": respond(200, fixture(t, "summary.json")),
		"/v1internal:fetchAvailableModels":     respond(200, fixture(t, "models.json")),
	}}
}

func loginFiles(access string) map[string]string {
	if access == "" {
		return map[string]string{}
	}
	return map[string]string{"/home/u/.gemini/antigravity-cli/antigravity-oauth-token": `{"token":{"access_token":"` + access + `"}}`}
}

// config is a measurement over fakes: no app process, the given login files and
// cloud server, no keychain or secret service, no agy binary.
func config(files map[string]string, cloud Doer) Config {
	return Config{
		HTTP: cloud, Home: "/home/u", GOOS: "linux", Now: func() time.Time { return testNow },
		LookupEnv: func(string) (string, bool) { return "", false },
		ReadFile: func(path string) ([]byte, error) {
			if body, ok := files[path]; ok {
				return []byte(body), nil
			}
			return nil, os.ErrNotExist
		},
		SecretService: func(context.Context) (string, error) { return "", errors.New("no secret service") },
		LookPath:      func(string) (string, error) { return "", errors.New("not found") },
		Processes:     func(context.Context) []Proc { return nil },
		ListenPorts:   func(context.Context, int) []int { return nil },
		Locked:        func(_ string, fn func() error) error { return fn() },
	}
}

func near(a *float64, want float64) bool { return a != nil && math.Abs(*a-want) < 0.001 }

// group returns the bucket of group and window, or nil.
func group(buckets []quota.Bucket, name, window string) *quota.Bucket {
	for i, b := range buckets {
		if b.Scope.Kind == quota.ScopeGroup && b.Scope.Name == name && b.Window == window {
			return &buckets[i]
		}
	}
	return nil
}

// The cloud summary names each group's windows; every bucket is group-scoped
// with the group as its scope name, never account-scoped.
func TestMeasureCloudSummaryEveryGroupWithItsScope(t *testing.T) {
	cloud := cloudServer(t)
	observed, err := Measure(context.Background(), config(loginFiles(token), cloud))
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range observed.Buckets {
		if b.Scope.Kind != quota.ScopeGroup || b.Scope.Name == "" {
			t.Fatalf("a bucket is not group-scoped: %+v", b)
		}
	}
	type want struct {
		group, window string
		used          float64
		reset         string
	}
	// Order: gemini, 3p, unknown; by window length inside a group. The models
	// list adds the unmapped "mystery-1" session window as group unknown and
	// leaves out the internal model and the unrecognised monthly window's
	// session masquerade.
	wants := []want{
		{"gemini", "5h", 20, "2026-11-06T09:00:00Z"},
		{"gemini", "7d", 20, "2026-11-10T01:00:00Z"},
		{"3p", "5h", 50, "2026-11-06T09:00:00Z"},
		{"3p", "7d", 60, "2026-11-10T01:00:00Z"},
		{"unknown", "5h", 50, "2026-11-06T09:00:00Z"},
		{"unknown", "7d", 90, "2026-11-10T01:00:00Z"},
		{"unknown", "unknown", 10, "2026-11-06T09:00:00Z"},
	}
	if len(observed.Buckets) != len(wants) {
		t.Fatalf("buckets %+v", observed.Buckets)
	}
	for i, w := range wants {
		b := observed.Buckets[i]
		if b.Scope.Name != w.group || b.Window != w.window || !near(b.UsedPct, w.used) || b.ResetsAt != w.reset {
			t.Errorf("bucket %d = %+v, want %+v", i, b, w)
		}
	}
	if observed.Source != quota.SourceUsageAPI || observed.Plan != "GOOGLE AI PRO" {
		t.Fatalf("source %q plan %q", observed.Source, observed.Plan)
	}
	urls := cloud.urls()
	if len(urls) != 2 || urls[0] != "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary" || urls[1] != "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels" {
		t.Fatalf("requests %v", urls)
	}
	for _, r := range cloud.requests {
		if r.Header.Get("Authorization") != "Bearer "+token || r.Method != http.MethodPost || r.Header.Get("User-Agent") != UserAgent {
			t.Fatalf("request %s %s auth %q ua %q", r.Method, r.URL, r.Header.Get("Authorization"), r.Header.Get("User-Agent"))
		}
	}
}

// A group the summary omits is filled from its models' session quota.
func TestMeasureMergesTheMissingGroupFromTheModelList(t *testing.T) {
	summary := `{"groups":[{"displayName":"Claude and GPT models","buckets":[{"bucketId":"3p-5h","window":"5h","remainingFraction":0.5,"resetTime":"2026-11-06T09:00:00Z"}]}]}`
	cloud := &server{replies: map[string]func(*http.Request) (*http.Response, error){
		"/v1internal:retrieveUserQuotaSummary": respond(200, summary),
		"/v1internal:fetchAvailableModels":     respond(200, fixture(t, "models.json")),
	}}
	observed, err := Measure(context.Background(), config(loginFiles(token), cloud))
	if err != nil {
		t.Fatal(err)
	}
	if b := group(observed.Buckets, "gemini", "5h"); b == nil || !near(b.UsedPct, 0.3521) {
		t.Fatalf("gemini from the models list: %+v", observed.Buckets)
	}
	// The summary's 50% is the worse 3p reading; the model's 0% does not hide it.
	if b := group(observed.Buckets, "3p", "5h"); b == nil || !near(b.UsedPct, 50) {
		t.Fatalf("3p keeps the worst reading: %+v", observed.Buckets)
	}
}

// A group absent from the answer is absent from the reading: another group's
// numbers never stand in for it.
func TestMeasureDoesNotInventAMissingGroup(t *testing.T) {
	summary := `{"groups":[{"displayName":"Claude and GPT models","buckets":[{"bucketId":"3p-5h","window":"5h","remainingFraction":0.5}]}]}`
	cloud := &server{replies: map[string]func(*http.Request) (*http.Response, error){
		"/v1internal:retrieveUserQuotaSummary": respond(200, summary),
	}}
	observed, err := Measure(context.Background(), config(loginFiles(token), cloud))
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.Buckets) != 1 || group(observed.Buckets, "3p", "5h") == nil || group(observed.Buckets, "gemini", "5h") != nil {
		t.Fatalf("buckets %+v", observed.Buckets)
	}
	if observed.Buckets[0].ResetsAt != "" {
		t.Fatalf("a window with no reset got one: %+v", observed.Buckets[0])
	}
}

func TestMeasureModelListFallbackWindows(t *testing.T) {
	body := `{"models":{"gemini-3.7-flash":{"quotaInfo":{"remainingFraction":0.5,"window":"weekly","resetTime":"2026-11-10T01:00:00Z"}},"claude-3.7":{"quotaInfo":{"remainingFraction":0.25}}}}`
	observed, err := parse(mustObject(t, body), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(observed.Buckets) != 2 || group(observed.Buckets, "gemini", "7d") == nil || group(observed.Buckets, "3p", "5h") == nil {
		t.Fatalf("buckets %+v", observed.Buckets)
	}
}

func TestParseRefusesAnAnswerWithNoWindow(t *testing.T) {
	for _, body := range []string{`{}`, `{"groups":[]}`, `{"groups":[{"displayName":"Gemini","buckets":[{"bucketId":"gemini-5h","window":"5h"}]}]}`, `{"groups":[{"displayName":"Gemini","buckets":[{"bucketId":"gemini-5h","window":"5h","remainingFraction":2}]}]}`} {
		if _, err := parse(mustObject(t, body), nil); quota.KindOf(err) != quota.ErrKindParse {
			t.Errorf("%s: err %v, want parse_error", body, err)
		}
	}
}

func TestGroupMapping(t *testing.T) {
	for _, c := range []struct{ id, display, want string }{
		{"gemini-3.7-flash", "", GroupGemini},
		{"gemini-5h", "", GroupGemini},
		{"3p-5h", "", Group3P},
		{"3p_weekly", "", Group3P},
		{"claude-sonnet-4-6", "", Group3P},
		{"claude-opus-4-6-thinking", "", Group3P},
		{"gpt-oss-120b-medium", "", Group3P},
		{"model-x", "Gemini X", GroupGemini},
		{"model-x", "Claude and GPT models", Group3P},
		{"mystery-1", "", GroupUnknown},
		{"gemini-argon-wtu-weekly", "", GroupUnknown},
		{"gemini-argon-model", "", GroupUnknown},
		{"model-argon", "Argon Weekly", GroupUnknown},
		{"", "", GroupUnknown},
	} {
		if got := Group(c.id, c.display); got != c.want {
			t.Errorf("Group(%q, %q) = %q, want %q", c.id, c.display, got, c.want)
		}
	}
}

// The live-shaped response yields gemini 7d and 5h, 3p 7d and 5h; an exhausted
// Argon weekly bucket is published under group unknown and does not fold into
// gemini 7d (so flash38 rungs are not blocked).
func TestSummaryExactBucketMatchingAndArgonSeparation(t *testing.T) {
	summary := `{
		"groups": [
			{
				"displayName": "Gemini Models",
				"buckets": [
					{"bucketId": "gemini-5h", "window": "5h", "remainingFraction": 0.95, "resetTime": "2026-11-12T09:00:00Z"},
					{"bucketId": "gemini-weekly", "window": "weekly", "remainingFraction": 0.75, "resetTime": "2026-11-19T01:00:00Z"},
					{"bucketId": "gemini-argon-wtu-weekly", "window": "weekly", "remainingFraction": 0.0, "resetTime": "2026-11-19T01:00:00Z"}
				]
			},
			{
				"displayName": "Claude and GPT models",
				"buckets": [
					{"bucketId": "3p-5h", "window": "5h", "remainingFraction": 0.1, "resetTime": "2026-11-12T08:00:00Z"},
					{"bucketId": "3p-weekly", "window": "weekly", "remainingFraction": 0.5, "resetTime": "2026-11-19T03:00:00Z"}
				]
			}
		]
	}`
	observed, err := parse(mustObject(t, summary), nil)
	if err != nil {
		t.Fatal(err)
	}

	gemini5h := group(observed.Buckets, "gemini", "5h")
	if gemini5h == nil || !near(gemini5h.UsedPct, 5) {
		t.Fatalf("gemini 5h: %+v", gemini5h)
	}

	gemini7d := group(observed.Buckets, "gemini", "7d")
	if gemini7d == nil || !near(gemini7d.UsedPct, 25) {
		t.Fatalf("gemini 7d must be 25%% (from gemini-weekly), not tainted by argon: %+v", gemini7d)
	}

	threeP5h := group(observed.Buckets, "3p", "5h")
	if threeP5h == nil || !near(threeP5h.UsedPct, 90) {
		t.Fatalf("3p 5h: %+v", threeP5h)
	}

	threeP7d := group(observed.Buckets, "3p", "7d")
	if threeP7d == nil || !near(threeP7d.UsedPct, 50) {
		t.Fatalf("3p 7d: %+v", threeP7d)
	}

	argon := group(observed.Buckets, "unknown", "7d")
	if argon == nil || !near(argon.UsedPct, 100) {
		t.Fatalf("argon weekly must be published under group unknown: %+v", argon)
	}
}

// Unknown buckets and unrecognized windows are logged by id and published
// under group unknown rather than silently dropped.
func TestSummaryUnknownBucketsLoggedAndNotDropped(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	summary := `{
		"groups": [
			{
				"displayName": "Gemini Models",
				"buckets": [
					{"bucketId": "gemini-5h", "window": "5h", "remainingFraction": 0.8},
					{"bucketId": "future-experiment-bucket", "window": "daily", "remainingFraction": 0.6},
					{"bucketId": "strange-window-bucket", "window": "quarterly", "remainingFraction": 0.4}
				]
			}
		]
	}`
	observed, err := parse(mustObject(t, summary), nil)
	if err != nil {
		t.Fatal(err)
	}

	daily := group(observed.Buckets, "unknown", "1d")
	if daily == nil || !near(daily.UsedPct, 40) {
		t.Fatalf("unknown daily bucket not published: %+v", observed.Buckets)
	}

	unknown := group(observed.Buckets, "unknown", "unknown")
	if unknown == nil || !near(unknown.UsedPct, 60) {
		t.Fatalf("unknown quarterly window bucket not published: %+v", observed.Buckets)
	}

	logs := buf.String()
	if !strings.Contains(logs, "future-experiment-bucket") {
		t.Fatalf("expected log mentioning future-experiment-bucket, got %q", logs)
	}
	if !strings.Contains(logs, "strange-window-bucket") {
		t.Fatalf("expected log mentioning strange-window-bucket, got %q", logs)
	}
}

// The configured account fingerprint travels on the observation: it is the
// account measured.
func TestMeasureCarriesTheConfiguredAccount(t *testing.T) {
	cfg := config(loginFiles(token), cloudServer(t))
	cfg.Account = "0123456789abcdef"
	observed, err := Measure(context.Background(), cfg)
	if err != nil || observed.AccountFP != "0123456789abcdef" {
		t.Fatalf("account %q err %v", observed.AccountFP, err)
	}
}

func TestMeasureFallsBackToTheSecondHost(t *testing.T) {
	var hosts []string
	cloud := doer(func(r *http.Request) (*http.Response, error) {
		hosts = append(hosts, r.URL.Host)
		if r.URL.Host == "daily-cloudcode-pa.googleapis.com" {
			return nil, errors.New("down")
		}
		if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
			return respond(200, `{"models":{}}`)(r)
		}
		return respond(200, fixture(t, "summary.json"))(r)
	})
	if _, err := Measure(context.Background(), config(loginFiles(token), cloud)); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(hosts, " "); !strings.HasPrefix(got, "daily-cloudcode-pa.googleapis.com cloudcode-pa.googleapis.com") {
		t.Fatalf("hosts %v", hosts)
	}
}

type doer func(*http.Request) (*http.Response, error)

func (d doer) Do(r *http.Request) (*http.Response, error) { return d(r) }

// Error kinds follow pacelever's: no login credentials; a refused login
// auth_failed; 429 rate_limited with Retry-After; a server error a retryable
// network failure; another status http; unreadable JSON parse_error; nothing
// answering network.
func TestMeasureErrorKinds(t *testing.T) {
	status := func(code int, header http.Header, body string) Doer {
		return doer(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
		})
	}
	for name, c := range map[string]struct {
		files map[string]string
		cloud Doer
		want  string
	}{
		"no login":      {loginFiles(""), status(200, nil, `{}`), quota.ErrKindCredentials},
		"refused 401":   {loginFiles(token), status(401, nil, `{}`), quota.ErrKindAuthFailed},
		"refused 403":   {loginFiles(token), status(403, nil, `{}`), quota.ErrKindAuthFailed},
		"rate limited":  {loginFiles(token), status(429, http.Header{"Retry-After": {"120"}}, `{}`), quota.ErrKindRateLimited},
		"server error":  {loginFiles(token), status(503, nil, `{}`), quota.ErrKindNetwork},
		"other status":  {loginFiles(token), status(404, nil, `{}`), quota.ErrKindHTTP},
		"not json":      {loginFiles(token), status(200, nil, `<html>`), quota.ErrKindParse},
		"no windows":    {loginFiles(token), status(200, nil, `{"groups":[]}`), quota.ErrKindParse},
		"nothing there": {loginFiles(token), doer(func(*http.Request) (*http.Response, error) { return nil, errors.New("down") }), quota.ErrKindNetwork},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Measure(context.Background(), config(c.files, c.cloud))
			if quota.KindOf(err) != c.want {
				t.Fatalf("err %v, want kind %s", err, c.want)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("the token is in the error: %v", err)
			}
		})
	}
	_, err := Measure(context.Background(), config(loginFiles(token), status(429, http.Header{"Retry-After": {"120"}}, `{}`)))
	if e, _ := err.(*quota.Error); e == nil || e.RetryAfter != 120*time.Second {
		t.Fatalf("Retry-After %v", err)
	}
}

// A refused login is re-armed once with `agy models` and retried; a token the
// wake wrote is the one retried.
func TestMeasureWakeRefreshesARefusedToken(t *testing.T) {
	files := loginFiles("expired-token")
	var woken []string
	cloud := doer(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer fresh-token" {
			return respond(401, `{}`)(r)
		}
		if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
			return respond(200, `{"models":{}}`)(r)
		}
		return respond(200, fixture(t, "summary.json"))(r)
	})
	cfg := config(files, cloud)
	cfg.LookPath = func(name string) (string, error) { return "/bin/" + name, nil }
	cfg.Wake = func(_ context.Context, binary string) error {
		woken = append(woken, binary)
		files["/home/u/.gemini/antigravity-cli/antigravity-oauth-token"] = `{"token":{"access_token":"fresh-token"}}`
		return nil
	}
	observed, err := Measure(context.Background(), cfg)
	if err != nil || len(observed.Buckets) == 0 {
		t.Fatalf("after the wake: %+v %v", observed, err)
	}
	if len(woken) != 1 || woken[0] != "/bin/agy" {
		t.Fatalf("woken %v", woken)
	}
}

func TestBinaryOverrideUsesPanewireThenLegacy(t *testing.T) {
	cfg := config(nil, nil)
	cfg.LookupEnv = func(name string) (string, bool) {
		switch name {
		case "PANEWIRE_AGY_BIN":
			return "/new/agy", true
		case "PACELEVER_AGY_BIN":
			return "/old/agy", true
		default:
			return "", false
		}
	}
	cfg.LookPath = func(name string) (string, error) { return name, nil }
	if got := cfg.binary(); got != "/new/agy" {
		t.Fatalf("new override = %q", got)
	}
	cfg.LookupEnv = func(name string) (string, bool) {
		return "/old/agy", name == "PACELEVER_AGY_BIN"
	}
	if got := cfg.binary(); got != "/old/agy" {
		t.Fatalf("legacy override = %q", got)
	}
}

// A token another holder already refreshed is reused without waking the CLI.
func TestMeasureReusesATokenAnotherHolderRefreshed(t *testing.T) {
	files := loginFiles("expired-token")
	cloud := doer(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer fresh-token" {
			return respond(401, `{}`)(r)
		}
		if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
			return respond(200, `{"models":{}}`)(r)
		}
		return respond(200, fixture(t, "summary.json"))(r)
	})
	cfg := config(files, cloud)
	cfg.LookPath = func(name string) (string, error) { return "/bin/" + name, nil }
	cfg.Locked = func(_ string, fn func() error) error {
		files["/home/u/.gemini/antigravity-cli/antigravity-oauth-token"] = `{"token":{"access_token":"fresh-token"}}`
		return fn()
	}
	cfg.Wake = func(context.Context, string) error {
		t.Fatal("the CLI was woken though the token was already refreshed")
		return nil
	}
	if _, err := Measure(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

func TestMeasureRefusedLoginKeepsTheHintWhenTheWakeCannotRun(t *testing.T) {
	refuse := doer(func(*http.Request) (*http.Response, error) { return respond(401, `{}`)(nil) })
	// No agy binary: nothing can re-arm the login.
	_, err := Measure(context.Background(), config(loginFiles(token), refuse))
	if quota.KindOf(err) != quota.ErrKindAuthFailed || !strings.Contains(err.Error(), "unavailable to refresh") {
		t.Fatalf("no binary: %v", err)
	}
	cfg := config(loginFiles(token), refuse)
	cfg.LookPath = func(name string) (string, error) { return "/bin/" + name, nil }
	cfg.Wake = func(context.Context, string) error { return errors.New("exit 1") }
	_, err = Measure(context.Background(), cfg)
	if quota.KindOf(err) != quota.ErrKindAuthFailed || !strings.Contains(err.Error(), "could not refresh") {
		t.Fatalf("wake failed: %v", err)
	}
}

// macOS: the keychain copy of the login is used when the file has none, and as
// the fallback when the file's token stays refused.
func TestMeasureKeychainLogin(t *testing.T) {
	envelope := `go-keyring-base64:` + "eyJ0b2tlbiI6eyJhY2Nlc3NfdG9rZW4iOiJrZXljaGFpbi10b2tlbiJ9fQ==" // {"token":{"access_token":"keychain-token"}}
	byToken := func(want string) Doer {
		return doer(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "Bearer "+want {
				return respond(401, `{}`)(r)
			}
			if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
				return respond(200, `{"models":{}}`)(r)
			}
			return respond(200, fixture(t, "summary.json"))(r)
		})
	}
	// File has no login: the keychain's is used.
	cfg := config(loginFiles(""), byToken("keychain-token"))
	cfg.GOOS = "darwin"
	cfg.Keychain = func(context.Context) (string, error) { return envelope, nil }
	if _, err := Measure(context.Background(), cfg); err != nil {
		t.Fatalf("keychain login: %v", err)
	}
	// File login refused, keychain's accepted.
	cfg = config(loginFiles("file-token"), byToken("keychain-token"))
	cfg.GOOS = "darwin"
	cfg.Keychain = func(context.Context) (string, error) { return envelope, nil }
	if _, err := Measure(context.Background(), cfg); err != nil {
		t.Fatalf("keychain fallback: %v", err)
	}
	// Off darwin the keychain is never read.
	cfg = config(loginFiles(""), byToken("keychain-token"))
	cfg.Keychain = func(context.Context) (string, error) {
		t.Fatal("keychain read off darwin")
		return "", nil
	}
	if _, err := Measure(context.Background(), cfg); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("no login off darwin: %v", err)
	}
}

// Linux: the secret service copy of the login is used when the file has none,
// and as the fallback when the file's token stays refused.
func TestMeasureSecretServiceLogin(t *testing.T) {
	envelope := `go-keyring-base64:` + base64.StdEncoding.EncodeToString([]byte(`{"token":{"access_token":"secret-service-token"}}`))
	byToken := func(want string) Doer {
		return doer(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "Bearer "+want {
				return respond(401, `{}`)(r)
			}
			if strings.HasSuffix(r.URL.Path, ":fetchAvailableModels") {
				return respond(200, `{"models":{}}`)(r)
			}
			return respond(200, fixture(t, "summary.json"))(r)
		})
	}
	// File has no login: the secret service's is used.
	cfg := config(loginFiles(""), byToken("secret-service-token"))
	cfg.SecretService = func(context.Context) (string, error) { return envelope, nil }
	if _, err := Measure(context.Background(), cfg); err != nil {
		t.Fatalf("secret service login: %v", err)
	}
	// File login refused, the secret service's accepted.
	cfg = config(loginFiles("file-token"), byToken("secret-service-token"))
	cfg.SecretService = func(context.Context) (string, error) { return envelope, nil }
	if _, err := Measure(context.Background(), cfg); err != nil {
		t.Fatalf("secret service fallback: %v", err)
	}
}

// secretServiceConfig is a linux measurement with no login file and the given
// secret service read.
func secretServiceConfig(read func(context.Context) (string, error)) Config {
	cfg := config(loginFiles(""), nil)
	cfg.SecretService = read
	return cfg
}

// Linux: the secret service item is the login, unwrapped like the keychain's.
// go-keyring stores the CLI's value as it was given, and the envelope prefixes
// are handled too.
func TestSecretTokenLinuxSecretService(t *testing.T) {
	value := `{"token":{"access_token":"secret-service-token"}}`
	for name, raw := range map[string]string{
		"raw JSON":        value,
		"base64 envelope": "go-keyring-base64:" + base64.StdEncoding.EncodeToString([]byte(value)),
		"hex envelope":    "go-keyring-encoded:" + hexOf(value),
	} {
		t.Run(name, func(t *testing.T) {
			cfg := secretServiceConfig(func(context.Context) (string, error) { return raw, nil })
			if got := cfg.secretToken(context.Background()); got != "secret-service-token" {
				t.Fatalf("secretToken = %q", got)
			}
		})
	}
}

// Linux without a usable secret service — no session bus, a locked or absent
// item, a value that is no login — has no token and never panics.
func TestSecretTokenLinuxWithoutASecretService(t *testing.T) {
	for name, read := range map[string]func(context.Context) (string, error){
		"no session bus": func(context.Context) (string, error) {
			return "", errors.New("dbus: couldn't determine address of session bus")
		},
		"locked item": func(context.Context) (string, error) { return "", errors.New("agy login item is locked") },
		"absent item": func(context.Context) (string, error) { return "", nil },
		"not a login": func(context.Context) (string, error) { return "not a token", nil },
	} {
		t.Run(name, func(t *testing.T) {
			called := false
			cfg := secretServiceConfig(func(ctx context.Context) (string, error) {
				called = true
				return read(ctx)
			})
			if got := cfg.secretToken(context.Background()); got != "" {
				t.Fatalf("secretToken = %q", got)
			}
			if !called {
				t.Fatal("the linux secret service was not read")
			}
		})
	}
}

// The secret read is bounded at 10 s: a reader that waits for its deadline
// returns "" and the caller's earlier deadline is the one that ends it.
func TestSecretTokenReadIsBounded(t *testing.T) {
	var deadline time.Duration
	cfg := secretServiceConfig(func(ctx context.Context) (string, error) {
		at, ok := ctx.Deadline()
		if !ok {
			t.Fatal("the read has no deadline")
		}
		deadline = time.Until(at)
		return "", errors.New("no session bus")
	})
	if got := cfg.secretToken(context.Background()); got != "" {
		t.Fatalf("secretToken = %q", got)
	}
	if deadline <= 0 || deadline > 10*time.Second {
		t.Fatalf("the read's deadline is %v, want at most 10s", deadline)
	}

	cfg = secretServiceConfig(func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	parent, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if got := cfg.secretToken(parent); got != "" {
		t.Fatalf("secretToken = %q", got)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the read ran %v past its deadline", elapsed)
	}
}

// macOS reads the keychain and never the secret service; Linux reads the
// secret service and never the keychain; other platforms read neither.
func TestSecretTokenPlatforms(t *testing.T) {
	cfg := secretServiceConfig(func(context.Context) (string, error) {
		t.Fatal("the secret service was read on darwin")
		return "", nil
	})
	cfg.GOOS = "darwin"
	cfg.Keychain = func(context.Context) (string, error) { return `{"token":{"access_token":"keychain-token"}}`, nil }
	if got := cfg.secretToken(context.Background()); got != "keychain-token" {
		t.Fatalf("darwin secretToken = %q", got)
	}

	cfg = secretServiceConfig(func(context.Context) (string, error) { return `{"token":{"access_token":"secret-service-token"}}`, nil })
	cfg.Keychain = func(context.Context) (string, error) {
		t.Fatal("the keychain was read on linux")
		return "", nil
	}
	if got := cfg.secretToken(context.Background()); got != "secret-service-token" {
		t.Fatalf("linux secretToken = %q", got)
	}

	for _, goos := range []string{"windows", "freebsd"} {
		cfg = secretServiceConfig(func(context.Context) (string, error) {
			t.Fatalf("the secret service was read on %s", goos)
			return "", nil
		})
		cfg.GOOS = goos
		cfg.Keychain = func(context.Context) (string, error) {
			t.Fatalf("the keychain was read on %s", goos)
			return "", nil
		}
		if got := cfg.secretToken(context.Background()); got != "" {
			t.Fatalf("%s secretToken = %q", goos, got)
		}
	}
}

func TestEnvelopeToken(t *testing.T) {
	for raw, want := range map[string]string{
		`{"token":{"access_token":" t1 "}}`:                              "t1",
		"go-keyring-encoded:" + hexOf(`{"token":{"access_token":"t2"}}`): "t2",
		"go-keyring-base64:@@@":                                          "",
		"go-keyring-encoded:zz":                                          "",
		`not json`:                                                       "",
		`{"token":{}}`:                                                   "",
	} {
		if got := envelopeToken(raw); got != want {
			t.Errorf("envelopeToken(%q) = %q, want %q", raw, got, want)
		}
	}
}

func hexOf(s string) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(s))
	for _, b := range []byte(s) {
		out = append(out, digits[b>>4], digits[b&15])
	}
	return string(out)
}

func TestLoginFileLocations(t *testing.T) {
	files := map[string]string{"/xdg/antigravity-cli/antigravity-oauth-token": `{"token":{"access_token":"xdg-token"}}`}
	cfg := config(files, nil)
	cfg.LookupEnv = func(name string) (string, bool) { return "/xdg", name == "XDG_CONFIG_HOME" }
	if got := cfg.fileToken(); got != "xdg-token" {
		t.Fatalf("XDG location: %q", got)
	}
	files["/home/u/.gemini/antigravity-cli/antigravity-oauth-token"] = `{"token":{"access_token":"gemini-token"}}`
	if got := cfg.fileToken(); got != "gemini-token" {
		t.Fatalf("~/.gemini wins: %q", got)
	}
	files["/home/u/.gemini/antigravity-cli/antigravity-oauth-token"] = strings.Repeat("x", authFileLimit+1)
	if got := cfg.fileToken(); got != "xdg-token" {
		t.Fatalf("an oversized file has no token: %q", got)
	}
}

func TestWakeLockPathUsesPanewireAndLegacyCache(t *testing.T) {
	cfg := config(nil, nil)
	if got := cfg.wakeLockPath(); got != "/home/u/.cache/panewire/agy-wake.lock" {
		t.Fatalf("default %q", got)
	}
	cfg.LookupEnv = func(name string) (string, bool) { return "/c/snapshots.json", name == "PACELEVER_CACHE" }
	if got := cfg.wakeLockPath(); got != "/c/agy-wake.lock" {
		t.Fatalf("PACELEVER_CACHE %q", got)
	}
	cfg.LookupEnv = func(name string) (string, bool) {
		if name == "PANEWIRE_CACHE" {
			return "/new/records.json", true
		}
		if name == "PACELEVER_CACHE" {
			return "/old/records.json", true
		}
		return "", false
	}
	if got := cfg.wakeLockPath(); got != "/new/agy-wake.lock" {
		t.Fatalf("PANEWIRE_CACHE precedence %q", got)
	}
}

func TestWithFileLockSerialisesAndReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "agy-wake.lock")
	for i := 0; i < 2; i++ {
		ran := false
		if err := withFileLock(path, func() error { ran = true; return nil }); err != nil || !ran {
			t.Fatalf("run %d: ran=%v err=%v", i, ran, err)
		}
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("lock file %v %v", info, err)
	}
}

func mustObject(t *testing.T, body string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
