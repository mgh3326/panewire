package claude

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// testNow is the clock the pacelever fixtures were written against.
var testNow = time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)

// secretToken is the OAuth token every test login holds. No error, argv or
// environment may ever carry it beyond the one Authorization header.
const secretToken = "sk-ant-oat01-test-token-must-not-leak"

type doFunc func(*http.Request) (*http.Response, error)

func (f doFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func respond(status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

// login is a fake filesystem holding one claude login.
type login struct {
	files map[string]string
}

func (l login) read(path string) ([]byte, error) {
	if body, ok := l.files[path]; ok {
		return []byte(body), nil
	}
	return nil, os.ErrNotExist
}

func credentialFile(token string, expiresAt time.Time) string {
	return `{"claudeAiOauth":{"accessToken":"` + token + `","expiresAt":` + itoa(expiresAt.UnixMilli()) + `}}`
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// testConfig is a login in /home/u/.claude with a valid token and account.
func testConfig(t *testing.T, client Doer) Config {
	t.Helper()
	l := login{files: map[string]string{
		"/home/u/.claude/.credentials.json": credentialFile(secretToken, testNow.Add(time.Hour)),
		"/home/u/.claude.json":              `{"oauthAccount":{"accountUuid":"00000000-0000-4000-8000-000000000001"}}`,
	}}
	return Config{
		HTTP: client, Now: func() time.Time { return testNow }, Home: "/home/u", ReadFile: l.read,
		LookupEnv: func(string) (string, bool) { return "", false }, GOOS: "linux",
		RunCLI: func(context.Context, string, string, string) (string, error) {
			t.Error("the CLI fallback ran")
			return "", errors.New("unexpected")
		},
	}
}

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "screens", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// The pacelever screen fixtures (6/10/0 with exact resets), both "% used" and
// "% left" forms.
func TestUsageScreenFixtures(t *testing.T) {
	for _, tt := range []struct {
		file string
		want []float64
	}{
		{"claude_usage.txt", []float64{6, 10, 0}},
		{"claude_usage_left.txt", []float64{6, 10, 0}},
	} {
		t.Run(tt.file, func(t *testing.T) {
			o, err := ParseUsageScreen(fixture(t, tt.file), testNow)
			if err != nil || o.Source != "cli:/usage" || len(o.Buckets) != len(tt.want) {
				t.Fatalf("invalid result %+v err=%v", o, err)
			}
			for i, want := range tt.want {
				if o.Buckets[i].UsedPct == nil || *o.Buckets[i].UsedPct != want {
					t.Fatalf("bucket %d = %+v want %g", i, o.Buckets[i], want)
				}
			}
			if b := o.Buckets[0]; b.Window != "5h" || b.Scope.Kind != "account" || b.ResetsAt != "2026-10-29T14:40:00Z" {
				t.Fatalf("session bucket %+v", b)
			}
			if b := o.Buckets[1]; b.Window != "7d" || b.Scope.Kind != "account" || b.ResetsAt != "2026-10-31T17:00:00Z" {
				t.Fatalf("weekly bucket %+v", b)
			}
			if b := o.Buckets[2]; b.Window != "7d" || b.Scope.Kind != "model" || b.Scope.Name != "Fable" || b.ResetsAt != "2026-10-31T17:00:00Z" {
				t.Fatalf("model bucket %+v", b)
			}
		})
	}
}

func TestUsageScreenRejectsIncompleteOutput(t *testing.T) {
	o, err := ParseUsageScreen("Current week (all models): 10% used · resets Nov 1 at 2am (Asia/Seoul)\n", testNow)
	if quota.KindOf(err) != quota.ErrKindParse || len(o.Buckets) != 0 {
		t.Fatalf("incomplete output accepted: %+v err=%v", o, err)
	}
}

// A heading with no value of its own must not borrow the next section's
// percentage or reset time.
func TestUsageScreenDoesNotReadAcrossHeadings(t *testing.T) {
	for name, screen := range map[string]string{
		"no percent": "Current session\n  resets Oct 29 at 11:40pm (Asia/Seoul)\nCurrent week (all models)\n  10% used\n  resets Nov 1 at 2am (Asia/Seoul)\n",
		"no reset":   "Current session: 6% used\nCurrent week (all models): 10% used\n  resets Nov 1 at 2am (Asia/Seoul)\n",
	} {
		t.Run(name, func(t *testing.T) {
			o, err := ParseUsageScreen(screen, testNow)
			if quota.KindOf(err) != quota.ErrKindParse || len(o.Buckets) != 0 {
				t.Fatalf("borrowed a value from the next section: %+v err=%v", o, err)
			}
		})
	}
}

// The API body of pacelever's TestHTTPProviders.
func TestAPIBody(t *testing.T) {
	body := `{"five_hour":{"utilization":12,"resets_at":"2026-10-29T00:00:00Z"},"seven_day":{"utilization":20}}`
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(200, body, nil), nil }))
	o, err := Measure(t.Context(), cfg)
	if err != nil || o.Source != quota.SourceUsageAPI || len(o.Buckets) != 2 || *o.Buckets[0].UsedPct != 12 {
		t.Fatalf("result %+v err=%v", o, err)
	}
	if b := o.Buckets[0]; b.Window != "5h" || b.Scope.Kind != "account" || b.ResetsAt != "2026-10-29T00:00:00Z" {
		t.Fatalf("5h bucket %+v", b)
	}
	if b := o.Buckets[1]; b.Window != "7d" || *b.UsedPct != 20 || b.ResetsAt != "" {
		t.Fatalf("7d bucket %+v", b)
	}
}

func TestAPIModelLimitsAndInvalidUtilisation(t *testing.T) {
	body := `{"five_hour":{"utilization":120},"seven_day":{"utilization":20},"limits":[{"utilization":55.5,"resets_at":"2026-11-01T00:00:00Z","scope":{"model":{"display_name":"Fable"}}},{"utilization":9,"scope":{}}]}`
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(200, body, nil), nil }))
	o, err := Measure(t.Context(), cfg)
	if err != nil || len(o.Buckets) != 3 {
		t.Fatalf("result %+v err=%v", o, err)
	}
	if o.Buckets[0].UsedPct != nil {
		t.Fatalf("utilisation 120 kept: %+v", o.Buckets[0])
	}
	if b := o.Buckets[2]; b.Scope.Kind != "model" || b.Scope.Name != "Fable" || b.Window != "7d" || *b.UsedPct != 55.5 || b.ResetsAt != "2026-11-01T00:00:00Z" {
		t.Fatalf("model bucket %+v", b)
	}
}

// The request carries the contract headers and nothing else of the login.
func TestAPIRequestShape(t *testing.T) {
	var seen *http.Request
	cfg := testConfig(t, doFunc(func(r *http.Request) (*http.Response, error) {
		seen = r
		return respond(200, `{"five_hour":{"utilization":1},"seven_day":{"utilization":2}}`, nil), nil
	}))
	if _, err := Measure(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if seen.Method != "GET" || seen.URL.String() != "https://api.anthropic.com/api/oauth/usage" {
		t.Fatalf("request %s %s", seen.Method, seen.URL)
	}
	if seen.Header.Get("Authorization") != "Bearer "+secretToken || seen.Header.Get("anthropic-beta") != "oauth-2025-04-20" || seen.Header.Get("Accept") != "application/json" || seen.Header.Get("User-Agent") == "" {
		t.Fatalf("headers %v", seen.Header)
	}
	if strings.Contains(seen.URL.String(), secretToken) {
		t.Fatal("token in URL")
	}
}

// Status mapping: 401/403→auth, 429→rate_limited, other→http,
// transport→network, bad JSON or no windows→parse_error. The CLI fallback runs
// once and its failure leaves the API kind.
func TestErrorKindMapping(t *testing.T) {
	leak := "response-body-must-not-escape " + secretToken
	for _, tt := range []struct {
		name string
		do   doFunc
		kind string
	}{
		{"401", func(*http.Request) (*http.Response, error) { return respond(401, leak, nil), nil }, quota.ErrKindAuth},
		{"403", func(*http.Request) (*http.Response, error) { return respond(403, leak, nil), nil }, quota.ErrKindAuth},
		{"429", func(*http.Request) (*http.Response, error) { return respond(429, leak, nil), nil }, quota.ErrKindRateLimited},
		{"500", func(*http.Request) (*http.Response, error) { return respond(500, leak, nil), nil }, quota.ErrKindHTTP},
		{"network", func(*http.Request) (*http.Response, error) { return nil, errors.New(leak) }, quota.ErrKindNetwork},
		{"bad json", func(*http.Request) (*http.Response, error) { return respond(200, leak, nil), nil }, quota.ErrKindParse},
		{"no windows", func(*http.Request) (*http.Response, error) { return respond(200, `{"other":{}}`, nil), nil }, quota.ErrKindParse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, tt.do)
			cliRuns := 0
			cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
				cliRuns++
				return "", errors.New("claude missing " + leak)
			}
			_, err := Measure(t.Context(), cfg)
			if quota.KindOf(err) != tt.kind {
				t.Fatalf("kind = %q, want %q (err %v)", quota.KindOf(err), tt.kind, err)
			}
			if cliRuns != 1 {
				t.Fatalf("CLI fallback ran %d times, want once", cliRuns)
			}
			if strings.Contains(err.Error(), "must-not-escape") || strings.Contains(err.Error(), secretToken) {
				t.Fatalf("error leaks: %v", err)
			}
		})
	}
}

func TestMissingLoginNeverCallsTheAPI(t *testing.T) {
	for _, tt := range []struct {
		name  string
		files map[string]string
		kind  string
	}{
		{"no credentials", map[string]string{}, quota.ErrKindCredentials},
		{"no token", map[string]string{"/home/u/.claude/.credentials.json": `{"claudeAiOauth":{"expiresAt":1}}`}, quota.ErrKindCredentials},
		{"expired", map[string]string{"/home/u/.claude/.credentials.json": credentialFile(secretToken, testNow.Add(-time.Hour))}, quota.ErrKindTokenExpired},
		// Expiry is in milliseconds: half a second ago is expired, not valid.
		{"expired within the second", map[string]string{"/home/u/.claude/.credentials.json": credentialFile(secretToken, testNow.Add(-500*time.Millisecond))}, quota.ErrKindTokenExpired},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
				t.Error("API called without a usable login")
				return nil, errors.New("unexpected")
			}))
			cfg.ReadFile = login{files: tt.files}.read
			_, err := Measure(t.Context(), cfg)
			if quota.KindOf(err) != tt.kind || strings.Contains(err.Error(), secretToken) {
				t.Fatalf("err = %v, want kind %s", err, tt.kind)
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		header string
		want   time.Duration
	}{
		{"120", 120 * time.Second},
		{"0", 300 * time.Second},
		{"", 300 * time.Second},
		{"not-a-date", 300 * time.Second},
		{"999999", 86400 * time.Second},
		{testNow.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{testNow.Add(-time.Hour).UTC().Format(http.TimeFormat), 300 * time.Second},
	} {
		cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
			return respond(429, `{}`, http.Header{"Retry-After": []string{tt.header}}), nil
		}))
		cfg.RunCLI = func(context.Context, string, string, string) (string, error) { return "", errors.New("no") }
		_, err := Measure(t.Context(), cfg)
		var failure *quota.Error
		if !errors.As(err, &failure) || failure.Kind != quota.ErrKindRateLimited || failure.RetryAfter != tt.want {
			t.Fatalf("retry-after %q: %+v, want %s", tt.header, err, tt.want)
		}
	}
}

// pacelever's TestOAuthChoiceWithDecodedNumbers.
func TestOAuthChoiceWithDecodedNumbers(t *testing.T) {
	expired := map[string]any{"accessToken": "fixture-file-placeholder", "expiresAt": json.Number("1")}
	fresh := map[string]any{"accessToken": "fixture-keychain-placeholder", "expiresAt": float64(testNow.Add(time.Hour).UnixMilli())}
	if chosen := chooseOAuth(expired, fresh, testNow); chosen["expiresAt"] != fresh["expiresAt"] {
		t.Fatal("expired file preferred over current keychain context")
	}
	if expires, ok := finiteNumber(json.Number("1000")); !ok || expires != 1000 {
		t.Fatal("JSON expiry lost numeric meaning")
	}
	later := map[string]any{"accessToken": "k", "expiresAt": float64(testNow.Add(2 * time.Hour).UnixMilli())}
	file := map[string]any{"accessToken": "f", "expiresAt": float64(testNow.Add(time.Hour).UnixMilli())}
	if chosen := chooseOAuth(file, later, testNow); chosen["accessToken"] != "k" {
		t.Fatal("later expiry not preferred")
	}
	if chosen := chooseOAuth(map[string]any{}, file, testNow); chosen["accessToken"] != "f" {
		t.Fatal("a file without a token must yield to the keychain")
	}
}

// The macOS keychain is read only on darwin, by the hashed service name when a
// config dir is set. The Linux test path drives it through the fake runner.
func TestKeychainReadOnlyOnDarwin(t *testing.T) {
	var services []string
	keychain := func(_ context.Context, service string) ([]byte, error) {
		services = append(services, service)
		return []byte(`{"claudeAiOauth":{"accessToken":"` + secretToken + `","expiresAt":` + itoa(testNow.Add(time.Hour).UnixMilli()) + `}}`), nil
	}
	ok := doFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer "+secretToken {
			t.Errorf("keychain token not used")
		}
		return respond(200, `{"five_hour":{"utilization":1}}`, nil), nil
	})
	cfg := testConfig(t, ok)
	cfg.ReadFile = login{files: map[string]string{}}.read
	cfg.Keychain = keychain
	if _, err := Measure(t.Context(), cfg); quota.KindOf(err) != quota.ErrKindCredentials || len(services) != 0 {
		t.Fatalf("keychain read off darwin: err=%v services=%v", err, services)
	}
	cfg.GOOS = "darwin"
	if _, err := Measure(t.Context(), cfg); err != nil || len(services) != 1 || services[0] != "Claude Code-credentials" {
		t.Fatalf("darwin: err=%v services=%v", err, services)
	}
	cfg.ConfigDir = "/cfg/work"
	if _, err := Measure(t.Context(), cfg); err != nil || len(services) != 2 || !strings.HasPrefix(services[1], "Claude Code-credentials-") || len(services[1]) != len("Claude Code-credentials-")+8 {
		t.Fatalf("darwin with config dir: err=%v services=%v", err, services)
	}
	failing := cfg
	failing.Keychain = func(context.Context, string) ([]byte, error) { return nil, errors.New("locked") }
	if _, err := Measure(t.Context(), failing); quota.KindOf(err) != quota.ErrKindCredentials {
		t.Fatalf("locked keychain: %v", err)
	}
}

// Where the credential, identity and keychain service live.
func TestLocations(t *testing.T) {
	env := func(values map[string]string) func(string) (string, bool) {
		return func(name string) (string, bool) { v, ok := values[name]; return v, ok }
	}
	home := Config{Home: "/home/u", LookupEnv: env(nil)}.locations()
	if home.credential != "/home/u/.claude/.credentials.json" || home.identity != "/home/u/.claude.json" || home.service != "Claude Code-credentials" || !home.shareable {
		t.Fatalf("home %+v", home)
	}
	viaEnv := Config{Home: "/home/u", LookupEnv: env(map[string]string{"CLAUDE_CONFIG_DIR": "/cfg/a"})}.locations()
	if viaEnv.credential != "/cfg/a/.credentials.json" || viaEnv.identity != "/cfg/a/.claude.json" || !strings.HasPrefix(viaEnv.service, "Claude Code-credentials-") || !viaEnv.shareable {
		t.Fatalf("env %+v", viaEnv)
	}
	selected := Config{Home: "/home/u", ConfigDir: "/cfg/b", LookupEnv: env(map[string]string{"CLAUDE_CONFIG_DIR": "/cfg/a"})}.locations()
	if selected.credential != "/cfg/b/.credentials.json" {
		t.Fatalf("the selector must beat the environment: %+v", selected)
	}
	secure := Config{Home: "/home/u", LookupEnv: env(map[string]string{"CLAUDE_CONFIG_DIR": "/cfg/a", "CLAUDE_SECURESTORAGE_CONFIG_DIR": "/vault"})}.locations()
	if secure.credential != "/vault/.credentials.json" || secure.identity != "/cfg/a/.claude.json" || secure.shareable {
		t.Fatalf("secure storage %+v", secure)
	}
	same := Config{Home: "/home/u", LookupEnv: env(map[string]string{"CLAUDE_CONFIG_DIR": "/cfg/a", "CLAUDE_SECURESTORAGE_CONFIG_DIR": "/cfg/a"})}.locations()
	if !same.shareable {
		t.Fatalf("secure storage in the config dir stays shareable: %+v", same)
	}
}

// pacelever's fingerprint, hex(sha256("claude-account|"+uuid))[:16]. The value
// for this uuid was computed with pacelever@f53bb01's claudeAccountFP.
func TestAccountFingerprint(t *testing.T) {
	const want = "e864f6030c9d151d"
	cfg := testConfig(t, nil)
	if got := AccountFP(cfg); got != want {
		t.Fatalf("fingerprint %q, want %q", got, want)
	}
	cfg.ReadFile = login{files: map[string]string{"/home/u/.claude.json": `{"oauthAccount":{}}`}}.read
	if got := AccountFP(cfg); got != "" {
		t.Fatalf("unrecorded account fingerprint %q", got)
	}
	cfg = testConfig(t, nil)
	cfg.LookupEnv = func(name string) (string, bool) {
		return map[string]string{"CLAUDE_CONFIG_DIR": "/cfg/a", "CLAUDE_SECURESTORAGE_CONFIG_DIR": "/vault"}[name], name != "HOME"
	}
	if got := AccountFP(cfg); got != "" {
		t.Fatalf("unshareable login fingerprint %q", got)
	}
}

func TestMeasureAttachesFingerprint(t *testing.T) {
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
		return respond(200, `{"five_hour":{"utilization":1}}`, nil), nil
	}))
	o, err := Measure(t.Context(), cfg)
	if err != nil || o.AccountFP != AccountFP(cfg) || o.AccountFP == "" {
		t.Fatalf("observation %+v err=%v", o, err)
	}
}

// An API failure gets exactly one CLI attempt in a dedicated empty directory,
// with the account selector and nothing of the token.
func TestCLIFallbackOnceAfterAPIFailure(t *testing.T) {
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(429, "do not leak", nil), nil }))
	cfg.Binary, cfg.ConfigDir = "fake-claude", "/cfg/work"
	cfg.ReadFile = login{files: map[string]string{
		"/cfg/work/.credentials.json": credentialFile(secretToken, testNow.Add(time.Hour)),
		"/cfg/work/.claude.json":      `{"oauthAccount":{"accountUuid":"00000000-0000-4000-8000-000000000001"}}`,
	}}.read
	calls := 0
	var privateDir string
	cfg.RunCLI = func(_ context.Context, binary, dir, configDir string) (string, error) {
		calls++
		privateDir = dir
		if binary != "fake-claude" || configDir != "/cfg/work" {
			t.Errorf("unexpected command context %q %q", binary, configDir)
		}
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("CLI did not receive a dedicated empty directory: %v %v", entries, err)
		}
		for _, arg := range []string{binary, dir, configDir} {
			if strings.Contains(arg, secretToken) {
				t.Error("token reached the CLI")
			}
		}
		return fixture(t, "claude_usage.txt"), nil
	}
	o, err := Measure(t.Context(), cfg)
	if err != nil || calls != 1 || o.Source != quota.SourceCLI || len(o.Buckets) != 3 || o.AccountFP == "" {
		t.Fatalf("fallback result calls=%d %+v err=%v", calls, o, err)
	}
	if _, err := os.Stat(privateDir); !os.IsNotExist(err) {
		t.Fatalf("the CLI's private directory was left behind: %v", err)
	}
}

func TestCLIFailureKeepsAPIFailure(t *testing.T) {
	for _, tt := range []struct {
		name string
		run  func(context.Context, string, string, string) (string, error)
		do   doFunc
		kind string
	}{
		{"missing binary", func(context.Context, string, string, string) (string, error) { return "", errors.New("missing") },
			func(*http.Request) (*http.Response, error) { return respond(429, "x", nil), nil }, quota.ErrKindRateLimited},
		{"unrecognised panel", func(context.Context, string, string, string) (string, error) { return "not usage", nil },
			func(*http.Request) (*http.Response, error) { return respond(401, "x", nil), nil }, quota.ErrKindAuth},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(t, tt.do)
			cfg.RunCLI = tt.run
			o, err := Measure(t.Context(), cfg)
			if quota.KindOf(err) != tt.kind || len(o.Buckets) != 0 {
				t.Fatalf("CLI failure replaced the API failure: %+v err=%v", o, err)
			}
		})
	}
}

// A 200 whose windows are null or out of range measured nothing; the CLI
// still gets its one attempt, and a partial answer stays an API success.
func TestAPIWithoutUsableWindowFallsBackToCLI(t *testing.T) {
	for name, body := range map[string]string{
		"null windows":   `{"five_hour":null,"seven_day":null}`,
		"no utilisation": `{"five_hour":{"resets_at":"2026-10-29T00:00:00Z"}}`,
		"out of range":   `{"five_hour":{"utilization":120}}`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(200, body, nil), nil }))
			calls := 0
			cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
				calls++
				return fixture(t, "claude_usage.txt"), nil
			}
			o, err := Measure(t.Context(), cfg)
			if err != nil || calls != 1 || o.Source != quota.SourceCLI {
				t.Fatalf("calls=%d %+v err=%v", calls, o, err)
			}
		})
	}
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
		return respond(200, `{"five_hour":null,"seven_day":{"utilization":20}}`, nil), nil
	}))
	cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
		t.Error("CLI ran after a partial API answer")
		return "", errors.New("unexpected")
	}
	if o, err := Measure(t.Context(), cfg); err != nil || o.Source != quota.SourceUsageAPI {
		t.Fatalf("partial answer %+v err=%v", o, err)
	}
}

// Anything after the JSON object makes the body untrustworthy, so it is a
// parse failure and the CLI gets its one attempt.
func TestAPITrailingDataFallsBackToCLI(t *testing.T) {
	for name, body := range map[string]string{
		"garbage":    `{"five_hour":{"utilization":12}} garbage`,
		"second doc": `{"five_hour":{"utilization":12}}{"x":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(200, body, nil), nil }))
			calls := 0
			cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
				calls++
				return fixture(t, "claude_usage.txt"), nil
			}
			o, err := Measure(t.Context(), cfg)
			if err != nil || calls != 1 || o.Source != quota.SourceCLI {
				t.Fatalf("calls=%d %+v err=%v", calls, o, err)
			}
		})
	}
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
		return respond(200, "{\"five_hour\":{\"utilization\":12}}\n", nil), nil
	}))
	cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
		t.Error("CLI ran after a clean body with a trailing newline")
		return "", errors.New("unexpected")
	}
	if o, err := Measure(t.Context(), cfg); err != nil || o.Source != quota.SourceUsageAPI {
		t.Fatalf("trailing newline %+v err=%v", o, err)
	}
}

func TestCLINotRunAfterAPISuccess(t *testing.T) {
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) {
		return respond(200, `{"five_hour":{"utilization":1}}`, nil), nil
	}))
	if _, err := Measure(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
}

// The real command runner: argv, working directory, environment and the
// timeout, against a fake claude script. The token is on none of them.
func TestRunUsageCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake claude is a shell script")
	}
	bin := t.TempDir()
	record := filepath.Join(t.TempDir(), "record")
	script := "#!/bin/sh\n{ printf 'argv:'; for a in \"$@\"; do printf '[%s]' \"$a\"; done; printf '\\nenv:%s\\ncwd:%s\\n' \"$CLAUDE_CONFIG_DIR\" \"$(pwd)\"; env; } > " + record + "\ncat <<'EOF'\n" + fixture(t, "claude_usage.txt") + "EOF\n"
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("ANTHROPIC_API_KEY", "")
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(500, "x", nil), nil }))
	cfg.RunCLI = nil
	cfg.ConfigDir = "/cfg/work"
	cfg.ReadFile = login{files: map[string]string{"/cfg/work/.credentials.json": credentialFile(secretToken, testNow.Add(time.Hour))}}.read
	o, err := Measure(t.Context(), cfg)
	if err != nil || o.Source != quota.SourceCLI || len(o.Buckets) != 3 {
		t.Fatalf("result %+v err=%v", o, err)
	}
	data, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatal(readErr)
	}
	text := string(data)
	if !strings.HasPrefix(text, "argv:[-p][/usage][--allowed-tools][]\n") {
		t.Fatalf("argv %q", strings.SplitN(text, "\n", 2)[0])
	}
	if !strings.Contains(text, "env:/cfg/work\n") {
		t.Fatalf("CLAUDE_CONFIG_DIR not passed: %q", text)
	}
	if strings.Contains(text, secretToken) {
		t.Fatal("token reached the CLI's argv or environment")
	}
	cwd := strings.SplitN(strings.SplitN(text, "cwd:", 2)[1], "\n", 2)[0]
	if cwd == "" || !strings.Contains(filepath.Base(cwd), cliTempPrefix) {
		t.Fatalf("CLI ran in %q, not a private temp directory", cwd)
	}
}

func TestRunUsageCommandTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake claude is a shell script")
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte("#!/bin/sh\nsleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	cfg := testConfig(t, doFunc(func(*http.Request) (*http.Response, error) { return respond(500, "x", nil), nil }))
	cfg.RunCLI = nil
	cfg.CLITimeout = 200 * time.Millisecond
	start := time.Now()
	_, err := Measure(t.Context(), cfg)
	if quota.KindOf(err) != quota.ErrKindHTTP {
		t.Fatalf("err = %v", err)
	}
	// The hung child must be killed at the timeout, not waited out.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("hung CLI held the measurement for %s", elapsed)
	}
}

// The body is bounded and a redirect is never followed (it would carry the
// token to another host).
func TestDefaultClientRefusesRedirects(t *testing.T) {
	// A zero Config builds its own client; reach it through an HTTP server.
	var hits int
	target := http.NewServeMux()
	target.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path == "/usage" {
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
			return
		}
		t.Error("redirect followed")
	})
	server := newLoopbackServer(t, target)
	cfg := testConfig(t, nil)
	cfg.Endpoint = server + "/usage"
	cfg.RunCLI = func(context.Context, string, string, string) (string, error) { return "", errors.New("no") }
	_, err := Measure(t.Context(), cfg)
	if quota.KindOf(err) != quota.ErrKindHTTP || hits != 1 {
		t.Fatalf("err=%v hits=%d", err, hits)
	}
}
