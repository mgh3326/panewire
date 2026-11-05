// Package claude measures the claude pool's quota the way pacelever does: the
// OAuth usage API first, one `claude -p /usage` fallback if that fails, the
// same buckets, the same error kinds and the same account fingerprint.
//
// The OAuth token is read into memory for the one request. It is never
// logged, written, placed in an error, or put on a command line.
package claude

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	// UsageEndpoint is the OAuth usage API.
	UsageEndpoint    = "https://api.anthropic.com/api/oauth/usage"
	apiTimeout       = 20 * time.Second
	apiBodyLimit     = 2 << 20
	defaultRateWait  = 300 * time.Second
	maxRetryAfter    = 86400 * time.Second
	credentialsLimit = 1 << 20
)

// Doer is the HTTP client the usage request goes through.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config is everything a measurement reads from its environment. The zero
// value measures this host's login with the real filesystem, keychain, claude
// binary and wall clock; tests replace the seams.
type Config struct {
	HTTP     Doer   // nil: 20 s timeout, redirects refused
	Endpoint string // empty: UsageEndpoint
	// Now is the clock for token expiry and CLI reset dates; nil is the wall
	// clock.
	Now func() time.Time
	// Home is the home directory; empty is os.UserHomeDir.
	Home string
	// ConfigDir selects the account (CLAUDE_CONFIG_DIR); empty reads the
	// environment.
	ConfigDir string
	// LookupEnv reads the environment; nil is os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// GOOS is the platform name; empty is runtime.GOOS. The keychain is read
	// only for "darwin".
	GOOS string
	// ReadFile reads a credential or identity file; nil is os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Keychain returns the item for a service name (`security
	// find-generic-password -s <service> -w`); nil runs `security`.
	Keychain func(ctx context.Context, service string) ([]byte, error)
	// Binary is the claude executable for the CLI fallback; empty is "claude".
	Binary string
	// CLITimeout bounds the fallback; zero is 15 s.
	CLITimeout time.Duration
	// RunCLI runs `claude -p /usage --allowed-tools ""` in dir and returns its
	// standard output; nil runs the real binary. configDir is the account
	// selector to pass through as CLAUDE_CONFIG_DIR, empty for none.
	RunCLI func(ctx context.Context, binary, dir, configDir string) (string, error)
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

// Measure reads the claude quota: the usage API, then one CLI fallback. The
// error is a *quota.Error whose kind names the first (API) failure when both
// fail, as pacelever reports it.
func Measure(ctx context.Context, cfg Config) (quota.Observation, error) {
	loc := cfg.locations()
	oauth := cfg.oauth(ctx, loc)
	token := str(oauth["accessToken"])
	if token == "" {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	if expires, ok := finiteNumber(oauth["expiresAt"]); ok && expires/1000 < float64(cfg.now().Unix()) {
		return quota.Observation{}, quota.Fail(quota.ErrKindTokenExpired, "token expired")
	}
	observed, err := cfg.fetchAPI(ctx, token)
	if err != nil {
		cli, cliErr := cfg.fetchCLI(ctx)
		if cliErr != nil {
			return quota.Observation{}, err
		}
		observed = cli
	}
	if loc.shareable {
		observed.AccountFP = cfg.accountFP(loc.identity)
	}
	return observed, nil
}

// AccountFP is this host's claude account fingerprint, "" when the login is
// not one an account may be shared from or has no recorded account.
func AccountFP(cfg Config) string {
	loc := cfg.locations()
	if !loc.shareable {
		return ""
	}
	return cfg.accountFP(loc.identity)
}

// fetchAPI is one usage API request. Every failure is a kind plus a fixed
// sentence; the response body and the token never reach an error.
func (c Config) fetchAPI(ctx context.Context, token string) (quota.Observation, error) {
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = UsageEndpoint
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: apiTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("anthropic-beta", "oauth-2025-04-20")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "panewire")
	response, err := client.Do(request)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return quota.Observation{}, statusError(response.StatusCode, response.Header, c.now())
	}
	var raw map[string]json.RawMessage
	if json.NewDecoder(io.LimitReader(response.Body, apiBodyLimit)).Decode(&raw) != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "invalid usage JSON")
	}
	observed := quota.Observation{Source: quota.SourceUsageAPI, Buckets: parseAPI(raw)}
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "usage response has no recognized windows")
	}
	return observed, nil
}

// statusError is the one mapping from a non-200 status to a safe failure.
func statusError(status int, header http.Header, now time.Time) *quota.Error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return quota.Fail(quota.ErrKindAuth, "usage HTTP "+http.StatusText(status))
	case http.StatusTooManyRequests:
		e := quota.Fail(quota.ErrKindRateLimited, "usage HTTP 429")
		e.RetryAfter = retryAfter(header.Get("Retry-After"), now)
		if e.RetryAfter <= 0 {
			e.RetryAfter = defaultRateWait
		}
		return e
	}
	return quota.Fail(quota.ErrKindHTTP, "usage HTTP "+http.StatusText(status))
}

// retryAfter reads a Retry-After header as delay-seconds or an HTTP-date,
// capped at a day. Missing, zero, negative and unparseable values are 0.
func retryAfter(header string, now time.Time) time.Duration {
	value := strings.TrimSpace(header)
	if value == "" {
		return 0
	}
	if n, ok := quota.ParseFinite(value); ok {
		if n <= 0 {
			return 0
		}
		return time.Duration(math.Min(n, maxRetryAfter.Seconds()) * float64(time.Second))
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := at.Sub(now); d > 0 {
			return min(d, maxRetryAfter)
		}
	}
	return 0
}

// parseAPI reads the usage endpoint: the 5h and 7d account windows plus any
// per-model weekly limits.
func parseAPI(raw map[string]json.RawMessage) []quota.Bucket {
	var buckets []quota.Bucket
	for _, axis := range []struct{ key, window string }{{"five_hour", "5h"}, {"seven_day", "7d"}} {
		var w struct {
			Utilization *float64 `json:"utilization"`
			ResetsAt    string   `json:"resets_at"`
		}
		if json.Unmarshal(raw[axis.key], &w) == nil {
			buckets = append(buckets, accountBucket(axis.window, validated(w.Utilization), w.ResetsAt))
		}
	}
	var limits []struct {
		Utilization *float64 `json:"utilization"`
		ResetsAt    string   `json:"resets_at"`
		Scope       struct {
			Model struct {
				DisplayName string `json:"display_name"`
			} `json:"model"`
		} `json:"scope"`
	}
	_ = json.Unmarshal(raw["limits"], &limits)
	for _, w := range limits {
		if w.Scope.Model.DisplayName != "" {
			buckets = append(buckets, modelBucket(w.Scope.Model.DisplayName, "7d", validated(w.Utilization), w.ResetsAt))
		}
	}
	return buckets
}

func accountBucket(window string, used *float64, reset string) quota.Bucket {
	return quota.Bucket{Window: window, UsedPct: used, ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeAccount}}
}

func modelBucket(name, window string, used *float64, reset string) quota.Bucket {
	return quota.Bucket{Window: window, UsedPct: used, ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeModel, Name: name}}
}

func validated(p *float64) *float64 {
	if !quota.ValidPct(p) {
		return nil
	}
	return p
}

// readObject reads a JSON object file; anything unreadable is an empty object.
// Numbers stay json.Number so an expiry keeps its numeric meaning.
func (c Config) readObject(path string) map[string]any {
	read := c.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	data, err := read(path)
	if err != nil || len(data) > credentialsLimit {
		return map[string]any{}
	}
	return decodeObject(data)
}

func decodeObject(data []byte) map[string]any {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	var out map[string]any
	if decoder.Decode(&out) != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func object(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// finiteNumber accepts a decoded JSON number only.
func finiteNumber(value any) (float64, bool) {
	var n float64
	switch v := value.(type) {
	case float64:
		n = v
	case json.Number:
		var err error
		if n, err = v.Float64(); err != nil {
			return 0, false
		}
	case int64:
		n = float64(v)
	case int:
		n = float64(v)
	default:
		return 0, false
	}
	return n, !math.IsNaN(n) && !math.IsInf(n, 0)
}

func (c Config) home() string {
	if c.Home != "" {
		return c.Home
	}
	home, _ := os.UserHomeDir()
	return home
}

func (c Config) lookupEnv(name string) (string, bool) {
	if c.LookupEnv != nil {
		return c.LookupEnv(name)
	}
	return os.LookupEnv(name)
}
