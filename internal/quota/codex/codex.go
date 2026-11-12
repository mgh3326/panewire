// Package codex measures the codex pool's quota the way pacelever does: the
// app-server JSON-RPC source first, the chatgpt.com wham usage API if that
// fails, the same buckets, the same error kinds and the same account
// fingerprint.
//
// The access token is read into memory for the one HTTP request. It is never
// logged, written, placed in an error, or put on a command line; the
// app-server reads its own login from CODEX_HOME.
package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	// UsageEndpoint is the wham usage API.
	UsageEndpoint    = "https://chatgpt.com/backend-api/wham/usage"
	apiTimeout       = 20 * time.Second
	apiBodyLimit     = 2 << 20
	authFileLimit    = 1 << 20
	creditsFullScale = 1000.0
)

// Doer is the HTTP client the usage request goes through.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config is everything a measurement reads from its environment. The zero
// value measures this host's login with the real filesystem, codex binary and
// wall clock; tests replace the seams.
type Config struct {
	HTTP     Doer   // nil: 20 s timeout, redirects refused
	Endpoint string // empty: UsageEndpoint
	// Now is the clock for relative reset times; nil is the wall clock.
	Now func() time.Time
	// Home is the home directory; empty is os.UserHomeDir.
	Home string
	// CodexHome selects the account (the account selector, then CODEX_HOME,
	// then ~/.codex).
	CodexHome string
	// LookupEnv reads the environment; nil is os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// ReadFile reads auth.json; nil is os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Binary is the configured codex executable, tried after the
	// PANEWIRE_CODEX_BIN override and before PATH.
	Binary string
	// GOOS is the platform name; empty is runtime.GOOS. The desktop app's
	// bundled binary is looked up only for "darwin".
	GOOS string
	// LookPath finds codex on PATH; nil is exec.LookPath.
	LookPath func(string) (string, error)
	// IsFile reports whether a path is an existing regular file; nil stats it.
	IsFile func(string) bool
	// RPCTimeout bounds the whole app-server exchange; zero is 15 s.
	RPCTimeout time.Duration
	// StartRPC starts one app-server and returns its line transport; nil
	// spawns the real binary.
	StartRPC func(ctx context.Context, binary, dir, codexHome string) (Transport, error)
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
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

// codexHome is the Codex CLI home: the selector, then CODEX_HOME, then
// ~/.codex. A blank value does not select.
func (c Config) codexHome() string {
	if dir := strings.TrimSpace(c.CodexHome); dir != "" {
		return dir
	}
	if dir, _ := c.lookupEnv("CODEX_HOME"); strings.TrimSpace(dir) != "" {
		return strings.TrimSpace(dir)
	}
	return filepath.Join(c.home(), ".codex")
}

// tokens is the token block of the login file under dir. An unreadable,
// oversized or malformed file has no tokens.
type tokens struct {
	access, account string
}

func (c Config) tokens(dir string) tokens {
	read := c.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	data, err := read(filepath.Join(dir, "auth.json"))
	if err != nil || len(data) > authFileLimit {
		return tokens{}
	}
	var file struct {
		Tokens json.RawMessage `json:"tokens"`
	}
	var block map[string]any
	if json.Unmarshal(data, &file) != nil || json.Unmarshal(file.Tokens, &block) != nil {
		return tokens{}
	}
	access, _ := block["access_token"].(string)
	account, _ := block["account_id"].(string)
	return tokens{access: access, account: account}
}

// fingerprint is hex(sha256("codex-account|"+accountID))[:16]; no account has
// none.
func fingerprint(account string) string {
	if account == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("codex-account|" + account))
	return hex.EncodeToString(sum[:8])
}

// AccountFP is this host's codex account fingerprint, "" when the login has
// no account_id. It is read before any request, with exactly the hash a
// measurement attaches to its observation.
func AccountFP(cfg Config) string {
	return fingerprint(cfg.tokens(cfg.codexHome()).account)
}

// HasLogin reports whether this host has a codex login token. A host without
// one is logged out, which is a different failure from a login that names no
// account.
func HasLogin(cfg Config) bool { return cfg.tokens(cfg.codexHome()).access != "" }

// Measure reads the codex quota: the app-server first, then the wham usage
// API. Without a login token nothing is spawned or sent. The error is a
// *quota.Error; an RPC failure is never reported, only the API's.
func Measure(ctx context.Context, cfg Config) (quota.Observation, error) {
	dir := cfg.codexHome()
	login := cfg.tokens(dir)
	if login.access == "" {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	observed, ok := cfg.fetchRPC(ctx, dir)
	if !ok {
		var err error
		if observed, err = cfg.fetchAPI(ctx, login); err != nil {
			return quota.Observation{}, err
		}
	}
	observed.AccountFP = fingerprint(login.account)
	return observed, nil
}

// fetchAPI is one wham usage request. Every failure is a kind plus a fixed
// sentence; the response body and the token never reach an error.
func (c Config) fetchAPI(ctx context.Context, login tokens) (quota.Observation, error) {
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
	request.Header.Set("Authorization", "Bearer "+login.access)
	if login.account != "" {
		request.Header.Set("ChatGPT-Account-Id", login.account)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "panewire")
	response, err := client.Do(request)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		// No 300 s default on a bare 429: the shared flow backs off itself.
		return quota.Observation{}, quota.StatusError(response.StatusCode, response.Header, c.now(), 0)
	}
	var raw map[string]json.RawMessage
	body := json.NewDecoder(io.LimitReader(response.Body, apiBodyLimit))
	if body.Decode(&raw) != nil || body.Decode(&struct{}{}) != io.EOF {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "invalid usage JSON")
	}
	observed := quota.Observation{Source: quota.SourceUsageAPI}
	parseAPI(&observed, raw, response.Header, c.now())
	if !hasMeasuredBucket(observed.Buckets) {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "usage response has no usable windows")
	}
	return observed, nil
}

// hasMeasuredBucket reports whether any window carries a valid utilisation.
// Windows without one cannot be published, so they are a parse failure here
// rather than a probe_failed later.
func hasMeasuredBucket(buckets []quota.Bucket) bool {
	for _, b := range buckets {
		if b.UsedPct != nil {
			return true
		}
	}
	return false
}

// apiWindow is one wham usage window.
type apiWindow struct {
	Used       *float64 `json:"used_percent"`
	Seconds    float64  `json:"limit_window_seconds"`
	Reset      float64  `json:"reset_at"`
	ResetAfter float64  `json:"reset_after_seconds"`
}

func (w apiWindow) bucket(scope quota.Scope, now time.Time) quota.Bucket {
	reset := ""
	switch {
	case w.Reset > 0:
		reset = time.UnixMilli(int64(w.Reset * 1000)).UTC().Format(time.RFC3339Nano)
	case w.ResetAfter > 0:
		reset = now.Add(time.Duration(w.ResetAfter) * time.Second).UTC().Format(time.RFC3339Nano)
	}
	return quota.Bucket{Window: windowLabel(w.Seconds), UsedPct: validated(w.Used), ResetsAt: reset, Scope: scope}
}

// windowLabel names a window by its length: >=28d is 30d, >=6d 7d, >=20h 1d,
// 4-6h 5h, anything else "<n>s".
func windowLabel(seconds float64) string {
	switch {
	case seconds >= 28*86400:
		return "30d"
	case seconds >= 6*86400:
		return "7d"
	case seconds >= 20*3600:
		return "1d"
	case seconds >= 4*3600 && seconds <= 6*3600:
		return "5h"
	}
	return fmt.Sprintf("%gs", seconds)
}

// parseAPI reads the wham usage body. The x-codex-*-used-percent headers take
// precedence over the body's percentages; the credits balance is read from a
// header or the credits block and measured against a fixed 1000.
func parseAPI(o *quota.Observation, raw map[string]json.RawMessage, header http.Header, now time.Time) {
	_ = json.Unmarshal(raw["plan_type"], &o.Plan)
	var limit struct {
		Primary   *apiWindow `json:"primary_window"`
		Secondary *apiWindow `json:"secondary_window"`
	}
	_ = json.Unmarshal(raw["rate_limit"], &limit)
	headerNames := []string{"x-codex-primary-used-percent", "x-codex-secondary-used-percent"}
	for i, w := range []*apiWindow{limit.Primary, limit.Secondary} {
		if w != nil {
			b := w.bucket(quota.Scope{Kind: quota.ScopeAccount}, now)
			if pct := validated(headerFloat(header, headerNames[i])); pct != nil {
				b.UsedPct = pct
			}
			o.Buckets = append(o.Buckets, b)
		}
	}
	var extra []struct {
		Name  string `json:"limit_name"`
		Limit struct {
			Primary   *apiWindow `json:"primary_window"`
			Secondary *apiWindow `json:"secondary_window"`
		} `json:"rate_limit"`
	}
	_ = json.Unmarshal(raw["additional_rate_limits"], &extra)
	for _, x := range extra {
		for _, w := range []*apiWindow{x.Limit.Primary, x.Limit.Secondary} {
			if w != nil {
				o.Buckets = append(o.Buckets, w.bucket(quota.Scope{Kind: quota.ScopeModel, Name: x.Name}, now))
			}
		}
	}
	var credits struct {
		Has     bool `json:"has_credits"`
		Balance any  `json:"balance"`
	}
	_ = json.Unmarshal(raw["credits"], &credits)
	if credits.Has {
		balance := headerFloat(header, "x-codex-credits-balance")
		if balance == nil {
			balance = looseNumber(credits.Balance)
		}
		if balance != nil {
			used := 100 - (*balance / creditsFullScale * 100)
			o.Buckets = append(o.Buckets, quota.Bucket{Window: "credit", UsedPct: quota.Pct(used), Scope: quota.Scope{Kind: quota.ScopeAccount}})
		}
	}
}

func validated(p *float64) *float64 {
	if !quota.ValidPct(p) {
		return nil
	}
	return p
}

// headerFloat parses a numeric response header; absent or unparsable is nil.
func headerFloat(header http.Header, name string) *float64 {
	if header == nil {
		return nil
	}
	if n, ok := quota.ParseFinite(strings.TrimSpace(header.Get(name))); ok {
		return &n
	}
	return nil
}

// looseNumber is a JSON number or a numeric string as a pointer: nil when the
// value is neither (the credits balance has been observed as "0").
func looseNumber(v any) *float64 {
	switch x := v.(type) {
	case float64:
		if !math.IsNaN(x) && !math.IsInf(x, 0) {
			return &x
		}
	case string:
		if n, ok := quota.ParseFinite(strings.TrimSpace(x)); ok {
			return &n
		}
	}
	return nil
}
