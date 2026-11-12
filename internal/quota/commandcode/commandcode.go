// Package commandcode measures the commandcode pool's quota the way pacelever
// does — the CLI login key, the same headers, the 5h and 7d window limits of
// /alpha/billing/credits, the same account fingerprint — and adds the monthly
// window pacelever could not read: 30d is the share of the billing period's
// plan credits already spent (docs/quota.md §Native measurement: commandcode).
//
// The API key is read into memory for the requests of one measurement. It is
// never logged, written, placed in an error, or put on a command line.
package commandcode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	// APIBase is the commandcode API host.
	APIBase = "https://api.commandcode.ai"
	// UserAgent is the CLI's own user agent: the API's edge rejects generic
	// clients. It is the one pacelever sends.
	UserAgent     = "cmd/1.88.1"
	apiTimeout    = 20 * time.Second
	apiBodyLimit  = 2 << 20
	authFileLimit = 1 << 20
	// billingPeriod is the usage summary's periodBasis that the 30d window is
	// defined on; any other basis is not the plan's monthly allowance.
	billingPeriod = "billing-period"
)

// Doer is the HTTP client the requests go through.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config is everything a measurement reads from its environment. The zero
// value measures this host's login against the real API, filesystem and wall
// clock; tests replace the seams.
type Config struct {
	HTTP    Doer   // nil: 20 s timeout, redirects refused
	BaseURL string // empty: APIBase
	// Now is the clock for a Retry-After date; nil is the wall clock.
	Now func() time.Time
	// Home is the home directory; empty is os.UserHomeDir.
	Home string
	// LookupEnv reads the environment; nil is os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// ReadFile reads the CLI login file; nil is os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Identities remembers the account behind a key between measurements, so
	// the fingerprint read before each refresh costs no request once known.
	// Nil asks every time.
	Identities *IdentityCache
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c Config) lookupEnv(name string) string {
	if c.LookupEnv != nil {
		value, _ := c.LookupEnv(name)
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(os.Getenv(name))
}

// token is the CLI login key: pacelever's two environment names, then the
// apiKey of ~/.commandcode/auth.json. An unreadable, oversized or malformed
// file has none.
func (c Config) token() string {
	for _, name := range []string{"COMMAND_CODE_API_KEY", "COMMANDCODE_API_KEY"} {
		if value := c.lookupEnv(name); value != "" {
			return value
		}
	}
	home := c.Home
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	read := c.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	data, err := read(filepath.Join(home, ".commandcode", "auth.json"))
	if err != nil || len(data) > authFileLimit {
		return ""
	}
	var file struct {
		APIKey string `json:"apiKey"`
	}
	if json.Unmarshal(data, &file) != nil {
		return ""
	}
	return strings.TrimSpace(file.APIKey)
}

// Identity is the account a key belongs to: Org scopes the credits request,
// FP is the shared-account fingerprint ("" when the API names no account).
type Identity struct {
	Org string
	FP  string
}

// IdentityCache remembers the identity of the last key it saw.
type IdentityCache struct {
	mu       sync.Mutex
	keyHash  [sha256.Size]byte
	identity Identity
	known    bool
}

func (c *IdentityCache) get(token string) (Identity, bool) {
	if c == nil {
		return Identity{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.identity, c.known && c.keyHash == sha256.Sum256([]byte(token))
}

func (c *IdentityCache) put(token string, identity Identity) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keyHash, c.identity, c.known = sha256.Sum256([]byte(token)), identity, true
}

// fingerprint is hex(sha256("commandcode-account|"+id))[:16]; no account has
// none. It is pacelever's accountFP("commandcode", id).
func fingerprint(account string) string {
	if account == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("commandcode-account|" + account))
	return hex.EncodeToString(sum[:8])
}

// AccountFP is this host's commandcode account fingerprint, "" when the API
// names no account for the key. It needs one request (whoami) the first time a
// key is seen, with exactly the hash a measurement attaches to its
// observation. Without a key nothing is sent and the failure is credentials.
func AccountFP(ctx context.Context, cfg Config) (string, error) {
	token := cfg.token()
	if token == "" {
		return "", quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	if identity, ok := cfg.Identities.get(token); ok {
		return identity.FP, nil
	}
	identity, err := cfg.whoami(ctx, token)
	if err != nil {
		return "", err
	}
	return identity.FP, nil
}

// Measure reads the commandcode quota. Without a key nothing is sent. The
// 5h and 7d windows come from the credits response and are required; 30d needs
// the usage summary as well, and the subscription for its reset. A secondary
// request that fails leaves its window out — never a guessed value — unless
// the context ended, which fails the whole measurement. The error is a
// *quota.Error.
func Measure(ctx context.Context, cfg Config) (quota.Observation, error) {
	token := cfg.token()
	if token == "" {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	identity, err := cfg.whoami(ctx, token)
	if err != nil {
		return quota.Observation{}, err
	}
	path := "/alpha/billing/credits"
	if identity.Org != "" {
		path += "?orgId=" + url.QueryEscape(identity.Org)
	}
	credits, err := cfg.get(ctx, token, path)
	if err != nil {
		return quota.Observation{}, err
	}
	observed := quota.Observation{Source: quota.SourceUsageAPI, AccountFP: identity.FP}
	observed.Buckets = windowBuckets(credits)
	remaining, remainingOK := monthlyRemaining(credits)

	// The secondary reads are best effort: their loss costs the 30d window and
	// the plan name, not the reading.
	var spent *float64
	if remainingOK {
		if summary, err := cfg.get(ctx, token, "/alpha/usage/summary"); err == nil {
			spent = monthlySpent(summary)
		} else if ctx.Err() != nil {
			return quota.Observation{}, err
		}
	}
	reset := ""
	if subscription, err := cfg.get(ctx, token, "/alpha/billing/subscriptions"); err == nil {
		reset, observed.Plan = subscriptionPeriod(subscription)
	} else if ctx.Err() != nil {
		return quota.Observation{}, err
	}
	if spent != nil {
		if total := *spent + remaining; total > 0 {
			if pct := quota.Pct(100 * *spent / total); pct != nil {
				observed.Buckets = append(observed.Buckets, quota.Bucket{Window: "30d", UsedPct: pct, ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeAccount}})
			}
		}
	}
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "usage response has no usable windows")
	}
	return observed, nil
}

// whoami asks who the key is and remembers the answer. pacelever's rule: the
// account is the user id, else the org id; a body that says success=false is a
// refusal.
func (c Config) whoami(ctx context.Context, token string) (Identity, error) {
	body, err := c.get(ctx, token, "/alpha/whoami?limits=1")
	if err != nil {
		return Identity{}, err
	}
	data := object(body["data"])
	if len(data) == 0 {
		data = body
	}
	if success, ok := data["success"].(bool); ok && !success {
		return Identity{}, quota.Fail(quota.ErrKindHTTP, "commandcode rejected the request")
	}
	org := text(object(data["org"])["id"])
	account := text(object(data["user"])["id"])
	if account == "" {
		account = org
	}
	identity := Identity{Org: org, FP: fingerprint(account)}
	// Only a named account is remembered: a whoami that omitted both ids must
	// be asked again, not pin the key to "no account" after the API recovers.
	if identity.FP != "" {
		c.Identities.put(token, identity)
	}
	return identity, nil
}

// get is one authenticated GET answered with its JSON object. Every failure is
// a kind plus a fixed sentence; the response body, the URL and the key never
// reach an error. Auth failures follow pacelever: a 403 whose body is not JSON
// is the edge refusing the client, any other 401/403 a refused login.
func (c Config) get(ctx context.Context, token, path string) (map[string]any, error) {
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: apiTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	base := c.BaseURL
	if base == "" {
		base = APIBase
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if err != nil {
		return nil, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", UserAgent)
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || os.IsTimeout(err) {
			return nil, quota.Fail(quota.ErrKindTimeout, "usage request timed out")
		}
		return nil, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	defer response.Body.Close()
	var body map[string]any
	decoded := json.NewDecoder(io.LimitReader(response.Body, apiBodyLimit)).Decode(&body) == nil
	switch status := response.StatusCode; {
	case status == http.StatusOK:
		if !decoded {
			return nil, quota.Fail(quota.ErrKindParse, "invalid usage JSON")
		}
		return body, nil
	case status == http.StatusForbidden && !decoded:
		return nil, quota.Fail(quota.ErrKindEdgeRejected, "commandcode edge rejected the request")
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return nil, quota.Fail(quota.ErrKindAuthFailed, "commandcode authentication failed")
	default:
		// No default wait on a bare 429: the shared flow backs off itself.
		return nil, quota.StatusError(status, response.Header, c.now(), 0)
	}
}

// payload is the response's data object, or the response itself when it has
// no data envelope.
func payload(body map[string]any) map[string]any {
	if data := object(body["data"]); len(data) > 0 {
		return data
	}
	return body
}

// windowBuckets reads windowLimits.fiveHour and .weekly as 5h and 7d: used
// over cap. A window with no positive cap or a malformed used value is left
// out (a malformed value must never read as an unused window); one past its
// cap is exhausted, 100%. The vendor's 7d limit is a rolling window whose
// resetAt reads relative to the reading time (exactly 5 days after the reading
// time), not a fixed calendar reset boundary. It is marked unpaceable (no
// ResetsAt) so projectAtReset does not turn a moving relative reset into a
// false pace (hk 4330). The 30d window carries the billing period reset
// (currentPeriodEnd), and the 5h window keeps its reset.
func windowBuckets(credits map[string]any) []quota.Bucket {
	limits := object(payload(credits)["windowLimits"])
	var buckets []quota.Bucket
	for _, axis := range []struct{ key, window string }{{"fiveHour", "5h"}, {"weekly", "7d"}} {
		w := object(limits[axis.key])
		capacity, capacityOK := number(w["cap"])
		used, usedOK := number(w["used"])
		if !capacityOK || capacity <= 0 || !usedOK || used < 0 {
			continue
		}
		pct := quota.Pct(min(100*used/capacity, 100))
		if pct == nil {
			continue
		}
		reset := resetTime(w["resetAt"])
		if axis.window == "7d" {
			// hk 4330: the vendor's weekly limit is a rolling window whose resetAt
			// always reads reading_time + 5 days. It has no fixed calendar reset
			// boundary and is unpaceable.
			reset = ""
		}
		buckets = append(buckets, quota.Bucket{Window: axis.window, UsedPct: pct, ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeAccount}})
	}
	return buckets
}

// monthlyRemaining is credits.monthlyCredits: the plan credits left this
// billing period, in USD. Purchased and free credits are not part of the
// plan's allowance and are never read.
func monthlyRemaining(credits map[string]any) (float64, bool) {
	remaining, ok := number(object(payload(credits)["credits"])["monthlyCredits"])
	return max(remaining, 0), ok
}

// monthlySpent is the usage summary's totalMonthlyCredits: the plan credits
// spent this billing period. A summary on any other basis, or without a
// non-negative number, gives none.
func monthlySpent(summary map[string]any) *float64 {
	data := payload(summary)
	if basis, ok := data["periodBasis"].(string); ok && basis != billingPeriod {
		return nil
	}
	spent, ok := number(data["totalMonthlyCredits"])
	if !ok || spent < 0 {
		return nil
	}
	return &spent
}

// subscriptionPeriod is the current period's end (the 30d reset) and the plan
// id of data.
func subscriptionPeriod(subscription map[string]any) (reset, plan string) {
	data := payload(subscription)
	return resetTime(data["currentPeriodEnd"]), text(data["planId"])
}

func object(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func text(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

// number accepts a decoded JSON number only.
func number(v any) (float64, bool) {
	n, ok := v.(float64)
	return n, ok && !math.IsNaN(n) && !math.IsInf(n, 0)
}

// resetTime reads a reset sent as epoch seconds, milliseconds or microseconds
// (number or numeric string), RFC 3339 or an HTTP date, as an RFC 3339 string;
// anything else, and a time outside 2000..2200, is "".
func resetTime(v any) string {
	switch x := v.(type) {
	case float64:
		return epochTime(x)
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return ""
		}
		if at, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return at.UTC().Format(time.RFC3339Nano)
		}
		if n, ok := quota.ParseFinite(s); ok {
			return epochTime(n)
		}
		if at, err := http.ParseTime(s); err == nil {
			return at.UTC().Format(time.RFC3339Nano)
		}
	}
	return ""
}

func epochTime(n float64) string {
	if math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n > 9.2e18 {
		return ""
	}
	var at time.Time
	switch {
	case n >= 1e14:
		at = time.UnixMicro(int64(n))
	case n >= 1e11:
		at = time.UnixMilli(int64(n))
	default:
		at = time.Unix(int64(n), 0)
	}
	at = at.UTC()
	if at.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) || at.After(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return ""
	}
	return at.Format(time.RFC3339Nano)
}
