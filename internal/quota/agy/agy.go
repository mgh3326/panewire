// Package agy measures the agy (Antigravity) pool's quota the way pacelever
// does: the running app's own loopback server first, then the cloud quota
// summary with the CLI's OAuth login, with the same group-scoped buckets and
// error kinds (docs/quota.md §Native measurement: agy).
//
// agy meters per group — the gemini models and the third-party ("3p": Claude
// and GPT) models draw from separate quotas — so a reading carries buckets of
// scope "group", named by the group, and never an account-wide bucket.
//
// The OAuth token and the app's CSRF token are read into memory for the
// requests of one measurement. They are never logged, written, placed in an
// error or on a command line; the CSRF token is sent only to 127.0.0.1.
package agy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// The metering groups. A bucket's scope name is one of these; "unknown" is an
// identity that maps to neither, kept visible but never matched to a rung.
const (
	GroupGemini  = "gemini"
	Group3P      = "3p"
	GroupUnknown = "unknown"
)

const (
	apiTimeout    = 20 * time.Second
	apiBodyLimit  = 2 << 20
	authFileLimit = 1 << 20
	wakeTimeout   = 30 * time.Second
	authHint      = "antigravity authentication failed; run agy to refresh the login"
	// UserAgent is the User-Agent header sent to the cloud quota endpoints: the
	// agy CLI's own header string, which restores the Gemini weekly and Argon
	// quota pools.
	UserAgent = "antigravity/cli/1.4.12 (aidev_client; os_type=darwin; arch=arm64; auth_method=consumer)"
)

// cloudBases are the cloud quota hosts, in pacelever's order.
var cloudBases = []string{"https://daily-cloudcode-pa.googleapis.com", "https://cloudcode-pa.googleapis.com"}

// Doer is the HTTP client the requests go through.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config is everything a measurement reads from its environment. The zero
// value measures this host's app and login against the real endpoints,
// filesystem, processes and wall clock; tests replace the seams.
type Config struct {
	// HTTP is the cloud client; nil is a 20 s client that refuses redirects.
	HTTP Doer
	// LocalHTTP is the loopback client for the app's server; nil is a client
	// that skips certificate checks for the app's self-signed port and refuses
	// redirects. It is only ever pointed at 127.0.0.1.
	LocalHTTP Doer
	// Now is the clock for a Retry-After date; nil is the wall clock.
	Now func() time.Time
	// Home is the home directory; empty is os.UserHomeDir.
	Home string
	// GOOS names the platform; empty is runtime.GOOS. The keychain is read on
	// darwin, the secret service on linux, and neither elsewhere.
	GOOS string
	// LookupEnv reads the environment; nil is os.LookupEnv.
	LookupEnv func(string) (string, bool)
	// ReadFile reads the login file; nil is os.ReadFile.
	ReadFile func(string) ([]byte, error)
	// Keychain returns the CLI's keychain item (`security find-generic-password
	// -s gemini -a antigravity -w`); nil runs `security`.
	Keychain func(ctx context.Context) (string, error)
	// SecretService returns the CLI's secret service item (attributes service
	// "gemini" and username "antigravity"); nil reads the session bus.
	SecretService func(ctx context.Context) (string, error)
	// LookPath finds agy on PATH; nil is exec.LookPath.
	LookPath func(string) (string, error)
	// Wake re-arms the login by listing models with the agy binary; nil runs
	// `agy models` with its output discarded.
	Wake func(ctx context.Context, binary string) error
	// WakeLock is the lock file that keeps wake single-flight; empty uses the
	// panewire cache path with a legacy fallback. Locked runs fn under it; nil
	// takes the real advisory lock.
	WakeLock string
	Locked   func(path string, fn func() error) error
	// Processes lists candidate app servers (`pgrep -a -lf`); nil runs pgrep.
	Processes func(context.Context) []Proc
	// ListenPorts lists a process's listening TCP ports (`lsof`); nil runs lsof.
	ListenPorts func(ctx context.Context, pid int) []int
	// Account is the 16-hex fingerprint of the configured agy account. It is
	// attached to the observation as the account measured; agy has no login
	// from which to derive one, so it is configured, never guessed.
	Account string
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

func (c Config) goos() string {
	if c.GOOS != "" {
		return c.GOOS
	}
	return runtime.GOOS
}

func (c Config) env(name string) string {
	if c.LookupEnv != nil {
		value, _ := c.LookupEnv(name)
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(os.Getenv(name))
}

// base is an XDG root: the variable when set, else a directory under home.
func (c Config) base(variable, fallback string) string {
	if dir := c.env(variable); dir != "" {
		return dir
	}
	return filepath.Join(c.home(), fallback)
}

// Measure reads the agy quota: the running app's local server when one
// answers, else the cloud summary (and per-model list) with the saved login,
// re-armed once through `agy models` if the login is refused. Without a local
// answer or a login nothing is sent. The error is a *quota.Error.
func Measure(ctx context.Context, cfg Config) (quota.Observation, error) {
	if observed, ok := cfg.local(ctx); ok {
		observed.AccountFP = cfg.Account
		return observed, nil
	}
	observed, err := cfg.cloud(ctx)
	if err != nil {
		return quota.Observation{}, err
	}
	observed.AccountFP = cfg.Account
	return observed, nil
}

// cloud is pacelever's agyProvider.Fetch after the local source: the file
// login, else the OS keyring's (the keychain on macOS, the secret service on
// Linux); a refused login is re-armed once and retried, and a refused file
// login falls back to the keyring copy.
func (c Config) cloud(ctx context.Context) (quota.Observation, error) {
	token := c.fileToken()
	usedSecret := false
	if token == "" {
		if token = c.secretToken(ctx); token != "" {
			usedSecret = true
		}
	}
	if token == "" {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	observed, err := c.fetch(ctx, token)
	if quota.KindOf(err) != quota.ErrKindAuthFailed {
		return observed, err
	}
	secretRefused := usedSecret
	binary := c.binary()
	switch {
	case binary == "":
		err = quota.Fail(quota.ErrKindAuthFailed, "antigravity authentication failed; the agy command is unavailable to refresh the login")
	default:
		fresh, freshSecret, wakeErr := c.refresh(ctx, binary, token, usedSecret)
		if wakeErr != nil {
			err = quota.Fail(quota.ErrKindAuthFailed, "antigravity authentication failed; `agy models` could not refresh the login")
			break
		}
		token, usedSecret = fresh, freshSecret
		if observed, err = c.fetch(ctx, token); quota.KindOf(err) != quota.ErrKindAuthFailed {
			return observed, err
		}
		if usedSecret {
			secretRefused = true
		}
	}
	// A file login that is missing or still refused after the re-arm falls back
	// to the keyring copy. A refused fallback keeps the refresh hint rather
	// than masking it behind the same failure.
	if !secretRefused {
		if secret := c.secretToken(ctx); secret != "" && secret != token {
			if alt, altErr := c.fetch(ctx, secret); altErr == nil {
				return alt, nil
			}
		}
	}
	return quota.Observation{}, err
}

// fetch reads the cloud summary with token from the first host that answers.
func (c Config) fetch(ctx context.Context, token string) (quota.Observation, error) {
	headers := map[string]string{"Authorization": "Bearer " + token, "User-Agent": UserAgent, "Content-Type": "application/json"}
	var last jsonReply
	gotStatus := false
	for _, base := range cloudBases {
		reply, err := doJSON(ctx, c.HTTP, http.MethodPost, base+"/v1internal:retrieveUserQuotaSummary", headers, []byte("{}"))
		if err != nil {
			continue
		}
		last, gotStatus = reply, true
		if reply.status == http.StatusUnauthorized || reply.status == http.StatusForbidden {
			return quota.Observation{}, quota.Fail(quota.ErrKindAuthFailed, authHint)
		}
		if reply.status == http.StatusOK && !reply.parseError {
			return parse(reply.body, c.fetchModels(ctx, headers, base))
		}
	}
	switch {
	case gotStatus && last.status == http.StatusOK:
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "response is not valid JSON")
	case gotStatus:
		return quota.Observation{}, statusFailure(last, c.now())
	}
	return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
}

// statusFailure maps a non-200 cloud answer to a safe failure. A refused login
// is auth_failed (the other 4xx are http), a 429 is rate_limited with
// Retry-After, and a server error is a retryable network failure rather than a
// sign the login expired.
func statusFailure(reply jsonReply, now time.Time) *quota.Error {
	if reply.status >= 500 {
		return quota.Fail(quota.ErrKindNetwork, "antigravity temporarily unavailable")
	}
	failure := quota.StatusError(reply.status, reply.header, now, 0)
	if failure.Kind == quota.ErrKindAuth {
		failure.Kind, failure.Message = quota.ErrKindAuthFailed, authHint
	}
	return failure
}

// fetchModels reads the per-model quota list. The summary groups omit a
// metering group the account does not report; each model's quotaInfo still
// carries that group's session (5h) quota. Best effort: a failure keeps the
// summary-only reading. The host that answered the summary goes first.
func (c Config) fetchModels(ctx context.Context, headers map[string]string, answered string) map[string]any {
	hosts := []string{answered}
	for _, base := range cloudBases {
		if base != answered {
			hosts = append(hosts, base)
		}
	}
	for _, base := range hosts {
		reply, err := doJSON(ctx, c.HTTP, http.MethodPost, base+"/v1internal:fetchAvailableModels", headers, []byte("{}"))
		if err != nil || reply.status != http.StatusOK || reply.parseError {
			continue
		}
		root := reply.body
		if response := object(root["response"]); len(response) > 0 {
			root = response
		}
		return object(root["models"])
	}
	return nil
}

type jsonReply struct {
	status     int
	header     http.Header
	body       map[string]any
	parseError bool
}

// doJSON performs one request. It never returns the endpoint, a credential or
// a response body in an error.
func doJSON(ctx context.Context, client Doer, method, endpoint string, headers map[string]string, payload []byte) (jsonReply, error) {
	if client == nil {
		client = &http.Client{Timeout: apiTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return jsonReply{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return jsonReply{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	defer response.Body.Close()
	out := jsonReply{status: response.StatusCode, header: response.Header}
	if json.NewDecoder(io.LimitReader(response.Body, apiBodyLimit)).Decode(&out.body) != nil {
		out.parseError = true
	}
	return out, nil
}

// parse reads a quota summary (or the per-model answers shaped like one) into
// group-scoped buckets, the worst reading per group and window, ordered gemini,
// 3p, unknown and by window length.
func parse(raw, extraModels map[string]any) (quota.Observation, error) {
	observed := quota.Observation{Source: quota.SourceUsageAPI}
	root := raw
	if response := object(raw["response"]); len(response) > 0 {
		root = response
	}
	worst := map[string]quota.Bucket{}
	add := func(group, window string, fraction float64, reset string) {
		// An unmappable identity is emitted as group "unknown": visible in the
		// output but matched to no rung, so it fails closed instead of passing
		// as an account-wide bucket.
		if group == "" {
			group = GroupUnknown
		}
		if window == "" {
			window = "unknown"
		}
		if math.IsNaN(fraction) || fraction < 0 || fraction > 1 {
			return
		}
		bucket := quota.Bucket{Window: window, UsedPct: quota.Pct(100 * (1 - fraction)), ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeGroup, Name: group}}
		key := group + "\x00" + window
		if previous, ok := worst[key]; !ok || previous.UsedPct != nil && bucket.UsedPct != nil && *bucket.UsedPct > *previous.UsedPct {
			worst[key] = bucket
		}
	}
	for _, groupValue := range list(root["groups"]) {
		for _, value := range list(object(groupValue)["buckets"]) {
			bucket := object(value)
			fraction, ok := number(bucket["remainingFraction"])
			if !ok {
				continue
			}
			id := text(bucket["bucketId"])
			grp, win, known := summaryBucket(id, text(bucket["window"]))
			if !known {
				slog.Warn("agy unknown quota bucket", "bucket_id", id)
				grp = GroupUnknown
				win = bucketWindow(text(bucket["window"]), id)
				if win == "" {
					win = "unknown"
				}
			}
			add(grp, win, fraction, resetTime(bucket["resetTime"]))
		}
	}
	addModels := func(models map[string]any) {
		for key, value := range models {
			entry := object(value)
			if entry["isInternal"] == true {
				continue
			}
			info := object(entry["quotaInfo"])
			fraction, ok := number(info["remainingFraction"])
			if !ok {
				continue
			}
			window := bucketWindow(text(info["window"]), key)
			switch {
			case window != "":
			case text(info["window"]) == "":
				// Per-model quotaInfo without a window is the session quota.
				window = "5h"
			default:
				// An explicit window we do not recognize stays unmetered
				// instead of masquerading as the session quota.
				window = "unknown"
			}
			add(Group(key, text(entry["displayName"])), window, fraction, resetTime(info["resetTime"]))
		}
	}
	if len(worst) == 0 {
		addModels(object(root["models"]))
	}
	addModels(extraModels)
	for _, group := range []string{GroupGemini, Group3P, GroupUnknown} {
		for _, window := range []string{"5h", "7d", "30d", "1d", "unknown"} {
			if bucket, ok := worst[group+"\x00"+window]; ok {
				observed.Buckets = append(observed.Buckets, bucket)
			}
		}
	}
	for _, key := range []string{"planDaily", "plan"} {
		plan := object(raw[key])
		name := text(object(plan["paidTier"])["name"])
		if name == "" {
			name = text(object(plan["currentTier"])["name"])
		}
		if name != "" {
			observed.Plan = strings.ToUpper(name)
			break
		}
	}
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "no usable windows")
	}
	return observed, nil
}

// summaryBucket maps a quota summary bucket's id to its metering group and
// window length. Known bucket IDs match exactly; Argon is kept separate from
// the Gemini group so an exhausted Argon pool does not block flash38 rungs.
// An unknown bucket returns known=false.
func summaryBucket(bucketID, windowAttr string) (group, window string, known bool) {
	id := strings.ToLower(strings.TrimSpace(bucketID))
	switch id {
	case "gemini-5h":
		return GroupGemini, "5h", true
	case "gemini-weekly":
		return GroupGemini, "7d", true
	case "3p-5h":
		return Group3P, "5h", true
	case "3p-weekly":
		return Group3P, "7d", true
	case "gemini-argon-wtu-weekly":
		return GroupUnknown, "7d", true
	}
	if strings.Contains(id, "argon") {
		w := bucketWindow(windowAttr, id)
		if w == "" {
			w = "7d"
		}
		return GroupUnknown, w, true
	}
	return "", "", false
}

// bucketWindow is a bucket's window label: the reported period, else the
// bucket id's suffix; "" when neither names one.
func bucketWindow(window, bucketID string) string {
	switch strings.ToLower(strings.TrimSpace(window)) {
	case "weekly":
		return "7d"
	case "5h", "session":
		return "5h"
	case "daily":
		return "1d"
	}
	id := strings.ToLower(bucketID)
	switch {
	case strings.HasSuffix(id, "-weekly"):
		return "7d"
	case strings.HasSuffix(id, "-5h"):
		return "5h"
	}
	return ""
}

// Group maps a bucket or model identity to the metered group: gemini models
// are never gated on the Claude/GPT ("3p") numbers. It is pacelever's agyGroup,
// and the catalog rung's group comes from its model id through it. display is
// the identity's display text, which may be empty.
func Group(id, display string) string {
	identity := strings.ToLower(strings.TrimSpace(id))
	switch {
	case strings.Contains(identity, "argon"):
		return GroupUnknown
	case strings.HasPrefix(identity, "gemini"):
		return GroupGemini
	case strings.HasPrefix(identity, "3p-"), strings.HasPrefix(identity, "3p_"):
		return Group3P
	}
	joined := strings.ToLower(display + " " + identity)
	switch {
	case strings.Contains(joined, "argon"):
		return GroupUnknown
	case strings.Contains(joined, "gemini"):
		return GroupGemini
	case strings.Contains(joined, "claude"), strings.Contains(joined, "gpt"), strings.Contains(joined, "oss"):
		return Group3P
	}
	return GroupUnknown
}

func object(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func list(v any) []any {
	items, _ := v.([]any)
	return items
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

// binary is the agy executable that can re-arm the login, "" when none is
// installed: PANEWIRE_AGY_BIN (PACELEVER_AGY_BIN fallback), else agy on PATH.
func (c Config) binary() string {
	look := c.LookPath
	if look == nil {
		look = exec.LookPath
	}
	name := "agy"
	explicit := c.env("PANEWIRE_AGY_BIN")
	if explicit == "" {
		explicit = c.env("PACELEVER_AGY_BIN")
	}
	if explicit != "" {
		name = explicit
		if rest, ok := strings.CutPrefix(explicit, "~/"); ok {
			name = filepath.Join(c.home(), rest)
		}
	}
	path, err := look(name)
	if err != nil {
		return ""
	}
	return path
}
