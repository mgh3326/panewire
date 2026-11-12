package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/hkcred"
	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/textbound"
)

// fleetSource is the production PlacementSource. Host connectivity and load
// come from the hub's placement endpoint via panewire; host presence and the
// shared quota cache are read from handoffkeep over HTTPS.
type fleetSource struct{ a *App }

func (s fleetSource) Hosts(ctx context.Context, cwd string) ([]HostView, error) {
	if s.a.PlacementHosts != nil {
		return s.a.PlacementHosts(ctx, cwd)
	}
	// panewire place is an operator call: it rejects a node token.
	if str(s.a.Config.Hub, "hub_url") == "" || firstNonempty(str(s.a.Config.Hub, "operator_token_env"), str(s.a.Config.Hub, "hub_token_env")) == "" {
		return nil, fail(exitConfig, "hub placement unavailable")
	}
	args := append([]string{"place", "--class", "worker", "--signals", "--cwd", cwd}, s.a.controlHubArgs(true)...)
	r := s.a.call(ctx, "panewire", args...)
	if e := check(r, "hub placement"); e != nil {
		return nil, e
	}
	return parseHubHosts(r.Out)
}

func (s fleetSource) Presence(ctx context.Context) ([]PresenceRecord, error) {
	c, e := s.a.quotaClient()
	if e != nil {
		return nil, e
	}
	return c.presence(ctx)
}

func (s fleetSource) SharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	c, e := s.a.quotaClient()
	if e != nil {
		return SharedAccount{Key: key, Degraded: true}, e
	}
	return c.sharedAccount(ctx, key)
}

// cliWaker drives the in-flight `panewire node wake` contract: request the
// node, hold it awake for a bounded window and let the caller wait for the
// connect.
type cliWaker struct{ a *App }

func (w cliWaker) Wake(ctx context.Context, host string, req WakeRequest) error {
	hold := req.HoldMinutes
	if hold <= 0 {
		hold = 10
	}
	wait := req.WaitSeconds
	if wait <= 0 {
		wait = 90
	}
	args := []string{"node", "wake", "--target", host,
		"--hold", fmt.Sprintf("%dm", hold),
		"--timeout", fmt.Sprintf("%ds", wait),
		"--reason", req.Reason}
	args = append(args, w.a.controlHubArgs(true)...)
	r := w.a.call(ctx, "panewire", args...)
	if e := check(r, "node wake"); e != nil {
		return e
	}
	return nil
}

// quotaURL is the configured shared-quota endpoint: hosts.toml [quota] url,
// else the svc-panewire credential's HANDOFFKEEP_URL (hkcred.URL), even when
// that credential lacks its token.
func (a *App) quotaURL() string {
	if u := str(a.Config.Quota, "url"); u != "" {
		return u
	}
	return hkcred.URL()
}

// quotaClient resolves the handoffkeep client every jobs, bench, presence and
// shared-quota read uses. The bearer is panewire's svc-panewire credential
// (internal/hkcred: HANDOFFKEEP_URL + HANDOFFKEEP_TOKEN_SVC_PANEWIRE in the
// environment, else the mode-0600 ~/.config/panewire/hk-svc.env, path
// overridable by PANEWIRE_HK_ENV); hosts.toml [quota] url overrides the URL.
// The hk CLI's config.env and HANDOFFKEEP_TOKEN are never read, and
// [quota] credential_env is retired (hk 3570). Errors name keys and the file,
// never URLs or credentials.
func (a *App) quotaClient() (*hkQuota, error) {
	q := a.Config.Quota
	creds, err := hkcred.Load()
	if err != nil {
		return nil, fail(exitConfig, "shared quota cache unavailable: %v", err)
	}
	u := creds.URL
	if configured := str(q, "url"); configured != "" {
		u = configured
	}
	parsed, e := url.Parse(u)
	if e != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fail(exitConfig, "invalid shared quota endpoint")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, fail(exitConfig, "invalid shared quota endpoint scheme")
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) && !(boolean(q, "allow_plaintext_tailnet", false) && isTailnetHost(parsed.Hostname())) {
		return nil, fail(exitConfig, "shared quota cache requires HTTPS")
	}
	return &hkQuota{base: strings.TrimRight(u, "/"), token: creds.Token, cfID: creds.CFID, cfSecret: creds.CFSecret, now: a.Now, freshTTL: a.sharedFreshTTL}, nil
}

// hkQuota reads the shared quota cache and pacelever host presence from the
// handoffkeep HTTP API. Redirects are refused so authentication never follows
// another origin; response bodies and transport details stay out of errors.
type hkQuota struct {
	base, token, cfID, cfSecret string
	now                         func() time.Time
	freshTTL                    func(pool string) time.Duration
	client                      *http.Client
}

func (c *hkQuota) get(ctx context.Context, path string, out any) error {
	return c.request(ctx, http.MethodGet, path, nil, out)
}

// request is the shared handoffkeep HTTP call: bearer plus optional CF Access
// headers, JSON in and out, redirects refused so authentication never follows
// another origin. Non-2xx answers yield BackendError carrying the status and,
// when the body names one, the hk error code — never the body itself.
func (c *hkQuota) request(ctx context.Context, method, path string, body, out any) error {
	callCtx, cancel := context.WithTimeout(ctx, hkCallTimeout)
	defer cancel()

	client := c.client
	if client == nil {
		client = &http.Client{Timeout: hkCallTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	var rdr io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return fmt.Errorf("invalid backend request body")
		}
		rdr = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(callCtx, method, c.base+path, rdr)
	if e != nil {
		return fmt.Errorf("invalid backend request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.cfID != "" {
		req.Header.Set("CF-Access-Client-Id", c.cfID)
		req.Header.Set("CF-Access-Client-Secret", c.cfSecret)
	}
	resp, e := client.Do(req)
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) || hkRequestTimedOut(e) {
			return fmt.Errorf("handoffkeep request timed out after %s", hkCallTimeout)
		}
		return fmt.Errorf("%w", errHKTransport)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var er struct {
			Code    string `json:"error"`
			Reason  string `json:"reason"`
			Pattern string `json:"pattern"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&er)
		pattern := ""
		if safeHKPattern.MatchString(er.Pattern) {
			pattern = er.Pattern
		}
		return &BackendError{Status: resp.StatusCode, Code: er.Code, Reason: backendReason(er.Reason), Pattern: pattern}
	}
	if out != nil {
		if e := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out); e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("handoffkeep request timed out after %s", hkCallTimeout)
			}
			return fmt.Errorf("handoffkeep response is not valid JSON")
		}
	}
	return nil
}

func hkRequestTimedOut(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// BackendError reports an HTTP failure without response details. Reason is
// hk's store-authored refusal reason (the failing field or rule, hk #197),
// bounded and flattened to one line; nothing else of the body is kept.
type BackendError struct {
	Status  int
	Code    string
	Reason  string
	Pattern string
}

var safeHKPattern = regexp.MustCompile(`^[a-z0-9_]{1,40}$`)

var errHKTransport = errors.New("handoffkeep transport failure")

// backendReasonMaxBytes bounds the hk refusal reason a warning or a
// quarantine sidecar may carry.
const backendReasonMaxBytes = 200

func backendReason(reason string) string {
	reason = strings.Join(strings.Fields(reason), " ")
	if len(reason) > backendReasonMaxBytes {
		return textbound.PrefixWithin(reason, backendReasonMaxBytes) + "…"
	}
	return reason
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("handoffkeep HTTP %d (response withheld)", e.Status)
}

// presence reads the host-presence records pacelever publishes. The endpoint
// is delivered with pacelever v0.3.0's presence contract; the decoder accepts
// the documented record list under any of the plausible envelope keys.
func (c *hkQuota) presence(ctx context.Context) ([]PresenceRecord, error) {
	var body json.RawMessage
	if e := c.get(ctx, "/v1/quota/presence", &body); e != nil {
		return nil, e
	}
	var rows []PresenceRecord
	if json.Unmarshal(body, &rows) == nil && rows != nil {
		return rows, nil
	}
	var env map[string]json.RawMessage
	if e := json.Unmarshal(body, &env); e != nil {
		return nil, fmt.Errorf("handoffkeep presence response invalid")
	}
	for _, k := range []string{"records", "presence", "rows"} {
		if v, ok := env[k]; ok && json.Unmarshal(v, &rows) == nil && rows != nil {
			return rows, nil
		}
	}
	return nil, fmt.Errorf("handoffkeep presence response invalid")
}

// sharedAccount reads the latest shared observation for one account key and
// classes its freshness; an unreadable answer is degraded, not absent.
func (c *hkQuota) sharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	var body struct {
		Observation *struct {
			Kind        string             `json:"kind"`
			MeasuredAt  time.Time          `json:"measured_at"`
			ConfirmedAt *time.Time         `json:"confirmed_at"`
			Buckets     []accountBucket    `json:"buckets"`
			FreeBucket  *shared.FreeBucket `json:"free_bucket"`
		} `json:"observation"`
	}
	if i := strings.IndexByte(key, ':'); i <= 0 || !validID(key[:i]) || !accountFP.MatchString(key[i+1:]) {
		return SharedAccount{Key: key, Degraded: true}, fmt.Errorf("invalid quota account key")
	}
	if e := c.get(ctx, "/v1/quota/accounts/"+url.PathEscape(key)+"/latest", &body); e != nil {
		return SharedAccount{Key: key, Degraded: true}, e
	}
	acc := SharedAccount{Key: key}
	if o := body.Observation; o != nil && o.Kind == "auto" {
		acc.Observed = true
		// handoffkeep keeps measured_at when a re-measurement publishes the
		// same values and only advances confirmed_at, so the reading is valid
		// as of the later of the two (pacelever sharedValidAt).
		acc.MeasuredAt = o.MeasuredAt
		if o.ConfirmedAt != nil && o.ConfirmedAt.After(o.MeasuredAt) {
			acc.MeasuredAt = *o.ConfirmedAt
		}
		ttl := 300 * time.Second
		if c.freshTTL != nil {
			ttl = c.freshTTL(accountPool(key))
		}
		if age := c.now().Sub(acc.MeasuredAt); age >= -time.Minute && age <= ttl {
			acc.Fresh = true
		}
		acc.Quota = withFree(accountPoolView(o.Buckets, c.now()), o.FreeBucket, c.now())
	}
	return acc, nil
}

// withFree attaches a reading's free-tier state (devin's SWE-2 bucket) to its
// pool view. A free-only reading (no paid window) still yields a view: the
// free rung ranks on it, and every paid rung of that pool stays quota_unknown.
func withFree(v *PoolView, free *shared.FreeBucket, now time.Time) *PoolView {
	f, ok := shared.NormalizeFree(free, now)
	if !ok || f == nil {
		return v
	}
	if v == nil {
		v = &PoolView{Status: "ok", Class: "spend"}
	}
	v.Free = f
	return v
}

// accountBucket is the allowlisted shared-observation wire format. Model
// buckets are left to the exact profile's gate; account windows rank it. A
// reading with no account window at all (agy publishes only group windows)
// ranks on its group windows instead, so the pool is not quota_unknown.
type accountBucket struct {
	Window   string   `json:"window"`
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt string   `json:"resets_at"`
	Scope    string   `json:"scope"`
	Model    string   `json:"model,omitempty"`
}

func accountPoolView(buckets []accountBucket, now time.Time) *PoolView {
	if len(buckets) == 0 {
		return nil
	}
	rankScope := "group"
	if slices.ContainsFunc(buckets, func(b accountBucket) bool { return b.Scope == "account" }) {
		rankScope = "account"
	}
	// The account's own view: for a group-only reading its strictest group
	// gates every rung that maps to no group (PoolView.forRung).
	v := rankedPoolView(buckets, now, rankScope, "")
	if v == nil || rankScope != "group" {
		return v
	}
	// Each group's own view ranks and blocks the rungs that draw from it.
	v.Groups = map[string]PoolView{}
	for _, b := range buckets {
		if _, done := v.Groups[b.Model]; done || b.Scope != "group" || b.Model == "" {
			continue
		}
		if g := rankedPoolView(buckets, now, rankScope, b.Model); g != nil {
			v.Groups[b.Model] = *g
		}
	}
	// A rung that maps to no group is blocked and paced on the strictest
	// group: blocking keeps today's conservative rule, and waste is judged on
	// the same group's windows instead of on whichever group had a daily
	// window (hk 3415).
	if strict, ok := strictestGroup(v.Groups); ok {
		groups := v.Groups
		*v = strict
		v.Groups = groups
	}
	return v
}

// strictestGroup is the group whose whole view the account view of a
// group-only reading takes: the highest blocking share, then the highest
// projection at reset; equal views keep the lexically first group name, so the
// choice is deterministic.
func strictestGroup(groups map[string]PoolView) (PoolView, bool) {
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	slices.Sort(names)
	var best PoolView
	found := false
	for _, name := range names {
		g := groups[name]
		if !found || pct(g.Blocking) > pct(best.Blocking) || pct(g.Blocking) == pct(best.Blocking) && pct(g.Projected) > pct(best.Projected) {
			best, found = g, true
		}
	}
	return best, found
}

// pct reads an optional percentage as a number; an absent one is the least
// restrictive value.
func pct(v *float64) float64 {
	if v == nil {
		return -1
	}
	return *v
}

// rankedPoolView ranks the buckets of rankScope — only those of one group when
// group is set — after checking that every bucket in the reading is valid and
// still running.
//
// Only windows of a day or more rank a pool or group; shorter windows gate admission.
func rankedPoolView(buckets []accountBucket, now time.Time, rankScope, group string) *PoolView {
	inView := func(b accountBucket) bool { return b.Scope == rankScope && (group == "" || b.Model == group) }
	var rankLen time.Duration
	for _, b := range buckets {
		if b.UsedPct == nil || !finite(*b.UsedPct) || *b.UsedPct < 0 || *b.UsedPct > 100 {
			return nil
		}
		if b.ResetsAt != "" {
			reset, err := time.Parse(time.RFC3339Nano, b.ResetsAt)
			if err != nil || !reset.After(now) {
				return nil
			}
		}
		if w, err := windowDuration(b.Window); err == nil && inView(b) && w > rankLen {
			rankLen = w
		}
	}
	if rankLen < 24*time.Hour {
		rankLen = 0
	}
	v := &PoolView{Status: "ok", Class: "spend", Group: group}
	for _, b := range buckets {
		if !inView(b) {
			continue
		}
		if v.Blocking == nil || *b.UsedPct > *v.Blocking {
			v.Blocking = b.UsedPct
		}
		w, err := windowDuration(b.Window)
		if err == nil {
			v.windowObserved = true
		}
		if err == nil && (v.Shortest == nil || w < v.Shortest.window) {
			room := windowRoom{usedPct: *b.UsedPct, window: w}
			if reset, perr := time.Parse(time.RFC3339Nano, b.ResetsAt); perr == nil {
				h := reset.Sub(now).Hours()
				room.resetHours = &h
			}
			v.Shortest = &room
		}
		long := err == nil && w >= 24*time.Hour
		if err == nil && !long && (v.ShortUsed == nil || *b.UsedPct > *v.ShortUsed) {
			v.ShortUsed = b.UsedPct
		}
		x := projectAtReset(b.UsedPct, b.Window, b.ResetsAt, now)
		if x == nil {
			continue
		}
		if v.Projected == nil || *x > *v.Projected {
			v.Projected = x
		}
		if long && (v.WorstLongProjected == nil || *x > *v.WorstLongProjected) {
			v.WorstLongProjected = x
		}
		if rankLen != 0 && err == nil && w == rankLen && (v.RankProjected == nil || *x > *v.RankProjected) {
			v.RankProjected = x
		}
		if rankLen == 0 || err != nil || w != rankLen {
			continue
		}
		reset, _ := time.Parse(time.RFC3339Nano, b.ResetsAt)
		h := reset.Sub(now).Hours()
		u := (100 - *x) / h
		if v.Urgency == nil || u > *v.Urgency {
			v.Urgency, v.ResetHours = &u, &h
		}
	}
	return v
}

// windowRoom is one window's used share and time to its reset. It is the view
// a job's expected hours must fit inside (headroomFit, select.go).
type windowRoom struct {
	usedPct    float64
	window     time.Duration
	resetHours *float64
}
