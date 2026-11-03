package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	args := append([]string{"place", "--class", "worker", "--cwd", cwd}, s.a.controlHubArgs(true)...)
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

// hkRead returns the reader for the handoffkeep settings — environment first,
// then ~/.config/handoffkeep/config.env — together with the parsed file.
func (a *App) hkRead() (func(string) string, map[string]string) {
	envFile := map[string]string{}
	path := os.Getenv("HANDOFFKEEP_CONFIG")
	if path == "" {
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			path = filepath.Join(xdg, "handoffkeep", "config.env")
		} else {
			path = expand("~/.config/handoffkeep/config.env")
		}
	}
	if data, e := os.ReadFile(path); e == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if ok {
				envFile[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"'")
			}
		}
	}
	return func(k string) string {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
		return envFile[k]
	}, envFile
}

// quotaURL is the configured shared-quota endpoint: hosts.toml [quota] url,
// else HANDOFFKEEP_URL.
func (a *App) quotaURL(read func(string) string) string {
	if u := str(a.Config.Quota, "url"); u != "" {
		return u
	}
	return read("HANDOFFKEEP_URL")
}

// quotaClient resolves the shared-quota read endpoint. The URL and bearer
// credential follow the same convention as pacelever: HANDOFFKEEP_URL /
// HANDOFFKEEP_TOKEN in the environment or ~/.config/handoffkeep/config.env,
// with hosts.toml [quota] url / credential_env as explicit overrides. Errors
// never echo URLs or credentials.
func (a *App) quotaClient() (*hkQuota, error) {
	q := a.Config.Quota
	read, envFile := a.hkRead()
	u := a.quotaURL(read)
	tokenEnv := str(q, "credential_env")
	if tokenEnv == "" {
		tokenEnv = "HANDOFFKEEP_TOKEN"
	}
	token := strings.TrimSpace(os.Getenv(tokenEnv))
	if token == "" {
		token = envFile[tokenEnv]
	}
	if u == "" || token == "" {
		return nil, fail(exitConfig, "shared quota cache unavailable")
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
	c := &hkQuota{base: strings.TrimRight(u, "/"), token: token, cfID: read("HANDOFFKEEP_CF_ACCESS_CLIENT_ID"), cfSecret: read("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET"), now: a.Now, freshTTL: a.placementTTL("fresh_ttl_s", 300)}
	if (c.cfID == "") != (c.cfSecret == "") {
		return nil, fail(exitConfig, "CF Access credential pair incomplete")
	}
	return c, nil
}

// hkQuota reads the shared quota cache and pacelever host presence from the
// handoffkeep HTTP API. Redirects are refused so authentication never follows
// another origin; response bodies and transport details stay out of errors.
type hkQuota struct {
	base, token, cfID, cfSecret string
	now                         func() time.Time
	freshTTL                    time.Duration
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
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	var rdr io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return fmt.Errorf("invalid backend request body")
		}
		rdr = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
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
		return fmt.Errorf("handoffkeep request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var er struct {
			Code string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&er)
		return &BackendError{Status: resp.StatusCode, Code: er.Code}
	}
	if out != nil {
		if e := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(out); e != nil {
			return fmt.Errorf("handoffkeep response is not valid JSON")
		}
	}
	return nil
}

// BackendError reports an HTTP failure without response details.
type BackendError struct {
	Status int
	Code   string
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
			Kind       string          `json:"kind"`
			MeasuredAt time.Time       `json:"measured_at"`
			Buckets    []accountBucket `json:"buckets"`
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
		acc.MeasuredAt = o.MeasuredAt
		if age := c.now().Sub(o.MeasuredAt); age >= -time.Minute && age <= c.freshTTL {
			acc.Fresh = true
		}
		acc.Quota = accountPoolView(o.Buckets, c.now())
	}
	return acc, nil
}

// accountBucket is the allowlisted shared-observation wire format. Model and
// group buckets are left to the exact profile's gate; account windows rank it.
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
	v := &PoolView{Status: "ok", Class: "spend"}
	var longest time.Duration
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
		if b.Scope != "account" {
			continue
		}
		if v.Blocking == nil || *b.UsedPct > *v.Blocking {
			v.Blocking = b.UsedPct
		}
		w, err := windowDuration(b.Window)
		long := err == nil && w >= 24*time.Hour
		if long && w > longest {
			longest, v.Urgency, v.ResetHours = w, nil, nil
		}
		if err == nil && !long && (v.ShortUsed == nil || *b.UsedPct > *v.ShortUsed) {
			v.ShortUsed = b.UsedPct
		}
		if x := projectAtReset(b.UsedPct, b.Window, b.ResetsAt, now); x != nil {
			if v.Projected == nil || *x > *v.Projected {
				v.Projected = x
			}
			if long && (v.LongProjected == nil || *x > *v.LongProjected) {
				v.LongProjected = x
			}
			if long && w == longest {
				reset, _ := time.Parse(time.RFC3339Nano, b.ResetsAt)
				h := reset.Sub(now).Hours()
				u := (100 - *x) / h
				if v.Urgency == nil || u > *v.Urgency {
					v.Urgency, v.ResetHours = &u, &h
				}
			}
		}
	}
	return v
}
