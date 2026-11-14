package fleet

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/handoffkeep/client"
	"github.com/mgh3326/panewire/internal/hubretry"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// Hub transport (hosts.<name>.via = "hub"): a remote spawn goes through the
// panewire hub instead of ssh. The brief is uploaded as a handoffkeep document
// the node fetches with its own credential, the checkout is a spec the node
// prepares (worktree.prepare), and the target account is refreshed with the
// quota.refresh node command. There is no ssh fallback: a hub that cannot be
// reached, or a node without spawn_v2, refuses the host (design D4).

const (
	hubMaxBriefBytes = 64 << 10
	hubDefaultWait   = 240 // must exceed the default brief submit window plus launch
	hubBusyRetry     = 5 * time.Second
)

// HubTransport is the panewire side of the hub's spawn and node-command
// endpoints. Tests replace it; production uses the HTTP client below.
type HubTransport interface {
	Spawn(ctx context.Context, req HubSpawnRequest) (HubSpawnReply, error)
	Refresh(ctx context.Context, machine, pool string) error
}

// HubSpawnRequest is the body of POST /v1/spawn.
type HubSpawnRequest struct {
	RequestID   string       `json:"request_id"`
	Machine     string       `json:"machine"`
	CWDKey      string       `json:"cwd_key"`
	Brief       HubBrief     `json:"brief"`
	Checkout    *HubCheckout `json:"checkout,omitempty"`
	Args        []string     `json:"args"`
	WaitSeconds int          `json:"wait_seconds"`
}

// HubBrief names the brief document by its handoffkeep key.
type HubBrief struct {
	Doc string `json:"doc"`
}

// HubCheckout is the worktree.prepare grammar the hub forwards to the node.
type HubCheckout struct {
	Origin      string `json:"origin"`
	Mode        string `json:"mode"`
	Branch      string `json:"branch,omitempty"`
	Sha         string `json:"sha,omitempty"`
	Fetch       string `json:"fetch,omitempty"`
	Dir         string `json:"dir"`
	VerifyReuse bool   `json:"verify_reuse,omitempty"`
}

// HubSpawnReply is a completed node answer (HTTP 200). Status is set instead
// when the hub only knows the request as lost.
type HubSpawnReply struct {
	RequestID  string `json:"request_id"`
	Machine    string `json:"machine"`
	RC         int    `json:"rc"`
	JobID      string `json:"job_id"`
	Pane       string `json:"pane"`
	StdoutTail string `json:"stdout_tail"`
	Error      string `json:"error"`
	Status     string `json:"status"`
	// NextHost is set by the hub's own transport when it handed the request back
	// to placement after a refusal that rules out only this host (a first-run
	// dialog, a missing login, an unmapped repository). The loop tries the next
	// candidate; the refusal is not the request's answer.
	NextHost bool `json:"next_host,omitempty"`
}

// Hub failure kinds. Every one of them refuses or stops the host; none falls
// back to ssh.
const (
	hubDown        = "hub_unavailable"      // unreachable, timed out, 5xx without a known code
	hubNoCapacity  = "hub_node_unavailable" // node not connected / not accepting
	hubNoSpawnV2   = "hub_spawn_capability_required"
	hubPending     = "hub_spawn_pending"   // 504: the node may still be running it
	hubRejected    = "hub_invalid_request" // 400, 409
	hubAuth        = "hub_auth"            // 401, 403
	hubRefreshFail = "hub_quota_refresh_failed"
)

// HubError is a hub call that did not produce a node answer.
type HubError struct {
	Kind   string
	Status int
	Code   string
	// transport is the connection failure behind a hubDown with no status, so
	// a retry can tell a dropped connection from a refused request.
	transport error
}

func (e *HubError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%s (HTTP %d)", e.Kind, e.Status)
	}
	return e.Kind
}

// Unwrap exposes the transport failure, if there was one.
func (e *HubError) Unwrap() error { return e.transport }

// hubHTTP is the production HubTransport. Credentials come from the files
// named in [hub] and never appear in errors; redirects are refused so the
// bearer never follows another origin.
type hubHTTP struct {
	base, token, cfID, cfSecret string
	client                      *http.Client
	statusPoll                  time.Duration // 0 means the WaitSpawn default
	// retry bounds doIdempotent; the zero value is the production policy.
	retry hubretry.Policy
	// onRetry hears each idempotent call that was sent again. Nil hears nothing.
	onRetry func(hubretry.Stats)
}

var hubPoolRule = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func (a *App) hub() (HubTransport, error) {
	if a.Hub != nil {
		return a.Hub, nil
	}
	c, e := newHubHTTP(a.Config.Hub)
	if e != nil {
		return nil, e
	}
	c.onRetry = a.noteHubRetry
	a.Hub = c
	return c, nil
}

// noteHubRetry adds one call's retries to the command's tally.
func (a *App) noteHubRetry(stats hubretry.Stats) { a.hubRetries.Add(stats) }

// reportHubRetries prints the one line a command owes when a hub call was sent
// again, on stderr so stdout keeps the command's receipt lines, then clears the
// tally.
func (a *App) reportHubRetries() {
	if a.hubRetries.Retries > 0 && a.Err != nil {
		fmt.Fprintln(a.Err, a.hubRetries.Line())
	}
	a.hubRetries = hubretry.Stats{}
}

func newHubHTTP(cfg map[string]any) (*hubHTTP, error) {
	raw := str(cfg, "hub_url")
	tokenFile := firstNonempty(str(cfg, "operator_token_env"), str(cfg, "hub_token_env"))
	if raw == "" || tokenFile == "" {
		return nil, fail(exitConfig, "via=hub needs [hub] hub_url and an operator token file")
	}
	u, e := neturl.Parse(raw)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fail(exitConfig, "invalid hub URL")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !isLoopbackHost(u.Hostname())) {
		return nil, fail(exitConfig, "hub URL requires HTTPS")
	}
	env, e := readEnvFile(expand(tokenFile))
	if e != nil || env["HUB_MACHINE_ID"] != "operator" || env["HUB_TOKEN"] == "" {
		return nil, fail(exitConfig, "hub operator credential file unusable")
	}
	c := &hubHTTP{base: strings.TrimRight(raw, "/"), token: env["HUB_TOKEN"]}
	if cf := str(cfg, "hub_cf_env"); cf != "" {
		cfEnv, e := readEnvFile(expand(cf))
		if e != nil || cfEnv["CF_ACCESS_CLIENT_ID"] == "" || cfEnv["CF_ACCESS_CLIENT_SECRET"] == "" {
			return nil, fail(exitConfig, "hub access credential file unusable")
		}
		c.cfID, c.cfSecret = cfEnv["CF_ACCESS_CLIENT_ID"], cfEnv["CF_ACCESS_CLIENT_SECRET"]
	}
	return c, nil
}

// readEnvFile parses KEY=VALUE lines of a private regular file. Anything
// readable by group or others is refused, as panewire does.
func readEnvFile(path string) (map[string]string, error) {
	info, e := os.Lstat(path)
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("env file must be a regular private file")
	}
	data, e := os.ReadFile(path)
	if e != nil || len(data) > 64<<10 {
		return nil, errors.New("env file unreadable")
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "export "))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, errors.New("malformed env file")
		}
		out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"'")
	}
	return out, nil
}

// do sends one JSON request and returns status and body. Transport errors,
// timeouts and an unreadable body all read as the hub being down.
func (c *hubHTTP) do(ctx context.Context, method, path string, body any, timeout time.Duration) (int, []byte, error) {
	client := c.client
	if client == nil {
		client = &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	var rdr io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return 0, nil, &HubError{Kind: hubRejected}
		}
		rdr = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if e != nil {
		return 0, nil, &HubError{Kind: hubRejected}
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
		return 0, nil, &HubError{Kind: hubDown, transport: e}
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if e != nil {
		return resp.StatusCode, nil, &HubError{Kind: hubDown, Status: resp.StatusCode}
	}
	return resp.StatusCode, data, nil
}

// doIdempotent is do for a call that may be sent again: a transport error, a
// 502 or a 503 is retried within c.retry's budget (docs/hub-uninterrupted-calls.md).
// Only a GET, or a POST that the hub keys by a request id, may use it. Each
// retried call is reported through c.onRetry.
func (c *hubHTTP) doIdempotent(ctx context.Context, method, path string, body any, timeout time.Duration) (int, []byte, error) {
	type answer struct {
		status int
		data   []byte
	}
	got, stats, err := hubretry.Do(ctx, c.retry, func(ctx context.Context) (answer, int, error) {
		status, data, err := c.do(ctx, method, path, body, timeout)
		return answer{status, data}, status, err
	}, nil)
	if stats.Retries > 0 && c.onRetry != nil {
		c.onRetry(stats)
	}
	return got.status, got.data, err
}

// classify maps a non-answer status to a HubError.
func classify(status int, data []byte) *HubError {
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(data, &er)
	switch {
	case status == http.StatusServiceUnavailable && er.Error == "spawn_capability_required":
		return &HubError{Kind: hubNoSpawnV2, Status: status, Code: er.Error}
	case status == http.StatusServiceUnavailable && er.Error == "node_unavailable":
		return &HubError{Kind: hubNoCapacity, Status: status, Code: er.Error}
	case status == http.StatusGatewayTimeout:
		// Every 504 for a spawn means the request may already be running on
		// the node, whatever the body says (the hub's own carries
		// {"status":"pending"}, a proxy's may carry any error). The caller
		// stops instead of trying another host. Refresh handles its own 504
		// before it gets here.
		return &HubError{Kind: hubPending, Status: status}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return &HubError{Kind: hubAuth, Status: status, Code: er.Error}
	case status == http.StatusBadRequest || status == http.StatusConflict:
		return &HubError{Kind: hubRejected, Status: status, Code: er.Error}
	}
	return &HubError{Kind: hubDown, Status: status, Code: er.Error}
}

func (c *hubHTTP) Spawn(ctx context.Context, req HubSpawnRequest) (HubSpawnReply, error) {
	status, data, e := c.do(ctx, http.MethodPost, "/v1/spawn", req, time.Duration(req.WaitSeconds+15)*time.Second)
	if e != nil {
		return HubSpawnReply{}, e
	}
	if status != http.StatusOK {
		return HubSpawnReply{}, classify(status, data)
	}
	var r HubSpawnReply
	if json.Unmarshal(data, &r) != nil {
		return HubSpawnReply{}, &HubError{Kind: hubDown, Status: status}
	}
	return r, nil
}

func (c *hubHTTP) Refresh(ctx context.Context, machine, pool string) error {
	body := map[string]any{"op": "quota.refresh", "args": map[string]string{"pool": pool}}
	status, data, e := c.do(ctx, http.MethodPost, "/v1/nodes/"+neturl.PathEscape(machine)+"/command", body, 45*time.Second)
	if e != nil {
		return e
	}
	if status == http.StatusOK {
		var r struct {
			Result struct {
				Outcome string `json:"outcome"`
			} `json:"result"`
		}
		if json.Unmarshal(data, &r) != nil || r.Result.Outcome != "refreshed" {
			return &HubError{Kind: hubRefreshFail, Status: status}
		}
		return nil
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(data, &er)
	switch {
	case status == http.StatusBadGateway, status == http.StatusGatewayTimeout, er.Error == "node_lost":
		return &HubError{Kind: hubRefreshFail, Status: status, Code: er.Error}
	}
	return classify(status, data)
}

// NewRequestID is a fresh version-4 request id, for a hub-originated
// submission such as a quota-stop replacement.
func NewRequestID() (string, error) { return newRequestID() }

func newRequestID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

// hubOrigin turns the local origin URL into the userinfo-free https form the
// node accepts. scp-style and ssh:// remotes lose their user and become
// https on the same host; an unrecognised shape is refused.
func hubOrigin(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	if strings.Contains(raw, "://") {
		u, e := neturl.Parse(raw)
		if e != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
			return "", false
		}
		switch u.Scheme {
		case "https":
			if u.User != nil {
				return "", false
			}
			return raw, true
		case "ssh", "git":
			if u.Port() != "" && u.Scheme == "git" {
				return "", false
			}
			return "https://" + u.Hostname() + "/" + strings.TrimPrefix(u.Path, "/"), true
		}
		return "", false
	}
	left, path, ok := strings.Cut(raw, ":")
	if !ok || path == "" || strings.HasPrefix(path, "/") {
		return "", false
	}
	host := left
	if at := strings.LastIndex(left, "@"); at >= 0 {
		host = left[at+1:]
	}
	if host == "" {
		return "", false
	}
	return "https://" + host + "/" + path, true
}

var (
	pullRef    = regexp.MustCompile(`^refs/(?:remotes/origin/)?pull/([0-9]+)/head$`)
	hubShaRule = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hubKeyRule = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)
)

// hubCheckout builds the worktree.prepare spec for a job whose local checkout
// is a linked worktree. A pushed branch is prepared in branch mode, which
// tracks origin's tip; a detached HEAD, or a branch whose commits sit on some
// other origin ref, in detached mode at the exact commit. Commits that are on
// no origin ref, or a missing origin URL, refuse: the node would run code
// without them. Only refs already present locally are read; nothing is fetched
// here.
func (a *App) hubCheckout(ctx context.Context, repo *repoResolution) (*HubCheckout, string) {
	if a.Repository != nil && a.Repository.Checkout != nil {
		copy := *a.Repository.Checkout
		return &copy, ""
	}

	origin, ok := hubOrigin(repo.url)
	if !ok {
		return nil, fmt.Sprintf("origin of %s has no https form the node can clone", repo.name)
	}
	if !hubShaRule.MatchString(repo.head) {
		return nil, fmt.Sprintf("worktree %s has no readable HEAD", repo.wt)
	}
	dir := repo.localRoot + "/" + repo.wt
	if repo.sourceDir != "" {
		dir = repo.sourceDir
	}
	spec := &HubCheckout{Origin: origin, Dir: repo.wt}
	if repo.branch != "" {
		r := a.call(ctx, "git", "-C", dir, "merge-base", "--is-ancestor", repo.head, "refs/remotes/origin/"+repo.branch)
		if r.Code == 0 {
			spec.Mode, spec.Branch = "branch", repo.branch
			return spec, ""
		}
	}
	tips := a.refsAt(ctx, dir, "--points-at", repo.head, "refs/remotes/origin", "refs/pull")
	for _, ref := range tips {
		if m := pullRef.FindStringSubmatch(ref); m != nil {
			spec.Mode, spec.Sha, spec.Fetch = "detached", repo.head, "pull/"+m[1]+"/head"
			return spec, ""
		}
	}
	for _, name := range append(tips, a.refsAt(ctx, dir, "--contains", repo.head, "refs/remotes/origin")...) {
		if b, ok := strings.CutPrefix(name, "refs/remotes/origin/"); ok && b != "HEAD" && !strings.HasPrefix(b, "pull/") {
			spec.Mode, spec.Sha, spec.Fetch = "detached", repo.head, "refs/heads/"+b
			return spec, ""
		}
	}
	return nil, fmt.Sprintf("local commit %s of %s is on no origin ref; push the branch first", short(repo.head), firstNonempty(repo.branch, "the detached HEAD"))
}

// refsAt lists ref names for one for-each-ref selector, sorted by git.
func (a *App) refsAt(ctx context.Context, dir, selector, sha string, patterns ...string) []string {
	args := append([]string{"-C", dir, "for-each-ref", "--format=%(refname)", selector, sha}, patterns...)
	r := a.call(ctx, "git", args...)
	if r.Code != 0 {
		return nil
	}
	return strings.Fields(r.Out)
}

// hubArgs renders the panewire flags the hub's allowlist accepts. A field the
// allowlist cannot carry is refused rather than dropped, so a hub spawn never
// runs with weaker settings than the same spawn over ssh.
func hubArgs(jobs string, s Spawn) ([]string, error) {
	args, err := hubSpawnArgs(jobs, s)
	if err != nil {
		return nil, err
	}
	// The node applies its own argument rule to every job.spawn and drops the
	// whole message on a mismatch. Building the same args through that rule
	// here turns a silent node drop into a submit-time refusal naming the flag.
	if reason := spawnargs.Refusal(args, true); reason != "" {
		return nil, errors.New(reason)
	}
	return args, nil
}

// hubSpawnArgs renders the flags without applying the node's argument rule,
// which the callers that need a reason rather than an error apply themselves.
func hubSpawnArgs(jobs string, s Spawn) ([]string, error) {
	var args []string
	add := func(flag, v string) {
		if v != "" {
			args = append(args, flag, v)
		}
	}
	for _, u := range []struct {
		flag string
		set  bool
	}{{"--keep", s.Keep}, {"--job-dup-ok", s.DupOK}, {"--task-hk-bypass", s.Bypass}, {"--allow-escalation", s.AllowEscalation}} {
		if u.set {
			args = append(args, u.flag)
		}
	}
	for _, u := range []struct{ flag, v string }{{"--operator-request", s.OperatorRequest}, {"--requested-by", s.RequestedBy}, {"--purpose", s.Purpose}} {
		add(u.flag, u.v)
	}
	add("-m", s.Profile)
	add("-w", s.Workspace)
	add("-l", s.Label)
	add("--t", s.T)
	add("--job", s.Job)
	add("--owner", s.Owner)
	add("--effort", s.Effort)
	add("--e6-arm", s.E6Arm)
	add("--role", s.Role)
	add("--lane", s.Lane)
	add("--parent", s.Parent)
	// A parent job is resolved on the node from the hk job record, which
	// also supplies the task, so --task is not sent beside it.
	if s.ParentJob != "" {
		add("--parent-job", s.ParentJob)
	} else {
		add("--task", s.Task)
	}
	report, e := hubReportRelative(jobs, s)
	if e != nil {
		return nil, e
	}
	add("--report-relative", report)
	add("-L", s.ExecutionLane)
	add("--urgency", s.Urgency)
	add("--author-family", s.AuthorFamily)
	add("--builder-job", s.BuilderJob)
	add("--grade", s.Grade)
	add("--review-pass", s.ReviewPass)
	add("--override-reason", s.OverrideReason)
	add("--containment", s.Containment)
	return args, nil
}

// hubSpawnArgRefusal is the node's argument rule applied to the args the hub
// will send for s. A dry-run refuses here, before admission can answer for
// another reason; "" when the hub args cannot be built at all, whose error
// still surfaces where those args are built.
func hubSpawnArgRefusal(jobs string, s Spawn) string {
	args, err := hubSpawnArgs(jobs, s)
	if err != nil {
		return ""
	}
	return spawnargs.Refusal(args, true)
}

// hubReportValue is the shape the hub accepts for --report-relative: the same
// rule as panewire's validHubSpawnDocKey (a narrower charset than other hub
// argument values: no ':' or '@'), checked here so a path the hub would
// refuse never reaches it as an ambiguous 400.
var hubReportValue = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,200}$`)

func hubReportRelativeOK(rel string) bool {
	if !hubReportValue.MatchString(rel) || strings.HasPrefix(rel, "/") || strings.HasSuffix(rel, "/") {
		return false
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// hubReportRelative names the report for the node. The caller's absolute
// report path means nothing on the node, whose jobs root is under its own
// home, so a report is sent relative to the jobs root and the node resolves
// it there. A report outside the jobs root is refused, as the local spawn
// does; --report-relative given by the caller takes the same path.
func hubReportRelative(jobs string, s Spawn) (string, error) {
	rel := s.ReportRelative
	switch {
	case rel != "":
		if filepath.IsAbs(rel) || strings.Contains(rel, "..") {
			return "", fmt.Errorf("unsafe report-relative")
		}
	case s.Report != "":
		if !within(jobs, s.Report) {
			return "", fmt.Errorf("remote report outside jobs root")
		}
		r, e := filepath.Rel(jobs, s.Report)
		if e != nil {
			return "", fmt.Errorf("remote report outside jobs root")
		}
		rel = filepath.ToSlash(r)
	default:
		return "", nil
	}
	if !hubReportRelativeOK(rel) {
		return "", fmt.Errorf("report path %q cannot cross the hub (allowed: letters, digits and ._/-, no empty, . or .. segments, at most 200 characters)", rel)
	}
	return rel, nil
}

// putBrief uploads the brief as an hk document under the node's prefix and
// returns its key. The key is per job, so a retry overwrites its own upload.
// The document goes over hk HTTP with the svc-panewire credential: kind brief,
// the job id and the brief's bytes, as `handoffkeep doc put` sent them, under
// the upload bound (hkPutDocument), not a task read's.
func (a *App) putBrief(ctx context.Context, s Spawn, prefix, body string) (string, error) {
	key := prefix + s.Job + ".md"
	if e := hkPutDocument(ctx, client.Document{Key: key, Kind: "brief", Job: s.Job, Body: body}); e != nil {
		return "", fail(exitPlacement, "brief upload to %s failed: %s", key, hkErrText(e))
	}
	return key, nil
}

// spawnHub is spawnRemote for a via=hub host. It returns the same candResult
// vocabulary as the ssh leg, with hub failures recorded as the note.
func (a *App) spawnHub(ctx context.Context, s Spawn, brief string, c *placeCandidate, repo *repoResolution, forced bool) candResult {
	h := c.cfg
	refuse := func(note string, e error) candResult {
		if forced {
			return candResult{stop: e}
		}
		return candResult{rc: exitCode(e), note: note}
	}
	if !regexp.MustCompile(`^[a-z0-9-]{1,32}$`).MatchString(c.name) {
		return candResult{stop: fail(exitConfig, "host %s is not a valid hub machine id", c.name)}
	}
	if repo == nil {
		return candResult{note: "repo_unmappable"}
	}
	if repo.rel != "" {
		return refuse("hub_subdirectory_unsupported", fail(exitPlacement, "hub spawn cannot start in subdirectory %s of the repo", repo.rel))
	}
	key := RepoKey(repo.name)
	if !hubKeyRule.MatchString(key) {
		return refuse("hub_cwd_key_invalid", fail(exitConfig, "repo name %q is not a valid hub cwd key", key))
	}
	args, e := hubArgs(a.Jobs, s)
	if e != nil {
		return candResult{stop: fail(exitUsage, "hub spawn: %v", e)}
	}
	if s.DryRun {
		payload, e := a.prepareBriefForHost(s, c.name, brief)
		if e != nil {
			return candResult{stop: e}
		}
		a.dryRunLine(c.name, s, payload)
		return candResult{ok: true}
	}
	if !utf8.ValidString(brief) || len([]byte(brief)) > hubMaxBriefBytes {
		return candResult{stop: fail(exitUsage, "brief for hub spawn must be valid UTF-8 of at most %d bytes", hubMaxBriefBytes)}
	}
	hub, e := a.hub()
	if e != nil {
		return refuse("hub_unconfigured", e)
	}
	var checkout *HubCheckout
	if repo.wt != "" {
		var why string
		if checkout, why = a.hubCheckout(ctx, repo); checkout == nil {
			// A job from a linked worktree must run that checkout: fail
			// closed rather than try another host.
			return candResult{stop: fail(exitPlacement, "cannot prepare a worktree on %s: %s", c.name, why)}
		}
	}
	prefix := firstNonempty(str(h, "brief_doc_prefix"), "briefs/")
	doc, e := a.putBrief(ctx, s, prefix, brief)
	if e != nil {
		return candResult{stop: e}
	}
	wait := int(integer(h, "hub_wait", hubDefaultWait))
	for attempt := 0; ; attempt++ {
		id, e := newRequestID()
		if e != nil {
			return candResult{stop: e}
		}
		if s.placement != nil {
			s.placement.contactedHost = c.name
		}
		reply, err := hub.Spawn(ctx, HubSpawnRequest{RequestID: id, Machine: c.name, CWDKey: key, Brief: HubBrief{Doc: doc}, Checkout: checkout, Args: args, WaitSeconds: wait})
		s.placement.remoteReceipt(c.name, reply.StdoutTail)
		if err == nil && reply.RC == 0 && reply.Error == "" && reply.Status == "" && reply.Pane != "" && s.placement != nil && !s.placement.placed {
			s.placement.started = true
			s.placement.placed = true
			s.placement.execution = placementExecution{Host: c.name}
		}
		if err != nil {
			if errors.Is(err, ErrHubPlacementBound) {
				return candResult{stop: err}
			}
			var he *HubError
			if slices.Contains(args, "--report-relative") && errors.As(err, &he) && he.Kind == hubRejected && he.Status == http.StatusBadRequest && he.Code == "invalid_request" {
				// The hub validates the whole request, so one invalid_request
				// hides the cause. --report-relative is the one flag older hubs
				// refuse; name it as a possibility, keep the code, and do not
				// retry with an absolute --report the node would refuse anyway.
				return candResult{stop: fail(exitConfig, "hub rejected the spawn request (400 %s); if the hub predates the --report-relative capability, upgrade panewire on the hub, otherwise check the request fields", he.Code)}
			}
			return a.hubFailure(c.name, id, err, forced)
		}
		if reply.Status == "lost" || reply.Status == "pending" {
			return candResult{stop: fail(exitStateConflict, "hub spawn on %s is %s (request %s); the node may have started it", c.name, reply.Status, id)}
		}
		if reply.Error == "checkout_refused:busy" && attempt == 0 {
			if e := a.Sleep(ctx, hubBusyRetry); e != nil {
				return candResult{stop: e}
			}
			continue
		}
		return a.hubReply(c.name, checkout, reply, forced)
	}
}

// hubFailure turns a hub call that gave no node answer into a refusal. A hub
// that cannot be reached, a node that is not connected and a node without
// spawn_v2 refuse this host only; a spawn that may still be running stops.
func (a *App) hubFailure(host, id string, err error, forced bool) candResult {
	var he *HubError
	if !errors.As(err, &he) {
		he = &HubError{Kind: hubDown}
	}
	switch he.Kind {
	case hubPending:
		return candResult{stop: fail(exitStateConflict, "hub spawn on %s did not answer in time (request %s); the node may still be running it", host, id)}
	case hubNoCapacity:
		// Not connected is how a sleeping host looks: let placement wake it.
		return candResult{unreachable: true, rc: exitPlacement, note: he.Kind}
	case hubDown:
		if forced {
			return candResult{stop: fail(exitPlacement, "hub unavailable; remote placement on %s refused (no ssh fallback)", host)}
		}
	case hubAuth:
		return candResult{rc: exitConfig, note: he.Kind}
	case hubRejected:
		return candResult{rc: exitConfig, note: he.Kind}
	}
	return candResult{rc: exitPlacement, note: he.Kind}
}

// hubReply maps a node answer to a candidate result, mirroring the ssh leg.
func (a *App) hubReply(host string, checkout *HubCheckout, r HubSpawnReply, forced bool) candResult {
	if r.NextHost {
		return candResult{rc: exitConfig, note: "hub_" + strings.TrimPrefix(r.Error, "spawn_failed:")}
	}
	if outcome, ok := strings.CutPrefix(r.Error, "checkout_refused:"); ok {
		dir := ""
		if checkout != nil {
			dir = checkout.Dir
		}
		return candResult{stop: fail(exitPlacement, "cannot prepare worktree %s on %s: checkout refused (%s)", dir, host, outcome)}
	}
	switch r.Error {
	case "spawn_disabled", "cwd_unmapped", "panewire_missing", "brief_doc_unavailable":
		return candResult{rc: exitConfig, note: "hub_" + r.Error}
	case "timeout":
		return candResult{stop: fail(exitStateConflict, "hub spawn on %s timed out on the node; it may still be starting", host)}
	}
	if r.RC == 0 {
		line := ""
		for _, l := range strings.Split(r.StdoutTail, "\n") {
			if strings.HasPrefix(l, "OK pane=") {
				line = strings.TrimSpace(l)
			}
		}
		if line == "" && r.Pane != "" {
			line = "OK pane=" + r.Pane
			if r.JobID != "" {
				line += " job=" + r.JobID
			}
		}
		if line == "" {
			return candResult{stop: fail(exitStateConflict, "remote spawn returned no receipt")}
		}
		fmt.Fprint(a.Out, line+" host="+host+"\n")
		return candResult{ok: true}
	}
	if !forced && r.RC == exitConfig && strings.Contains(r.Error+r.StdoutTail, "WORKSPACE_UNCONFIGURED:") {
		return candResult{rc: r.RC, note: "workspace_unconfigured"}
	}
	if forced || slices.Contains([]int{2, 70, 74, 75, 76, 79, 82, exitTrustPrompt}, r.RC) {
		return candResult{stop: fail(r.RC, "remote spawn: %s", strings.TrimSpace(r.StdoutTail))}
	}
	return candResult{rc: r.RC, note: "refused rc=" + strconv.Itoa(r.RC)}
}

// refreshViaHub is measureTarget for a via=hub host: the on-demand refresh
// runs on the node through the quota.refresh node command.
func (a *App) refreshViaHub(ctx context.Context, host, pool string) error {
	if !hubPoolRule.MatchString(pool) || !regexp.MustCompile(`^[a-z0-9-]{1,32}$`).MatchString(host) {
		return fail(exitConfig, "target %s pool %s cannot be measured", host, pool)
	}
	hub, e := a.hub()
	if e != nil {
		return e
	}
	timeout := time.Duration(integer(a.Config.Quota, "measure_timeout_s", 60)) * time.Second
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := hub.Refresh(ctx, host, pool)
	if ctx.Err() != nil {
		return fail(exitTimeout, "measurement of pool %s on host %s timed out", pool, host)
	}
	if err == nil {
		return nil
	}
	var he *HubError
	if errors.As(err, &he) && (he.Kind == hubDown || he.Kind == hubNoCapacity) {
		return fail(exitPlacement, "hub unavailable; measurement of pool %s on host %s refused (no ssh fallback)", pool, host)
	}
	return fail(exitEvictionUnconfirmed, "measurement of pool %s on host %s failed: %v", pool, host, err)
}
