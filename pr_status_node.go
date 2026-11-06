package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The pr.status node op (docs/node-commands.md#prstatus) is the read-only
// transport for `panewire pr status` and `panewire pr findings`. It runs on
// the machine that hosts mergewake and answers two sources side by side:
//
//   - mergewake's last gate decision, read with `mergewake pr-status --json`
//     (zero GitHub calls unless the caller asked for fresh), and
//   - handoffkeep's change record, `GET /v1/changes?pr=<PR URL>`, read with
//     this node's own spawn.json handoffkeep_env.
//
// Neither source needs a credential from the caller, and neither failure fails
// the op: each source carries its own closed outcome so the CLI can say which
// one was missing and fall back to gh only when both are.
const (
	nodeCommandPRStatus = "pr.status"

	// Source outcomes. They are the only words a caller reads from a source.
	prStatusOK           = "ok"
	prStatusNotFound     = "not_found"
	prStatusFailed       = "failed"
	prStatusUnavailable  = "unavailable"
	prStatusUnconfigured = "unconfigured"
	prStatusTooLarge     = "too_large"

	// Closed reasons for a mergewake source that is not ok.
	prStatusReasonNotInstalled = "not_installed"
	prStatusReasonTimeout      = "timeout"
	prStatusReasonExit         = "exit_status"
	prStatusReasonUnreadable   = "unreadable_output"
	prStatusReasonOutputLarge  = "output_too_large"
	// Closed reasons for an hk source that is not ok.
	prStatusReasonHTTPStatus = "http_status"
	prStatusReasonRequest    = "request_failed"
	prStatusReasonBadBody    = "unreadable_body"

	prStatusHKTimeout = 5 * time.Second

	// prStatusMaxMergewakeOutput is mergewake's own response bound; a longer
	// stdout is refused rather than truncated into invalid JSON.
	prStatusMaxMergewakeOutput = 64 << 10
	prStatusMaxStderr          = 2 << 10
	prStatusMaxHKChange        = 4 << 10
	prStatusMaxFresh           = 2 << 10
	// prStatusResultBudget leaves the reply envelope room under
	// nodeCommandMaxResultBytes; findings are dropped from the tail to fit.
	prStatusResultBudget = nodeCommandMaxResultBytes - 2<<10

	prStatusMaxReasons  = 20
	prStatusMaxChecks   = 50
	prStatusMaxPasses   = 10
	prStatusMaxFindings = 50
	prStatusMaxShort    = 100
	prStatusMaxID       = 128
	prStatusMaxLocation = 200
	prStatusMaxSummary  = 240
	prStatusMaxURL      = 200
	prStatusMaxPR       = 1<<31 - 1

	// defaultPRStatusHost names the machine that hosts mergewake unless the
	// hub's --pr-status-host says otherwise.
	defaultPRStatusHost = "oci"
)

// prStatusMergewakeTimeout and prStatusHKTimeout run one after the other
// inside nodeCommandRunTimeout. The mergewake bound is a variable so a test can
// prove the timeout without waiting twelve seconds.
var prStatusMergewakeTimeout = 12 * time.Second

// nodeMergewakeExecutable is the mergewake binary pr.status runs; tests point
// it at a fake so they never reach a real mergewake on the host.
var nodeMergewakeExecutable = "mergewake"

// nodeMergewakeEnvAllow is every variable the mergewake child inherits. It is
// a closed list: the node's own environment may hold credentials that mergewake
// does not need to read a snapshot, and --fresh resolves its own token from
// mergewake's config on this host.
var nodeMergewakeEnvAllow = []string{"PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TMPDIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME", "XDG_DATA_HOME"}

// prStatusRepoPattern is owner/name as GitHub spells it; the segments may not
// be "." or "..", which the node checks separately.
var prStatusRepoPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}/[A-Za-z0-9._][A-Za-z0-9._-]{0,99}$`)

func validPRStatusRepo(repo string) bool {
	if !prStatusRepoPattern.MatchString(repo) {
		return false
	}
	_, name, _ := strings.Cut(repo, "/")
	return name != "." && name != ".." && !strings.HasSuffix(name, ".git")
}

type nodePRStatusArgs struct {
	Repo  string `json:"repo"`
	PR    int    `json:"pr"`
	Fresh bool   `json:"fresh"`
}

func validNodePRStatusArgs(args nodePRStatusArgs) bool {
	return validPRStatusRepo(args.Repo) && args.PR >= 1 && args.PR <= prStatusMaxPR
}

// PRStatusSnapshot is mergewake's per-PR gate snapshot, re-encoded by the node
// from mergewake's own JSON (docs/pr-status.md in the mergewake repository).
// mergewake's output is read leniently, so a field it adds later is dropped
// instead of failing the op; panewire's own wire shape below is strict.
type PRStatusSnapshot struct {
	Repo       string            `json:"repo"`
	PR         int               `json:"pr"`
	Head       string            `json:"head"`
	GateHead   string            `json:"gate_head"`
	State      string            `json:"state"`
	Reasons    []string          `json:"reasons"`
	Checks     []PRStatusCheck   `json:"checks"`
	Review     PRStatusReview    `json:"review"`
	Threads    PRStatusThreads   `json:"threads"`
	Findings   []PRStatusFinding `json:"findings"`
	BaseFresh  *bool             `json:"base_fresh"`
	BaseBehind bool              `json:"base_behind"`
	ObservedAt string            `json:"observed_at"`
	Fresh      json.RawMessage   `json:"fresh"`
	FreshAt    *string           `json:"fresh_at"`
	FreshError string            `json:"fresh_error,omitempty"`
	Source     string            `json:"source"`
	// FindingsOmitted counts findings the node dropped to fit the reply cap;
	// it is panewire's field, never mergewake's.
	FindingsOmitted int `json:"findings_omitted,omitempty"`
}

type PRStatusCheck struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type PRStatusPass struct {
	Pass   int    `json:"pass"`
	Lens   string `json:"lens,omitempty"`
	Family string `json:"family,omitempty"`
}

type PRStatusReview struct {
	Passes    []PRStatusPass `json:"passes"`
	Done      bool           `json:"done"`
	Inherited string         `json:"inherited_from,omitempty"`
}

type PRStatusThreads struct {
	Blocking   int  `json:"blocking"`
	Other      int  `json:"other"`
	Unresolved *int `json:"unresolved,omitempty"`
}

type PRStatusFinding struct {
	ID            string `json:"id"`
	Location      string `json:"location"`
	Severity      string `json:"severity,omitempty"`
	Blocking      bool   `json:"blocking"`
	Summary       string `json:"summary"`
	URL           string `json:"url,omitempty"`
	AuthorReplied bool   `json:"author_replied"`
}

// nodePRStatusMergewake is the mergewake half of the answer. Snapshot is set
// only when Outcome is ok; Reason names why it is not otherwise.
type nodePRStatusMergewake struct {
	Outcome  string            `json:"outcome"`
	Reason   string            `json:"reason,omitempty"`
	Snapshot *PRStatusSnapshot `json:"snapshot,omitempty"`
}

// nodePRStatusHK is the handoffkeep half. Change is hk's own JSON object,
// passed through unread because its shape is hk's contract, not panewire's.
type nodePRStatusHK struct {
	Outcome string          `json:"outcome"`
	Reason  string          `json:"reason,omitempty"`
	Status  int             `json:"status,omitempty"`
	Change  json.RawMessage `json:"change,omitempty"`
}

type nodePRStatusResult struct {
	Mergewake nodePRStatusMergewake `json:"mergewake"`
	HK        nodePRStatusHK        `json:"hk"`
}

func boundedStrings(values []string, limit, width int) []string {
	out := []string{}
	for _, value := range values {
		if len(out) == limit {
			break
		}
		out = append(out, utf8PrefixWithin(value, width))
	}
	return out
}

// boundPRStatusSnapshot applies every wire bound and replaces absent lists
// with empty ones, so the encoded snapshot is the same shape whatever
// mergewake omitted.
func boundPRStatusSnapshot(snapshot PRStatusSnapshot) PRStatusSnapshot {
	snapshot.Repo = utf8PrefixWithin(snapshot.Repo, prStatusMaxLocation)
	snapshot.Head = utf8PrefixWithin(snapshot.Head, prStatusMaxShort)
	snapshot.GateHead = utf8PrefixWithin(snapshot.GateHead, prStatusMaxShort)
	snapshot.State = utf8PrefixWithin(snapshot.State, prStatusMaxShort)
	snapshot.Source = utf8PrefixWithin(snapshot.Source, prStatusMaxShort)
	snapshot.ObservedAt = utf8PrefixWithin(snapshot.ObservedAt, prStatusMaxShort)
	snapshot.FreshError = utf8PrefixWithin(snapshot.FreshError, prStatusMaxSummary)
	snapshot.Reasons = boundedStrings(snapshot.Reasons, prStatusMaxReasons, prStatusMaxShort)
	checks := []PRStatusCheck{}
	for _, check := range snapshot.Checks {
		if len(checks) == prStatusMaxChecks {
			break
		}
		checks = append(checks, PRStatusCheck{Name: utf8PrefixWithin(check.Name, prStatusMaxShort), State: utf8PrefixWithin(check.State, prStatusMaxShort)})
	}
	snapshot.Checks = checks
	passes := []PRStatusPass{}
	for _, pass := range snapshot.Review.Passes {
		if len(passes) == prStatusMaxPasses {
			break
		}
		passes = append(passes, PRStatusPass{Pass: pass.Pass, Lens: utf8PrefixWithin(pass.Lens, prStatusMaxShort), Family: utf8PrefixWithin(pass.Family, prStatusMaxShort)})
	}
	snapshot.Review.Passes = passes
	snapshot.Review.Inherited = utf8PrefixWithin(snapshot.Review.Inherited, prStatusMaxShort)
	findings := []PRStatusFinding{}
	for _, finding := range snapshot.Findings {
		if len(findings) == prStatusMaxFindings {
			break
		}
		findings = append(findings, PRStatusFinding{
			ID:            utf8PrefixWithin(finding.ID, prStatusMaxID),
			Location:      utf8PrefixWithin(finding.Location, prStatusMaxLocation),
			Severity:      utf8PrefixWithin(finding.Severity, prStatusMaxShort),
			Blocking:      finding.Blocking,
			Summary:       utf8PrefixWithin(finding.Summary, prStatusMaxSummary),
			URL:           utf8PrefixWithin(finding.URL, prStatusMaxURL),
			AuthorReplied: finding.AuthorReplied,
		})
	}
	snapshot.Findings = findings
	if snapshot.FreshAt != nil {
		bounded := utf8PrefixWithin(*snapshot.FreshAt, prStatusMaxShort)
		snapshot.FreshAt = &bounded
	}
	if len(snapshot.Fresh) == 0 || len(snapshot.Fresh) > prStatusMaxFresh || !jsonObjectOrNull(snapshot.Fresh) {
		snapshot.Fresh = json.RawMessage("null")
	}
	return snapshot
}

func jsonObjectOrNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return true
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(trimmed, &object) == nil && object != nil
}

// fitPRStatusResult drops findings from the tail until the encoded result fits
// the reply budget, and records how many it dropped. Findings are the only
// unbounded-in-practice part: every other list is capped by a small constant.
func fitPRStatusResult(result nodePRStatusResult) nodePRStatusResult {
	snapshot := result.Mergewake.Snapshot
	if snapshot == nil {
		return result
	}
	for {
		encoded, err := marshalNodeCommandJSON(result)
		if err != nil || len(encoded) <= prStatusResultBudget || len(snapshot.Findings) == 0 {
			return result
		}
		snapshot.Findings = snapshot.Findings[:len(snapshot.Findings)-1]
		snapshot.FindingsOmitted++
	}
}

func validPRStatusSnapshot(snapshot PRStatusSnapshot) bool {
	if !validPRStatusRepo(snapshot.Repo) || snapshot.PR < 1 || snapshot.PR > prStatusMaxPR || snapshot.State == "" || snapshot.Reasons == nil || snapshot.Checks == nil || snapshot.Review.Passes == nil || snapshot.Findings == nil || snapshot.FindingsOmitted < 0 {
		return false
	}
	if _, err := time.Parse(time.RFC3339, snapshot.ObservedAt); err != nil {
		return false
	}
	return len(snapshot.Reasons) <= prStatusMaxReasons && len(snapshot.Checks) <= prStatusMaxChecks && len(snapshot.Review.Passes) <= prStatusMaxPasses && len(snapshot.Findings) <= prStatusMaxFindings &&
		len(snapshot.Fresh) > 0 && len(snapshot.Fresh) <= prStatusMaxFresh && jsonObjectOrNull(snapshot.Fresh)
}

// validNodePRStatusResult is the hub and node's shared check of a result: each
// source's outcome selects exactly which of its other fields may be set.
func validNodePRStatusResult(result nodePRStatusResult) bool {
	merge := result.Mergewake
	switch merge.Outcome {
	case prStatusOK:
		if merge.Snapshot == nil || merge.Reason != "" || !validPRStatusSnapshot(*merge.Snapshot) {
			return false
		}
	case prStatusNotFound:
		if merge.Snapshot != nil || merge.Reason != "" {
			return false
		}
	case prStatusFailed, prStatusUnavailable:
		if merge.Snapshot != nil {
			return false
		}
		switch merge.Reason {
		case prStatusReasonNotInstalled, prStatusReasonTimeout, prStatusReasonExit, prStatusReasonUnreadable, prStatusReasonOutputLarge:
		default:
			return false
		}
	default:
		return false
	}
	hk := result.HK
	switch hk.Outcome {
	case prStatusOK:
		trimmed := bytes.TrimSpace(hk.Change)
		var object map[string]json.RawMessage
		return hk.Reason == "" && hk.Status == http.StatusOK && len(trimmed) > 0 && len(trimmed) <= prStatusMaxHKChange && json.Unmarshal(trimmed, &object) == nil && object != nil
	case prStatusNotFound:
		return hk.Reason == "" && (hk.Status == http.StatusNotFound || hk.Status == http.StatusOK) && hk.Change == nil
	case prStatusUnconfigured, prStatusTooLarge:
		return hk.Reason == "" && hk.Status == 0 && hk.Change == nil
	case prStatusFailed:
		return hk.Change == nil && (hk.Reason == prStatusReasonHTTPStatus || hk.Reason == prStatusReasonRequest || hk.Reason == prStatusReasonBadBody)
	}
	return false
}

// prStatusLimitedBuffer keeps at most max bytes and remembers it dropped more,
// so a child that writes without end costs a bounded amount of memory.
type prStatusLimitedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (b *prStatusLimitedBuffer) Write(p []byte) (int, error) {
	kept := p
	if room := max(b.max-b.buf.Len(), 0); room < len(p) {
		b.overflow = true
		kept = p[:room]
	}
	b.buf.Write(kept)
	// Report every byte as written: a short count would fail the child's copy
	// and read as an error instead of as an oversize.
	return len(p), nil
}

func nodeMergewakeEnvironment() []string {
	var env []string
	for _, name := range nodeMergewakeEnvAllow {
		if value, found := os.LookupEnv(name); found {
			env = append(env, name+"="+value)
		}
	}
	return env
}

// nodePRStatusMergewake reads one snapshot. It never returns the child's
// stderr: stderr is only searched for mergewake's "no status snapshot" answer.
func readNodePRStatusMergewake(ctx context.Context, args nodePRStatusArgs) nodePRStatusMergewake {
	runContext, cancel := context.WithTimeout(ctx, prStatusMergewakeTimeout)
	defer cancel()
	argv := []string{"pr-status", "--repo", args.Repo, "--pr", strconv.Itoa(args.PR), "--json"}
	if args.Fresh {
		argv = append(argv, "--fresh")
	}
	cmd := exec.CommandContext(runContext, nodeMergewakeExecutable, argv...)
	cmd.Env = nodeMergewakeEnvironment()
	cmd.WaitDelay = 100 * time.Millisecond
	cmd.Stdin = nil
	stdout := &prStatusLimitedBuffer{max: prStatusMaxMergewakeOutput}
	stderr := &prStatusLimitedBuffer{max: prStatusMaxStderr}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	switch {
	case errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist):
		return nodePRStatusMergewake{Outcome: prStatusUnavailable, Reason: prStatusReasonNotInstalled}
	case runContext.Err() != nil:
		return nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonTimeout}
	case err != nil:
		if bytes.Contains(stderr.buf.Bytes(), []byte("no status snapshot")) {
			return nodePRStatusMergewake{Outcome: prStatusNotFound}
		}
		return nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonExit}
	case stdout.overflow:
		return nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonOutputLarge}
	}
	var snapshot PRStatusSnapshot
	if json.Unmarshal(stdout.buf.Bytes(), &snapshot) != nil || !strings.EqualFold(snapshot.Repo, args.Repo) || snapshot.PR != args.PR {
		return nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonUnreadable}
	}
	snapshot.Repo = args.Repo
	snapshot = boundPRStatusSnapshot(snapshot)
	if !validPRStatusSnapshot(snapshot) {
		return nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonUnreadable}
	}
	return nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot}
}

// prStatusHKTransport is the hk read's transport; nil is the default. Tests
// point it at a fixture server.
var prStatusHKTransport http.RoundTripper

// readNodePRStatusHK asks handoffkeep for the PR's change record with this
// node's own credential from spawn.json handoffkeep_env. A 404 is expected
// while a handoffkeep that predates /v1/changes is deployed, and is its own
// outcome so the CLI can say so.
func (client *HubClient) readNodePRStatusHK(ctx context.Context, args nodePRStatusArgs) nodePRStatusHK {
	config, err := loadHubSpawnConfig(client.spawnConfigPath)
	if err != nil || config.HandoffkeepEnv == "" {
		return nodePRStatusHK{Outcome: prStatusUnconfigured}
	}
	env, err := loadHubHandoffkeepEnv(config.HandoffkeepEnv)
	if err != nil {
		return nodePRStatusHK{Outcome: prStatusUnconfigured}
	}
	base, err := url.Parse(strings.TrimSuffix(env.URL, "/"))
	if err != nil {
		return nodePRStatusHK{Outcome: prStatusUnconfigured}
	}
	base.Path, base.RawPath, base.Fragment, base.RawFragment, base.ForceQuery = base.Path+"/v1/changes", "", "", "", false
	query := url.Values{}
	query.Set("pr", "https://github.com/"+args.Repo+"/pull/"+strconv.Itoa(args.PR))
	base.RawQuery = query.Encode()
	runContext, cancel := context.WithTimeout(ctx, prStatusHKTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(runContext, http.MethodGet, base.String(), nil)
	if err != nil {
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonRequest}
	}
	env.setAuth(request)
	request.Header.Set("Accept", "application/json")
	httpClient := &http.Client{Transport: prStatusHKTransport, Timeout: prStatusHKTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := httpClient.Do(request)
	if err != nil {
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonRequest}
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nodePRStatusHK{Outcome: prStatusNotFound, Status: http.StatusNotFound}
	default:
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonHTTPStatus}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, prStatusMaxHKChange+1))
	if err != nil {
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonBadBody}
	}
	if len(body) > prStatusMaxHKChange {
		return nodePRStatusHK{Outcome: prStatusTooLarge}
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil || object == nil {
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonBadBody}
	}
	// A list endpoint that answers 200 with no change for this PR holds no record.
	var changes []json.RawMessage
	if raw, listed := object["changes"]; listed && json.Unmarshal(raw, &changes) == nil && changes != nil && len(changes) == 0 {
		return nodePRStatusHK{Outcome: prStatusNotFound, Status: http.StatusOK}
	}
	compact := &bytes.Buffer{}
	if json.Compact(compact, body) != nil {
		return nodePRStatusHK{Outcome: prStatusFailed, Reason: prStatusReasonBadBody}
	}
	return nodePRStatusHK{Outcome: prStatusOK, Status: http.StatusOK, Change: compact.Bytes()}
}

// nodePRStatus is read-only: it neither takes the node's acting-command lock
// nor writes anything. Both sources are read on every call; a failed source is
// reported in its own outcome, never as an op error.
func (client *HubClient) nodePRStatus(ctx context.Context, args nodePRStatusArgs) (any, string) {
	result := nodePRStatusResult{
		Mergewake: readNodePRStatusMergewake(ctx, args),
		HK:        client.readNodePRStatusHK(ctx, args),
	}
	return fitPRStatusResult(result), ""
}
