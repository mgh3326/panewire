package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
)

type hubSpawnConfig struct {
	CWDMap       map[string]string `json:"cwd_map"`
	Workspace    string            `json:"workspace"`
	HERDRSession string            `json:"herdr_session"`
	// WorkRoot is the node's git work root for worktree.prepare: clones live
	// at <work_root>/<repo>. Node-local only; callers never supply it.
	WorkRoot       string `json:"work_root,omitempty"`
	BriefDocPrefix string `json:"brief_doc_prefix,omitempty"`
	HandoffkeepEnv string `json:"handoffkeep_env,omitempty"`
	// Checkouts is the checkout.pull allow-list: checkout name to absolute
	// checkout path (node_checkout_pull.go). Absent means the default rstack
	// checkout; an entry that is not a valid name and absolute path refuses the
	// whole file, as cwd_map does.
	Checkouts map[string]string `json:"checkouts,omitempty"`
	Enabled   bool              `json:"enabled"`
	// HandoffkeepEnvDefault is set only by resolveHubSpawnConfig on a node
	// running on defaults, never from JSON: the credential then resolves as
	// hkcred.Load does (handoffkeepEnv), without the HANDOFFKEEP_TOKEN
	// fallback a file named in spawn.json keeps. The path alone never decides.
	HandoffkeepEnvDefault bool `json:"-"`
}

func defaultHubSpawnConfigPath(path string) string {
	if path != "" {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "panewire", "spawn.json")
}

// hubSpawnConfigError is why spawn.json disables hub spawns: the rule the
// file failed and, where there is one, the offending key or value. The hub
// result still says spawn_disabled; the rule travels in the result's tail and
// in the node_spawn fleet event.
type hubSpawnConfigError struct {
	Rule   string
	Detail string
}

func (e *hubSpawnConfigError) Error() string {
	if e.Detail == "" {
		return "spawn.json rule " + e.Rule
	}
	return "spawn.json rule " + e.Rule + ": " + e.Detail
}

func spawnConfigRefusal(rule, detail string) (hubSpawnConfig, error) {
	return hubSpawnConfig{}, &hubSpawnConfigError{Rule: rule, Detail: utf8PrefixWithin(detail, 200)}
}

func loadHubSpawnConfig(path string) (hubSpawnConfig, error) {
	if path == "" {
		return spawnConfigRefusal("config_path_unset", "")
	}
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return spawnConfigRefusal("config_missing", "")
	case err != nil:
		return spawnConfigRefusal("config_unreadable", "")
	case !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0:
		return spawnConfigRefusal("config_not_regular_file", "")
	case info.Size() > 64<<10:
		return spawnConfigRefusal("config_too_large", "limit 65536 bytes")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return spawnConfigRefusal("config_unreadable", "")
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var config hubSpawnConfig
	if err := decoder.Decode(&config); err != nil {
		return spawnConfigRefusal("config_json_invalid", err.Error())
	}
	var trailing any
	switch {
	case decoder.Decode(&trailing) != io.EOF:
		return spawnConfigRefusal("config_trailing_data", "")
	case !config.Enabled:
		return spawnConfigRefusal("enabled_false", "")
	case config.Workspace == "":
		return spawnConfigRefusal("workspace_required", "")
	case config.HERDRSession == "":
		return spawnConfigRefusal("herdr_session_required", "")
	}
	keys := make([]string, 0, len(config.CWDMap))
	for key := range config.CWDMap {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if !hubSpawnCWDKeyPattern.MatchString(key) {
			return spawnConfigRefusal("cwd_map_key_pattern", fmt.Sprintf("key %q must match %s", key, hubSpawnCWDKeyPattern))
		}
		if !filepath.IsAbs(config.CWDMap[key]) {
			return spawnConfigRefusal("cwd_map_dir_absolute", fmt.Sprintf("cwd_map[%q] must be an absolute path", key))
		}
	}
	checkoutNames := make([]string, 0, len(config.Checkouts))
	for name := range config.Checkouts {
		checkoutNames = append(checkoutNames, name)
	}
	slices.Sort(checkoutNames)
	for _, name := range checkoutNames {
		dir := config.Checkouts[name]
		if !nodeCheckoutNamePattern.MatchString(name) {
			return spawnConfigRefusal("checkouts_invalid", fmt.Sprintf("checkout name %q must match %s", name, nodeCheckoutNamePattern))
		}
		if !filepath.IsAbs(dir) || len(dir) > nodeCommandMaxPathBytes {
			return spawnConfigRefusal("checkouts_invalid", fmt.Sprintf("checkouts[%q] must be an absolute path", name))
		}
	}
	return config, nil
}

func (client *HubClient) spawnStartupError() error {
	_, _, err := client.resolveHubSpawnConfig()
	var configErr *hubSpawnConfigError
	if errors.As(err, &configErr) && configErr.Rule == "config_json_invalid" && strings.Contains(configErr.Detail, `unknown field "runtug_path"`) {
		return errors.New("node startup refused: remove retired runtug_path from spawn.json before starting this in-process node; preserve cwd_map, workspace, herdr_session and work_root")
	}
	if err == nil {
		if _, err := client.acquireSpawnJournal(); err != nil {
			return fmt.Errorf("node startup refused: spawn journal unavailable: %w; configure a private writable node state directory and preserve existing journal records", err)
		}
		client.releaseSpawnJournal()
	}
	return nil
}

func (client *HubClient) handleHubSpawn(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	// A stop in progress refuses a spawn before it claims the journal row: the
	// hub replays the request to the restarted node, which then runs it whole.
	endWork, accepted := client.beginWork(true)
	if !accepted {
		client.warnMessage("node is stopping; spawn left for replay after restart")
		return
	}
	defer endWork()
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		client.sendSpawnResult(ctx, peer, hubSpawnResult{RequestID: message.RequestID, RC: 75, Error: "journal_unavailable"}, message.OperatorAuthorized)
		return
	}
	defer client.releaseSpawnJournal()
	claimed, cached, active, err := client.claimHubSpawn(ctx, journal, message)
	if err != nil {
		client.sendSpawnResult(ctx, peer, hubSpawnResult{RequestID: message.RequestID, RC: 75, Error: "journal_conflict"}, message.OperatorAuthorized)
		return
	}
	if cached != nil {
		client.sendSpawnResult(ctx, peer, *cached, message.OperatorAuthorized)
		return
	}
	if !claimed {
		// An executing entry this process is not running was left by a node
		// restart: nothing will ever complete it, so it is settled from the pane
		// (hub_spawn_settle.go); only a pane that cannot be read answers unknown,
		// and the hub then decides from the panes whether the request is orphaned.
		reply := "unknown"
		if active {
			reply = "reconciling"
		} else if settled, ok := client.settleRequest(ctx, journal, message.RequestID); ok {
			// The pane tells what the dead process could not: the replay gets
			// the real result instead of waiting for the request deadline.
			client.sendSpawnResult(ctx, peer, *settled, message.OperatorAuthorized)
			return
		}
		client.sendSpawnResult(ctx, peer, hubSpawnResult{RequestID: message.RequestID, RC: 75, Error: reply}, message.OperatorAuthorized)
		return
	}
	defer client.endHubSpawn(message.RequestID)
	deadline := time.Now().Add(time.Duration(message.WaitSeconds)*time.Second + client.spawnTimeoutExtra)
	if message.DeadlineMS != 0 && time.UnixMilli(message.DeadlineMS).Before(deadline) {
		deadline = time.UnixMilli(message.DeadlineMS)
	}
	executionContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	client.spawnMu.Lock()
	result := client.runClaimedHubSpawn(executionContext, message)
	client.spawnMu.Unlock()
	// Disconnect is not cancellation of this daemon-owned execution. Its result
	// remains available to the next authenticated replay even if this socket died.
	if ctx.Err() != nil && result.RC != 0 {
		// The node is stopping and the drain bound passed: this failure is the
		// abort, not the spawn's outcome (a pane may exist). The row stays
		// executing for the restart settle (hub_spawn_settle.go).
		return
	}
	persistCtx, persistCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer persistCancel()
	if journal.complete(persistCtx, result) != nil {
		return
	}
	client.sendSpawnResult(ctx, peer, result, message.OperatorAuthorized)
}

// claimHubSpawn claims message in the journal and reports whether this process
// is running it. The claim and the running mark change under one lock, so a
// replay racing a fresh claim never sees an executing entry nobody owns.
func (client *HubClient) claimHubSpawn(ctx context.Context, journal *nodeSpawnJournal, message hubOutboundMessage) (claimed bool, cached *hubSpawnResult, active bool, err error) {
	client.spawnActiveMu.Lock()
	defer client.spawnActiveMu.Unlock()
	claimed, cached, err = journal.claim(ctx, message)
	if claimed {
		if client.spawnActive == nil {
			client.spawnActive = map[string]bool{}
		}
		client.spawnActive[message.RequestID] = true
	}
	return claimed, cached, client.spawnActive[message.RequestID], err
}

// endHubSpawn drops the running mark once the execution has returned.
func (client *HubClient) endHubSpawn(requestID string) {
	client.spawnActiveMu.Lock()
	defer client.spawnActiveMu.Unlock()
	delete(client.spawnActive, requestID)
}

func (client *HubClient) sendSpawnResult(ctx context.Context, peer *hubClientConnection, result hubSpawnResult, extended bool) {
	_ = peer.write(ctx, spawnResultWire(result, extended))
}
func spawnResultWire(result hubSpawnResult, extended bool) any {
	if !extended {
		result.Receipt, result.Stage, result.StageMS, result.Retryable = nil, "", nil, false
		switch result.Error {
		case "reconciling", "unknown", "journal_unavailable", "journal_conflict":
			result.Error = "timeout" // Legacy hubs understand uncertainty as timeout.
		}
	}
	return struct {
		Receipt    *fleet.ExecutionReceipt `json:"receipt,omitempty"`
		Type       string                  `json:"type"`
		RequestID  string                  `json:"request_id"`
		RC         int                     `json:"rc"`
		JobID      string                  `json:"job_id"`
		Pane       string                  `json:"pane"`
		StdoutTail string                  `json:"stdout_tail"`
		Error      string                  `json:"error,omitempty"`
		StageMS    map[string]int64        `json:"stage_ms,omitempty"`
		Stage      string                  `json:"stage,omitempty"`
		Retryable  bool                    `json:"retryable,omitempty"`
	}{result.Receipt, "job.spawn.result", result.RequestID, result.RC, result.JobID, result.Pane, result.StdoutTail, result.Error, result.StageMS, result.Stage, result.Retryable}
}

func (client *HubClient) runHubSpawn(parent context.Context, message hubOutboundMessage) (result hubSpawnResult) {
	// reason and detail refine result.Error in the node_spawn fleet event.
	var reason, detail string
	started := client.fleetClock()()
	defer func() {
		client.noteLaunchResult(message, result)
		result = client.loginScreenSpawnRefusal(message, result)
		if result.RC != 0 && spawnResultFix(result.StdoutTail) == "" {
			fix := "Fix: " + nodeSpawnRefusalFix(result.Error, reason)
			if strings.HasPrefix(result.StdoutTail, "stderr: ") {
				result.StdoutTail = hubSpawnTailWithStderr("", strings.TrimPrefix(result.StdoutTail, "stderr: ")+"\n"+fix)
			} else if stdout, stderr, ok := strings.Cut(result.StdoutTail, "\nstderr: "); ok {
				result.StdoutTail = hubSpawnTailWithStderr(stdout, stderr+"\n"+fix)
			} else {
				result.StdoutTail = hubSpawnTailWithStderr(result.StdoutTail, fix)
			}
		}
		client.recordHubSpawnOutcome(message, result, reason, detail, started)
	}()
	result = hubSpawnResult{RequestID: message.RequestID, Retryable: true}
	if parent.Err() != nil {
		result.RC = 124
		result.Error = "timeout"
		result.Retryable = true
		return result
	}
	config, defaults, err := client.resolveHubSpawnConfig()
	if err != nil {
		result.RC, result.Error = 2, "spawn_disabled"
		var configErr *hubSpawnConfigError
		if errors.As(err, &configErr) {
			reason = configErr.Rule
		}
		detail = err.Error()
		result.StdoutTail = detail
		return result
	}
	cwd, found := config.CWDMap[message.CWDKey]
	if !found && defaults {
		// A node on defaults has no cwd_map: a repo is the directory of that
		// name under its work root.
		cwd, found = defaultRepoDir(config.WorkRoot, message.CWDKey)
	}
	// A checkout clones into the work root and sets cwd itself, so a repo
	// absent from cwd_map is not a refusal when the node has a work root.
	// This is the node-side half of the placement repo_unmapped rule.
	if !found && (message.Checkout == nil || config.WorkRoot == "") {
		result.RC, result.Error = 2, "cwd_unmapped"
		return result
	}
	// The document key is validated before any checkout or engine execution: a
	// key outside the node's prefix is refused whatever else is configured.
	if message.BriefDoc != "" {
		prefix := config.BriefDocPrefix
		if prefix == "" {
			prefix = "briefs/"
		}
		// "briefs" and "briefs/" name the same prefix.
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		if !strings.HasSuffix(prefix, "/") || !validHubSpawnDocKey(strings.TrimSuffix(prefix, "/")) || !strings.HasPrefix(message.BriefDoc, prefix) {
			result.RC, result.Error = 2, "brief_doc_unavailable"
			reason, detail = "brief_doc_prefix", "brief document key is outside this node's brief_doc_prefix"
			result.StdoutTail = detail
			return result
		}
	}
	extra := client.spawnTimeoutExtra
	runContext, cancel := context.WithTimeout(parent, time.Duration(message.WaitSeconds)*time.Second+extra)
	defer cancel()
	// The brief is fetched before any checkout: a failed fetch must not leave
	// a prepared worktree behind.
	briefText := message.BriefInline
	if message.BriefDoc != "" {
		fetched, err := client.fetchNodeSpawnBrief(runContext, config, message.BriefDoc)
		if err != nil || fetched == "" || !utf8.ValidString(fetched) || len([]byte(fetched)) > hubSpawnMaxBriefBytes {
			result.RC, result.Error = 2, "brief_doc_unavailable"
			reason, detail = "brief_doc_fetch", "this node could not fetch the brief document"
			if config.HandoffkeepEnv == "" {
				reason, detail = "handoffkeep_env_unset", "this node's spawn.json has no handoffkeep_env, so it cannot fetch the brief document"
			}
			result.StdoutTail = detail
			return result
		}
		briefText = fetched
	}
	var checkoutMS *int64
	if message.Checkout != nil {
		started := time.Now()
		result.Stage = "checkout"
		defer func() {
			if checkoutMS != nil {
				if result.StageMS == nil {
					result.StageMS = map[string]int64{}
				}
				result.StageMS["checkout"] = *checkoutMS
			}
		}()
		prepare := client.spawnPrepare
		if prepare == nil {
			prepare = client.nodeWorktreePrepare
		}
		prepared, code := prepare(runContext, nodeWorktreePrepareArgs{Origin: message.Checkout.Origin, Mode: message.Checkout.Mode, Branch: message.Checkout.Branch, Sha: message.Checkout.Sha, Fetch: message.Checkout.Fetch, Dir: message.Checkout.Dir})
		ms := time.Since(started).Milliseconds()
		checkoutMS = &ms
		if code != "" {
			result.Retryable = true
			result.RC, result.Error = 81, "checkout_refused:"+code
			return result
		}
		outcome, ok := prepared.(nodeWorktreePrepareResult)
		if !ok || (outcome.Outcome != nodePreparePrepared && outcome.Outcome != nodePrepareReused) || outcome.Worktree == nil {
			name := "git-failed"
			if ok && outcome.Outcome != "" {
				name = outcome.Outcome
			}
			result.RC, result.Error = 81, "checkout_refused:"+name
			return result
		}
		cwd = outcome.Worktree.Path
	}

	temporary, err := os.CreateTemp("", "panewire-spawn-*")
	if err != nil {
		result.RC, result.Error = 1, "spawn_failed"
		return result
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		result.RC, result.Error = 1, "spawn_failed"
		return result
	}
	_, writeErr := io.WriteString(temporary, briefText)
	closeErr := temporary.Close()
	if writeErr != nil || closeErr != nil {
		result.RC, result.Error = 1, "spawn_failed"
		return result
	}

	engine := client.spawnEngine
	if engine == nil {
		engine = runHubSpawnEngine
	}
	output, execution := engine(runContext, cwd, temporaryName, config.Workspace, config.HERDRSession, message.Args)
	result.Receipt = &execution.Receipt
	result.StdoutTail = output.tailString()
	result.Pane, result.JobID = output.pane, output.jobID
	if execution.Receipt.Pane != "" {
		result.Pane = execution.Receipt.Pane
	}
	if execution.Receipt.Job != "" {
		result.JobID = execution.Receipt.Job
	}
	result.RC, result.Stage, result.StageMS, result.Retryable = execution.RC, execution.Stage, execution.StageMS, execution.Retryable
	if execution.RC != 0 && runContext.Err() != nil {
		result.RC = 124
		result.Error = "timeout"
	} else if result.RC != 0 {
		result.Error = "spawn_failed"
	}
	if result.RC == 0 {
		return result
	}
	// panewire explains a refusal on stderr; without it the caller only sees
	// an empty "remote spawn:" message.
	if stderr := strings.TrimSpace(fleetEventRedact(output.stderr.String())); stderr != "" {
		result.StdoutTail = hubSpawnTailWithStderr(result.StdoutTail, stderr)
		detail = fmt.Sprintf("rc=%d stderr: %s", result.RC, utf8SuffixWithin(stderr, fleetEventMaxDetail-32))
	}
	return result
}

const authUnavailableSpawnHint = "Sign in to the selected quota account on the executing host and refresh that pool with panewire quota refresh POOL, then retry."

// loginScreenSpawnRefusal translates a verified rc=76 login-screen capture to
// the refusal code understood by older hubs. It changes only the hub result;
// the saved capture and job record remain the original spawn outcome.
func (client *HubClient) loginScreenSpawnRefusal(message hubOutboundMessage, result hubSpawnResult) hubSpawnResult {
	if result.RC != fleet.ExitSubmitUnconfirmed || !result.Retryable {
		return result
	}
	kind := fleet.ProfileKind(hubSpawnArg(message.Args, "-m"))
	job := result.JobID
	if job == "" {
		job = hubSpawnArg(message.Args, "--job")
	}
	if kind == "" || !client.launchShowedLoginScreen(kind, job) {
		return result
	}
	result.RC = fleet.ExitAuthUnavailable
	result.StdoutTail = hubSpawnTailWithStderr("", "AUTH_UNAVAILABLE: selected quota account login missing\nFix: "+authUnavailableSpawnHint)
	return result
}

// hubSpawnStderrTail bounds the panewire stderr kept in a spawn result.
const hubSpawnStderrTail = 1536

// hubSpawnTailWithStderr appends panewire's stderr tail to the stdout tail,
// keeping the whole within hubSpawnResultTail (the hub's wire bound) by
// dropping the oldest stdout first.
func hubSpawnTailWithStderr(stdout, stderr string) string {
	section := "stderr: " + utf8SuffixWithin(stderr, hubSpawnStderrTail)
	if stdout = strings.TrimRight(stdout, "\n"); stdout != "" {
		section = "\n" + section
	}
	return utf8SuffixWithin(stdout, hubSpawnResultTail-len(section)) + section
}

// fleetClock is the clock fleet events are stamped with.
func (client *HubClient) fleetClock() func() time.Time {
	if client.now != nil {
		return client.now
	}
	return time.Now
}

// recordHubSpawnOutcome writes the node_spawn fleet event of a hub spawn: a
// refused or failed one with its reason, a successful one with the profile it
// launched and how long the node took. Neither carries screen text, the brief
// or argv. reason defaults to the refusal's closed-set code, else the wire
// error; detail to the rc.
func (client *HubClient) recordHubSpawnOutcome(message hubOutboundMessage, result hubSpawnResult, reason, detail string, started time.Time) {
	now := client.fleetClock()
	write := emitFleetEvent
	if client.fleetEvents != nil {
		write = client.fleetEvents
	}
	if result.Error == "" {
		if result.RC != 0 {
			return
		}
		event := fleetEvent{Kind: "node_spawn", Job: hubSpawnArg(message.Args, "--job"), RequestID: message.RequestID, Outcome: "ok", Reason: "spawned"}
		if result.Receipt != nil {
			event.Profile = result.Receipt.Profile
		}
		elapsed := max(now().Sub(started).Milliseconds(), 0)
		event.DurationMS = &elapsed
		write(now(), event)
		return
	}
	outcome := "refused"
	if result.Error == "spawn_failed" || result.Error == "timeout" {
		outcome = "failed"
	}
	if reason == "" {
		reason = result.Error
		if code := spawnFailureCode(result); result.Error == "spawn_failed" && code != "" {
			reason = code
		}
	}
	if detail == "" {
		detail = fmt.Sprintf("rc=%d", result.RC)
	}
	write(now(), fleetEvent{Kind: "node_spawn", Job: hubSpawnArg(message.Args, "--job"), RequestID: message.RequestID, Outcome: outcome, Reason: reason, Detail: result.Error + " " + detail})
}

// hubSpawnArg returns the value of flag in a hub spawn's argv, or "".
func hubSpawnArg(args []string, flag string) string {
	for i, arg := range args {
		if value, ok := strings.CutPrefix(arg, flag+"="); ok {
			return value
		}
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// hubSpawnBriefTransport is the brief fetch's transport; nil is the default.
// Tests point it at a TLS test server.
var hubSpawnBriefTransport http.RoundTripper

// fetchNodeSpawnBrief fetches a brief document with the node's own hk
// credential under its spawn config (hubSpawnConfig.handoffkeepEnv): a node
// on defaults resolves as hkcred.Load does, a spawn.json handoffkeep_env is an
// explicit file.
func (client *HubClient) fetchNodeSpawnBrief(ctx context.Context, config hubSpawnConfig, key string) (string, error) {
	env, err := config.handoffkeepEnv()
	if err != nil {
		return "", errors.New("brief unavailable")
	}
	return client.fetchHubSpawnBriefWith(ctx, env, key)
}

// fetchHubSpawnBrief fetches a brief document with the explicit hk env file
// envPath (loadHubHandoffkeepEnv).
func (client *HubClient) fetchHubSpawnBrief(ctx context.Context, envPath, key string) (string, error) {
	env, err := loadHubHandoffkeepEnv(envPath)
	if err != nil {
		return "", errors.New("brief unavailable")
	}
	return client.fetchHubSpawnBriefWith(ctx, env, key)
}

func (client *HubClient) fetchHubSpawnBriefWith(ctx context.Context, env hubHandoffkeepEnv, key string) (string, error) {
	if !validHubSpawnDocKey(key) {
		return "", errors.New("brief unavailable")
	}
	base, err := url.Parse(strings.TrimSuffix(env.URL, "/"))
	if err != nil {
		return "", errors.New("brief unavailable")
	}
	base.Path = "/v1/documents/" + key
	base.RawPath = ""
	base.RawQuery = ""
	base.ForceQuery = false
	base.Fragment = ""
	base.RawFragment = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return "", errors.New("brief unavailable")
	}
	env.setAuth(request)
	request.Header.Set("Accept", "application/json")
	response, err := (&http.Client{Transport: hubSpawnBriefTransport, Timeout: handoffkeepTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(request)
	if err != nil {
		return "", errors.New("brief unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("brief unavailable")
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, hubSpawnMaxBodyBytes+1))
	if err != nil || len(payload) > hubSpawnMaxBodyBytes {
		return "", errors.New("brief unavailable")
	}
	var document struct {
		Body string `json:"body"`
	}
	if json.Unmarshal(payload, &document) != nil {
		return "", errors.New("brief unavailable")
	}
	return document.Body, nil
}

type hubSpawnOutput struct {
	// stderr keeps panewire's last stderr bytes for a failed spawn's result.
	stderr hubSpawnStderr
	tail   []byte
	line   []byte
	pane   string
	jobID  string
}

// hubSpawnStderr is an io.Writer that keeps the last hubSpawnStderrTail*2
// bytes written to it; the result bound is applied after redaction.
type hubSpawnStderr struct{ tail []byte }

func (s *hubSpawnStderr) Write(value []byte) (int, error) {
	s.tail = append(s.tail, value...)
	if limit := 2 * hubSpawnStderrTail; len(s.tail) > limit {
		s.tail = append(s.tail[:0], s.tail[len(s.tail)-limit:]...)
	}
	return len(value), nil
}

func (s *hubSpawnStderr) String() string { return strings.ToValidUTF8(string(s.tail), "?") }

func (output *hubSpawnOutput) Write(value []byte) (int, error) {
	const maxLine = 8 << 10
	written := len(value)
	if len(value) >= hubSpawnResultTail {
		output.tail = append(output.tail[:0], value[len(value)-hubSpawnResultTail:]...)
	} else {
		output.tail = append(output.tail, value...)
		if len(output.tail) > hubSpawnResultTail {
			output.tail = append(output.tail[:0], output.tail[len(output.tail)-hubSpawnResultTail:]...)
		}
	}
	for len(value) > 0 {
		index := bytes.IndexByte(value, '\n')
		if index < 0 {
			output.line = append(output.line, value...)
			if len(output.line) > maxLine {
				output.line = append(output.line[:0], output.line[len(output.line)-maxLine:]...)
			}
			break
		}
		output.line = append(output.line, value[:index]...)
		output.parseLine()
		output.line = output.line[:0]
		value = value[index+1:]
	}
	return written, nil
}

func (output *hubSpawnOutput) parseLine() {
	pane, jobID := parseHubSpawnOKLine(strings.TrimSuffix(string(output.line), "\r"))
	if pane == "" && jobID == "" {
		return
	}
	output.pane, output.jobID = pane, jobID
}

// parseHubSpawnOK follows panewire's stable OK line rather than guessing from a
// human-facing transcript. Missing keys remain empty by contract.
func parseHubSpawnOK(raw []byte) (pane, jobID string) {
	for _, line := range strings.Split(string(raw), "\n") {
		if parsedPane, parsedJob := parseHubSpawnOKLine(strings.TrimSuffix(line, "\r")); parsedPane != "" || parsedJob != "" {
			pane, jobID = parsedPane, parsedJob
		}
	}
	return pane, jobID
}

func parseHubSpawnOKLine(line string) (pane, jobID string) {
	if !strings.HasPrefix(line, "OK pane=") {
		return "", ""
	}
	for _, field := range strings.Fields(line) {
		key, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch key {
		case "pane":
			pane = value
		case "job":
			jobID = value
		}
	}
	return pane, jobID
}

func (output *hubSpawnOutput) tailString() string {
	output.parseLine()
	return string(output.tail)
}

func hubSpawnEnvironment(environment []string, session string) []string {
	filtered := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if found && name != "HERDR_SESSION" {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, "HERDR_SESSION="+session)
}

type hubSpawnEngine func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution)

func runHubSpawnEngine(ctx context.Context, cwd, brief, workspace, session string, args []string) (*hubSpawnOutput, fleet.Execution) {
	output := &hubSpawnOutput{}
	result := fleet.ExecuteNode(ctx, cwd, brief, workspace, session, args, output, &output.stderr)
	return output, result
}

func (client *HubClient) acquireSpawnJournal() (*nodeSpawnJournal, error) {
	client.spawnJournalMu.Lock()
	defer client.spawnJournalMu.Unlock()
	if client.spawnJournal == nil {
		root := client.spawnJournalDir
		if root == "" {
			state := panewireStateDir()
			if state == "" {
				return nil, errors.New("configure an absolute writable XDG_STATE_HOME for the spawn journal")
			}
			root = filepath.Join(state, "spawn")
		}
		if client.spawnConfigPath != "" {
			legacy := filepath.Join(filepath.Dir(client.spawnConfigPath), "spawn-inbox.sqlite3")
			if legacy != filepath.Join(root, "spawn-inbox.sqlite3") {
				if _, err := os.Lstat(legacy); err == nil {
					return nil, errors.New("legacy journal exists beside spawn.json; stop the node and migrate it with SQLite backup to the node state spawn directory before restarting")
				} else if !errors.Is(err, os.ErrNotExist) {
					return nil, err
				}
			}
		}
		journal, err := openNodeSpawnJournal(root)
		if err != nil {
			return nil, err
		}
		client.spawnJournal = journal
	}
	client.spawnJournalUsers++
	return client.spawnJournal, nil
}
func (client *HubClient) releaseSpawnJournal() {
	client.spawnJournalMu.Lock()
	defer client.spawnJournalMu.Unlock()
	client.spawnJournalUsers--
	if client.spawnJournalUsers == 0 {
		client.spawnJournal.db.Close()
		client.spawnJournal = nil
	}
}

func (client *HubClient) runClaimedHubSpawn(ctx context.Context, message hubOutboundMessage) hubSpawnResult {
	if message.DeadlineMS != 0 && time.Now().UnixMilli() >= message.DeadlineMS {
		return hubSpawnResult{RequestID: message.RequestID, JobID: hubSpawnArg(message.Args, "--job"), RC: 124, Stage: "claim", Error: "deadline_not_started", Retryable: true}
	}
	return client.runHubSpawn(ctx, message)
}
