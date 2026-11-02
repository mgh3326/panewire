package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mgh3326/panewire/internal/fleet"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
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
	Enabled        bool   `json:"enabled"`
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
	return config, nil
}

func (client *HubClient) rememberHubSpawn(requestID string) bool {
	client.spawnSeenMu.Lock()
	defer client.spawnSeenMu.Unlock()
	if client.spawnSeen == nil {
		client.spawnSeen = make(map[string]struct{})
	}
	if _, seen := client.spawnSeen[requestID]; seen {
		return false
	}
	client.spawnSeen[requestID] = struct{}{}
	return true
}

func (client *HubClient) handleHubSpawn(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	if !client.rememberHubSpawn(message.RequestID) {
		return
	}
	executionContext, cancel := context.WithTimeout(ctx, time.Duration(message.WaitSeconds)*time.Second+client.spawnTimeoutExtra)
	defer cancel()
	client.spawnMu.Lock()
	defer client.spawnMu.Unlock()

	result := client.runHubSpawn(executionContext, message)
	_ = peer.write(ctx, struct {
		Type       string           `json:"type"`
		RequestID  string           `json:"request_id"`
		RC         int              `json:"rc"`
		JobID      string           `json:"job_id"`
		Pane       string           `json:"pane"`
		StdoutTail string           `json:"stdout_tail"`
		Error      string           `json:"error,omitempty"`
		StageMS    map[string]int64 `json:"stage_ms,omitempty"`
		Stage      string           `json:"stage,omitempty"`
		Retryable  bool             `json:"retryable,omitempty"`
	}{Type: "job.spawn.result", RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, Pane: result.Pane, StdoutTail: result.StdoutTail, Error: result.Error, StageMS: result.StageMS, Stage: result.Stage, Retryable: result.Retryable})
}

func (client *HubClient) runHubSpawn(parent context.Context, message hubOutboundMessage) (result hubSpawnResult) {
	// reason and detail refine result.Error in the node_spawn fleet event.
	var reason, detail string
	defer func() { client.recordHubSpawnOutcome(message, result, reason, detail) }()
	result = hubSpawnResult{RequestID: message.RequestID}
	if parent.Err() != nil {
		result.RC = 124
		result.Error = "timeout"
		result.Retryable = true
		return result
	}
	config, err := loadHubSpawnConfig(client.spawnConfigPath)
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
	if !found {
		result.RC, result.Error = 2, "cwd_unmapped"
		return result
	}
	// The document key is validated before any checkout or runtug lookup: a
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
		fetched, err := client.fetchHubSpawnBrief(runContext, config.HandoffkeepEnv, message.BriefDoc)
		if err != nil || fetched == "" || !utf8.ValidString(fetched) || len([]byte(fetched)) > hubSpawnMaxBriefBytes {
			result.RC, result.Error = 2, "brief_doc_unavailable"
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
	result.StdoutTail = output.tailString()
	result.Pane, result.JobID = output.pane, output.jobID
	result.RC, result.Stage, result.StageMS, result.Retryable = execution.RC, execution.Stage, execution.StageMS, execution.Retryable
	if runContext.Err() != nil {
		result.RC = 124
		result.Error = "timeout"
	} else if result.RC != 0 {
		result.Error = "spawn_failed"
	}
	if result.RC == 0 {
		return result
	}
	// runtug explains a refusal on stderr; without it the caller only sees
	// an empty "remote spawn:" message.
	if stderr := strings.TrimSpace(fleetEventRedact(output.stderr.String())); stderr != "" {
		result.StdoutTail = hubSpawnTailWithStderr(result.StdoutTail, stderr)
		detail = fmt.Sprintf("rc=%d stderr: %s", result.RC, utf8SuffixWithin(stderr, fleetEventMaxDetail-32))
	}
	return result
}

// hubSpawnStderrTail bounds the runtug stderr kept in a spawn result.
const hubSpawnStderrTail = 1536

// hubSpawnTailWithStderr appends runtug's stderr tail to the stdout tail,
// keeping the whole within hubSpawnResultTail (the hub's wire bound) by
// dropping the oldest stdout first.
func hubSpawnTailWithStderr(stdout, stderr string) string {
	section := "stderr: " + utf8SuffixWithin(stderr, hubSpawnStderrTail)
	if stdout = strings.TrimRight(stdout, "\n"); stdout != "" {
		section = "\n" + section
	}
	return utf8SuffixWithin(stdout, hubSpawnResultTail-len(section)) + section
}

// recordHubSpawnOutcome writes the node_spawn fleet event for a refused or
// failed hub spawn. reason defaults to the wire error; detail to the rc.
func (client *HubClient) recordHubSpawnOutcome(message hubOutboundMessage, result hubSpawnResult, reason, detail string) {
	if result.Error == "" {
		return
	}
	outcome := "refused"
	if result.Error == "spawn_failed" || result.Error == "timeout" {
		outcome = "failed"
	}
	if reason == "" {
		reason = result.Error
	}
	if detail == "" {
		detail = fmt.Sprintf("rc=%d", result.RC)
	}
	now := time.Now
	if client.now != nil {
		now = client.now
	}
	emitFleetEvent(now(), "node_spawn", hubSpawnArg(message.Args, "--job"), outcome, reason, result.Error+" "+detail)
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

func (client *HubClient) fetchHubSpawnBrief(ctx context.Context, envPath, key string) (string, error) {
	if !validHubSpawnDocKey(key) || envPath == "" {
		return "", errors.New("brief unavailable")
	}
	env, err := loadHubHandoffkeepEnv(envPath)
	if err != nil {
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
	request.Header.Set("Authorization", "Bearer "+env.Token)
	request.Header.Set("Accept", "application/json")
	response, err := (&http.Client{Timeout: handoffkeepTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(request)
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
	// stderr keeps runtug's last stderr bytes for a failed spawn's result.
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

// parseHubSpawnOK follows runtug's stable OK line rather than guessing from a
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
