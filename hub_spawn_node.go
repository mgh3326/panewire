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
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

type hubSpawnConfig struct {
	CWDMap       map[string]string `json:"cwd_map"`
	Workspace    string            `json:"workspace"`
	HERDRSession string            `json:"herdr_session"`
	// RuntugPath is an optional explicit path to the runtug binary for hosts
	// where the daemon's minimal PATH (launchd/systemd) does not include it.
	RuntugPath string `json:"runtug_path,omitempty"`
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

func loadHubSpawnConfig(path string) (hubSpawnConfig, error) {
	if path == "" {
		return hubSpawnConfig{}, errors.New("spawn disabled")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return hubSpawnConfig{}, errors.New("spawn disabled")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return hubSpawnConfig{}, errors.New("spawn disabled")
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var config hubSpawnConfig
	if decoder.Decode(&config) != nil {
		return hubSpawnConfig{}, errors.New("spawn disabled")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || !config.Enabled || config.Workspace == "" || config.HERDRSession == "" {
		return hubSpawnConfig{}, errors.New("spawn disabled")
	}
	for key, directory := range config.CWDMap {
		if !hubSpawnCWDKeyPattern.MatchString(key) || !filepath.IsAbs(directory) {
			return hubSpawnConfig{}, errors.New("spawn disabled")
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
	client.spawnMu.Lock()
	defer client.spawnMu.Unlock()

	result := client.runHubSpawn(ctx, message)
	_ = peer.write(ctx, struct {
		Type       string `json:"type"`
		RequestID  string `json:"request_id"`
		RC         int    `json:"rc"`
		JobID      string `json:"job_id"`
		Pane       string `json:"pane"`
		StdoutTail string `json:"stdout_tail"`
		Error      string `json:"error,omitempty"`
	}{Type: "job.spawn.result", RequestID: result.RequestID, RC: result.RC, JobID: result.JobID, Pane: result.Pane, StdoutTail: result.StdoutTail, Error: result.Error})
}

func (client *HubClient) runHubSpawn(parent context.Context, message hubOutboundMessage) hubSpawnResult {
	result := hubSpawnResult{RequestID: message.RequestID}
	config, err := loadHubSpawnConfig(client.spawnConfigPath)
	if err != nil {
		result.RC, result.Error = 2, "spawn_disabled"
		return result
	}
	cwd, found := config.CWDMap[message.CWDKey]
	if !found {
		result.RC, result.Error = 2, "cwd_unmapped"
		return result
	}
	extra := client.spawnTimeoutExtra
	if extra == 0 {
		extra = 60 * time.Second
	}
	runContext, cancel := context.WithTimeout(parent, time.Duration(message.WaitSeconds)*time.Second+extra)
	defer cancel()
	if message.Checkout != nil {
		prepare := client.spawnPrepare
		if prepare == nil {
			prepare = client.nodeWorktreePrepare
		}
		prepared, code := prepare(runContext, nodeWorktreePrepareArgs{Origin: message.Checkout.Origin, Mode: message.Checkout.Mode, Branch: message.Checkout.Branch, Sha: message.Checkout.Sha, Fetch: message.Checkout.Fetch, Dir: message.Checkout.Dir})
		if code != "" {
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
	// The retired spawner is gone; a node without runtug is not spawn-capable.
	// The daemon's PATH is minimal under launchd/systemd, so the binary is
	// resolved through config, then ~/.local/bin, then PATH.
	home, _ := os.UserHomeDir()
	runtugPath := hubSpawnRuntugPath(config.RuntugPath, home, hubSpawnExecutable, exec.LookPath)
	if runtugPath == "" {
		result.RC, result.Error = 2, "runtug_missing"
		return result
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
	briefText := message.BriefInline
	if message.BriefDoc != "" {
		prefix := config.BriefDocPrefix
		if prefix == "" {
			prefix = "briefs/"
		}
		if !strings.HasSuffix(prefix, "/") || !validHubSpawnDocKey(strings.TrimSuffix(prefix, "/")) || !strings.HasPrefix(message.BriefDoc, prefix) {
			result.RC, result.Error = 2, "brief_doc_unavailable"
			return result
		}
		briefText, err = client.fetchHubSpawnBrief(runContext, config.HandoffkeepEnv, message.BriefDoc)
		if err != nil || briefText == "" || !utf8.ValidString(briefText) || len([]byte(briefText)) > hubSpawnMaxBriefBytes {
			_ = temporary.Close()
			result.RC, result.Error = 2, "brief_doc_unavailable"
			return result
		}
	}
	_, writeErr := io.WriteString(temporary, briefText)
	closeErr := temporary.Close()
	if writeErr != nil || closeErr != nil {
		result.RC, result.Error = 1, "spawn_failed"
		return result
	}

	output, runErr := runHubSpawnCommand(runContext, runtugPath, cwd, temporaryName, config.Workspace, config.HERDRSession, message.Args)
	result.StdoutTail = output.tailString()
	result.Pane, result.JobID = output.pane, output.jobID
	if runErr == nil {
		return result
	}
	result.RC = 1
	var exitError *exec.ExitError
	if errors.As(runErr, &exitError) && exitError.ProcessState != nil && exitError.ExitCode() > 0 {
		result.RC = exitError.ExitCode()
	}
	if errors.Is(runErr, context.DeadlineExceeded) {
		result.Error = "timeout"
	} else {
		result.Error = "spawn_failed"
	}
	return result
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
	tail  []byte
	line  []byte
	pane  string
	jobID string
}

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

// hubSpawnRuntugPath resolves the runtug binary: an explicit config path
// (authoritative — a configured-but-missing path is a misconfiguration, not a
// fallback), then the conventional ~/.local/bin install, then PATH.
func hubSpawnRuntugPath(configured, home string, isExecutable func(string) bool, lookPath func(string) (string, error)) string {
	if configured != "" {
		// Abs anchors a relative configured path so exec does not silently
		// fall back to PATH for a name with no separator.
		if absolute, err := filepath.Abs(configured); err == nil && isExecutable(absolute) {
			return absolute
		}
		return ""
	}
	if home != "" {
		candidate := filepath.Join(home, ".local", "bin", "runtug")
		if isExecutable(candidate) {
			return candidate
		}
	}
	if path, err := lookPath("runtug"); err == nil {
		return path
	}
	return ""
}

func hubSpawnExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0
}

// hubSpawnOverrideReason records why runtug's model/host auto-selection was
// bypassed: the hub directed this spawn at this node. runtug requires the
// reason whenever -m or --host is explicit, and --host local always is.
const hubSpawnOverrideReason = "hub-directed"

func runHubSpawnCommand(ctx context.Context, runtugPath, cwd, briefPath, workspace, session string, args []string) (*hubSpawnOutput, error) {
	argv := []string{"spawn", "-c", cwd, "-p", briefPath, "--host", "local", "-w", workspace}
	argv = append(argv, args...)
	// The node owns the reason for pinning the host; callers cannot supply
	// --override-reason (it is not in the hub's args allowlist).
	argv = append(argv, "--override-reason", hubSpawnOverrideReason)
	command := exec.CommandContext(ctx, runtugPath, argv...)
	command.Env = hubSpawnEnvironment(os.Environ(), session)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = time.Second
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	output := &hubSpawnOutput{}
	command.Stdout = output
	command.Stderr = io.Discard
	err := command.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return output, context.DeadlineExceeded
	}
	return output, err
}
