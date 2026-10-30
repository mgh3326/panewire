package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Node commands (docs/node-commands.md) are operator-authenticated hub→node
// requests that read or act on this node's herdr panes and git worktrees for a
// fleet tool such as mergewake reap, so no tool needs ssh to a session host.
// The op set is closed. pane.close never trusts the caller's snapshot: the
// node re-reads herdr and closes only a pane whose id, name, status and cwd
// still match exactly.
const (
	nodeCommandAgentsList      = "agents.list"
	nodeCommandPaneClose       = "pane.close"
	nodeCommandPaneCapture     = "pane.capture"
	nodeCommandWorktreeInspect = "worktree.inspect"

	// nodeCommandMaxResultBytes keeps a reply under the hub's per-message read
	// limit (hubMaxMessageBytes) with room for the envelope.
	nodeCommandMaxResultBytes = 28 << 10
	nodeCommandMaxArgsBytes   = 16 << 10
	nodeCommandRunTimeout     = 20 * time.Second
	nodeCommandMaxPathBytes   = 4096
)

// Closed outcome vocabularies. pane.close mirrors mergewake reap's reclose
// reasons byte for byte so receipts read the same over either transport.
const (
	nodePaneClosed       = "closed"
	nodePaneChanged      = "pane-changed"
	nodePaneGone         = "pane-gone"
	nodeCaptureReport    = "report"
	nodeCaptureScreen    = "screen"
	nodeCaptureWould     = "would-screen"
	nodeCaptureNotAJob   = "not-a-job"
	nodeCaptureScreenTxt = "pane-screen.txt"
)

var (
	nodeCommandPaneIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	nodeCommandStatusPattern = regexp.MustCompile(`^[a-z_-]{1,32}$`)
)

func knownNodeCommandOp(op string) bool {
	switch op {
	case nodeCommandAgentsList, nodeCommandPaneClose, nodeCommandPaneCapture, nodeCommandWorktreeInspect:
		return true
	}
	return false
}

// validNodeCommandError is the closed set a node may answer instead of a
// result. node_lost and node_timeout are hub-local and never on the wire.
func validNodeCommandError(code string) bool {
	switch code {
	case "invalid_args", "herdr_failed", "close_failed", "capture_failed", "jobs_root_unset", "result_too_large":
		return true
	}
	return false
}

type nodePaneCloseArgs struct {
	PaneID      string `json:"pane_id"`
	Name        string `json:"name"`
	AgentStatus string `json:"agent_status"`
	Cwd         string `json:"cwd"`
}

type nodePaneCaptureArgs struct {
	PaneID string `json:"pane_id"`
	Name   string `json:"name"`
	Cwd    string `json:"cwd"`
	// MergedAfter is a unix second: report.md counts only when its mtime is
	// strictly later. Zero means no report is fresh enough.
	MergedAfter int64 `json:"merged_after"`
	// Write false reports where the screen would be saved without reading
	// the pane or touching the job directory.
	Write bool `json:"write"`
}

type nodeWorktreeInspectArgs struct {
	Cwd string `json:"cwd"`
}

// nodeAgentRow keeps herdr's own field names so a caller that already decodes
// `herdr agent list` rows reads these unchanged.
type nodeAgentRow struct {
	PaneID      string `json:"pane_id"`
	Name        string `json:"name,omitempty"`
	AgentStatus string `json:"agent_status"`
	Agent       string `json:"agent,omitempty"`
	Cwd         string `json:"cwd"`
}

type nodeAgentsListResult struct {
	Agents []nodeAgentRow `json:"agents"`
	// Panes is `herdr pane list`, which also names plain shells; PanesOK is
	// false when that read failed, so a caller treats pane usage as unknown.
	Panes   []nodeAgentRow `json:"panes"`
	PanesOK bool           `json:"panes_ok"`
}

type nodePaneCloseResult struct {
	Outcome string `json:"outcome"`
}

type nodePaneCaptureResult struct {
	Outcome string `json:"outcome"`
	Saved   string `json:"saved,omitempty"`
}

type nodeWorktree struct {
	Path      string `json:"path"`
	Branch    string `json:"branch"`
	Head      string `json:"head"`
	OriginURL string `json:"origin_url"`
	Common    string `json:"common"`
}

type nodeWorktreeInspectResult struct {
	Worktree *nodeWorktree `json:"worktree"`
}

func validNodeJobName(name string) bool {
	if name == "" || len(name) > 128 || name[0] == '-' || name[0] == '.' {
		return false
	}
	return !strings.ContainsAny(name, "/\\ \t\r\n\x00") && !strings.Contains(name, "..")
}

func validNodeCommandPath(path string) bool {
	return path != "" && len(path) <= nodeCommandMaxPathBytes && filepath.IsAbs(path) && !strings.ContainsAny(path, "\r\n\x00")
}

// decodeNodeCommandStrict decodes exactly one JSON object with no unknown or
// trailing fields. Both hub and node validate with it.
func decodeNodeCommandStrict(raw []byte, target any) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return false
	}
	var trailing any
	return decoder.Decode(&trailing) == io.EOF
}

// decodeNodeCommandArgs returns the typed, validated arguments for op.
func decodeNodeCommandArgs(op string, raw json.RawMessage) (any, bool) {
	if len(raw) == 0 || len(raw) > nodeCommandMaxArgsBytes {
		return nil, false
	}
	switch op {
	case nodeCommandAgentsList:
		var args struct{}
		return args, decodeNodeCommandStrict(raw, &args)
	case nodeCommandPaneClose:
		var args nodePaneCloseArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && nodeCommandStatusPattern.MatchString(args.AgentStatus) && validNodeCommandPath(args.Cwd)
		return args, valid
	case nodeCommandPaneCapture:
		var args nodePaneCaptureArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && validNodeCommandPath(args.Cwd) && args.MergedAfter >= 0
		return args, valid
	case nodeCommandWorktreeInspect:
		var args nodeWorktreeInspectArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeCommandPath(args.Cwd)
	}
	return nil, false
}

// canonicalNodeCommandResult validates a node's result for op and re-encodes
// it, so the hub only ever forwards the documented shape.
func canonicalNodeCommandResult(op string, raw json.RawMessage) (json.RawMessage, bool) {
	var target any
	valid := false
	switch op {
	case nodeCommandAgentsList:
		var result nodeAgentsListResult
		valid = decodeNodeCommandStrict(raw, &result) && result.Agents != nil && (result.Panes != nil || !result.PanesOK)
		target = result
	case nodeCommandPaneClose:
		var result nodePaneCloseResult
		valid = decodeNodeCommandStrict(raw, &result) && (result.Outcome == nodePaneClosed || result.Outcome == nodePaneChanged || result.Outcome == nodePaneGone)
		target = result
	case nodeCommandPaneCapture:
		var result nodePaneCaptureResult
		switch {
		case !decodeNodeCommandStrict(raw, &result):
		case result.Outcome == nodeCaptureReport || result.Outcome == nodeCaptureScreen || result.Outcome == nodeCaptureWould:
			valid = validNodeCommandPath(result.Saved)
		case result.Outcome == nodeCaptureNotAJob || result.Outcome == nodePaneChanged || result.Outcome == nodePaneGone:
			valid = result.Saved == ""
		}
		target = result
	case nodeCommandWorktreeInspect:
		var result nodeWorktreeInspectResult
		valid = decodeNodeCommandStrict(raw, &result)
		target = result
	}
	if !valid {
		return nil, false
	}
	encoded, err := json.Marshal(target)
	return encoded, err == nil
}

func defaultNodeCommandRunner(name string) relayCommandRunner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).Output()
	}
}

func (client *HubClient) nodeHerdrRunner() relayCommandRunner {
	if client.nodeHerdr != nil {
		return client.nodeHerdr
	}
	return defaultNodeCommandRunner("herdr")
}

func (client *HubClient) nodeGitRunner() relayCommandRunner {
	if client.nodeGit != nil {
		return client.nodeGit
	}
	return defaultNodeCommandRunner("git")
}

type nodeCommandReply struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Op        string          `json:"op"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func (client *HubClient) handleNodeCommand(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	runContext, cancel := context.WithTimeout(ctx, nodeCommandRunTimeout)
	defer cancel()
	reply := nodeCommandReply{Type: "node.command.result", RequestID: message.RequestID, Op: message.CommandOp}
	result, code := client.runNodeCommand(runContext, message.CommandOp, message.CommandArgs)
	if code == "" {
		encoded, err := json.Marshal(result)
		switch {
		case err != nil:
			code = "herdr_failed"
		case len(encoded) > nodeCommandMaxResultBytes:
			code = "result_too_large"
		default:
			reply.Result = encoded
		}
	}
	reply.Error = code
	_ = peer.write(ctx, reply)
}

func (client *HubClient) runNodeCommand(ctx context.Context, op string, raw json.RawMessage) (any, string) {
	decoded, valid := decodeNodeCommandArgs(op, raw)
	if !valid {
		return nil, "invalid_args"
	}
	switch args := decoded.(type) {
	case nodePaneCloseArgs:
		// One acting command at a time per node, so two closes can never
		// interleave their re-read and close.
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneClose(ctx, args)
	case nodePaneCaptureArgs:
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneCapture(ctx, args)
	case nodeWorktreeInspectArgs:
		return nodeWorktreeInspect(ctx, client.nodeGitRunner(), args.Cwd), ""
	}
	return client.nodeAgentsList(ctx)
}

func nodeHerdrRows(output []byte, key string) ([]nodeAgentRow, error) {
	var envelope struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(output, &envelope); err != nil {
		return nil, err
	}
	var rows []nodeAgentRow
	if err := json.Unmarshal(envelope.Result[key], &rows); err != nil || rows == nil {
		return nil, errors.New("herdr list: missing " + key)
	}
	return rows, nil
}

func (client *HubClient) readNodeAgents(ctx context.Context) ([]nodeAgentRow, error) {
	output, err := client.nodeHerdrRunner()(ctx, "agent", "list")
	if err != nil {
		return nil, err
	}
	return nodeHerdrRows(output, "agents")
}

func (client *HubClient) nodeAgentsList(ctx context.Context) (any, string) {
	agents, err := client.readNodeAgents(ctx)
	if err != nil {
		return nil, "herdr_failed"
	}
	result := nodeAgentsListResult{Agents: agents}
	if output, err := client.nodeHerdrRunner()(ctx, "pane", "list"); err == nil {
		if panes, err := nodeHerdrRows(output, "panes"); err == nil {
			for index := range panes {
				// pane list carries a label, not the agent name.
				panes[index].Name = ""
			}
			result.Panes, result.PanesOK = panes, true
		}
	}
	return result, ""
}

// nodePaneMatch re-reads herdr and classifies the caller's expected pane.
func (client *HubClient) nodePaneMatch(ctx context.Context, paneID, name, status, cwd string, checkStatus bool) (string, error) {
	agents, err := client.readNodeAgents(ctx)
	if err != nil {
		return "", err
	}
	for _, agent := range agents {
		if agent.PaneID != paneID {
			continue
		}
		if agent.Name != name || agent.Cwd != cwd || (checkStatus && agent.AgentStatus != status) {
			return nodePaneChanged, nil
		}
		return "", nil
	}
	return nodePaneGone, nil
}

func (client *HubClient) nodePaneClose(ctx context.Context, args nodePaneCloseArgs) (any, string) {
	outcome, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, args.AgentStatus, args.Cwd, true)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCloseResult{Outcome: outcome}, ""
	}
	if _, err := client.nodeHerdrRunner()(ctx, "pane", "close", args.PaneID); err != nil {
		return nil, "close_failed"
	}
	return nodePaneCloseResult{Outcome: nodePaneClosed}, ""
}

// nodePaneCapture saves a pane's output into its job directory before a close,
// with the same rules mergewake reap applies over ssh: the job directory must
// already exist (proof the fleet spawned the pane), and report.md wins only
// when it is newer than the merge.
func (client *HubClient) nodePaneCapture(ctx context.Context, args nodePaneCaptureArgs) (any, string) {
	if client.jobsInboxRoot == "" {
		return nil, "jobs_root_unset"
	}
	dir := filepath.Join(client.jobsInboxRoot, "jobs", args.Name)
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist) || (err == nil && !info.IsDir()):
		return nodePaneCaptureResult{Outcome: nodeCaptureNotAJob}, ""
	case err != nil:
		return nil, "capture_failed"
	}
	report := filepath.Join(dir, "report.md")
	if args.MergedAfter > 0 {
		info, err := os.Stat(report)
		switch {
		case err == nil && info.Mode().IsRegular() && info.ModTime().Unix() > args.MergedAfter:
			return nodePaneCaptureResult{Outcome: nodeCaptureReport, Saved: report}, ""
		case err != nil && !errors.Is(err, os.ErrNotExist):
			return nil, "capture_failed"
		}
	}
	screen := filepath.Join(dir, nodeCaptureScreenTxt)
	if !args.Write {
		return nodePaneCaptureResult{Outcome: nodeCaptureWould, Saved: screen}, ""
	}
	// Status is not compared: an idle pane may report done by now, and the
	// close that follows re-checks it anyway.
	outcome, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, "", args.Cwd, false)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCaptureResult{Outcome: outcome}, ""
	}
	output, err := client.nodeHerdrRunner()(ctx, "pane", "read", args.PaneID)
	if err != nil {
		return nil, "capture_failed"
	}
	if err := replaceFileAtomically(screen, ".pane-screen-*", output, 0600); err != nil {
		return nil, "capture_failed"
	}
	return nodePaneCaptureResult{Outcome: nodeCaptureScreen, Saved: screen}, ""
}

// nodeWorktreeInspect resolves a cwd to a linked git worktree. A path that is
// not one — an ordinary directory, a bare repo, a clone's canonical checkout,
// or one git cannot read — reports a null worktree.
func nodeWorktreeInspect(ctx context.Context, git relayCommandRunner, cwd string) nodeWorktreeInspectResult {
	output, err := git(ctx, "-C", cwd, "rev-parse", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")
	if err != nil {
		return nodeWorktreeInspectResult{}
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 3 {
		return nodeWorktreeInspectResult{}
	}
	gitDir, common, top := lines[0], lines[1], lines[2]
	if !filepath.IsAbs(common) {
		// git prints a relative common dir against the directory it ran in.
		common = filepath.Join(cwd, common)
	}
	// A linked worktree keeps its git dir under <main>/.git/worktrees; the
	// canonical checkout's git dir is the common dir itself.
	if !strings.Contains(filepath.ToSlash(gitDir), "/.git/worktrees/") {
		return nodeWorktreeInspectResult{}
	}
	worktree := &nodeWorktree{Path: top, Common: common}
	if output, err := git(ctx, "-C", cwd, "branch", "--show-current"); err == nil {
		worktree.Branch = strings.TrimSpace(string(output))
	}
	if output, err := git(ctx, "-C", cwd, "rev-parse", "HEAD"); err == nil {
		worktree.Head = strings.TrimSpace(string(output))
	}
	if output, err := git(ctx, "-C", cwd, "remote", "get-url", "origin"); err == nil {
		worktree.OriginURL = redactNodeOriginURL(strings.TrimSpace(string(output)))
	}
	return nodeWorktreeInspectResult{Worktree: worktree}
}

// redactNodeOriginURL drops URL userinfo, query and fragment so a credential
// or signed token embedded in a remote URL never leaves the node; owner/name
// matching needs only scheme, host and path. scp-style remotes carry no
// secret.
func redactNodeOriginURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery, parsed.ForceQuery = "", false
	parsed.Fragment, parsed.RawFragment = "", ""
	return parsed.String()
}
