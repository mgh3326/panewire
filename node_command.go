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
	nodeCommandWorktreePrepare = "worktree.prepare"
	nodeCommandWorktreeRemove  = "worktree.remove"
	nodeCommandQuotaRefresh    = "quota.refresh"

	// nodeCommandMaxResultBytes keeps a reply under the hub's per-message read
	// limit (hubMaxMessageBytes) with room for the envelope.
	nodeCommandMaxResultBytes = 28 << 10
	nodeCommandMaxArgsBytes   = 16 << 10
	nodeCommandRunTimeout     = 20 * time.Second
	nodeCommandMaxPathBytes   = 4096
	// nodePaneCloseSettle separates the two re-reads pane.close requires
	// before it closes; see nodePaneClose.
	nodePaneCloseSettle = 2 * time.Second
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
	// pane.close only ever closes a finished pane: the expected status must
	// be one of these, and the node re-reads it before closing.
	nodePaneClosableStatus = map[string]bool{"idle": true, "done": true}
)

func knownNodeCommandOp(op string) bool {
	switch op {
	case nodeCommandAgentsList, nodeCommandPaneClose, nodeCommandPaneCapture, nodeCommandWorktreeInspect,
		nodeCommandWorktreePrepare, nodeCommandWorktreeRemove, nodeCommandQuotaRefresh:
		return true
	}
	return false
}

// validNodeCommandError is the closed set a node may answer instead of a
// result. node_lost and node_timeout are hub-local and never on the wire.
func validNodeCommandError(code string) bool {
	switch code {
	case "invalid_args", "herdr_failed", "close_failed", "capture_failed", "jobs_root_unset", "work_root_unset", "refresh_failed", "result_too_large":
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

// nodeCommandArgFields is each op's exact argument set. Every field is
// required: a zero value such as write:false or merged_after:0 is a decision
// the caller must state, never a default for an omitted field.
var nodeCommandArgFields = map[string][]string{
	nodeCommandAgentsList:      {},
	nodeCommandPaneClose:       {"pane_id", "name", "agent_status", "cwd"},
	nodeCommandPaneCapture:     {"pane_id", "name", "cwd", "merged_after", "write"},
	nodeCommandWorktreeInspect: {"cwd"},
	nodeCommandWorktreePrepare: {"origin", "mode", "branch", "sha", "fetch", "dir"},
	nodeCommandWorktreeRemove:  {"cwd", "expect_head", "base", "regenerable"},
	nodeCommandQuotaRefresh:    {"pool"},
}

// nodeCommandHasFields reports whether raw is an object holding exactly the
// named fields, none of them null.
func nodeCommandHasFields(raw json.RawMessage, names []string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || len(fields) != len(names) {
		return false
	}
	for _, name := range names {
		if value, found := fields[name]; !found || isJSONNull(value) {
			return false
		}
	}
	return true
}

// decodeNodeCommandArgs returns the typed, validated arguments for op.
func decodeNodeCommandArgs(op string, raw json.RawMessage) (any, bool) {
	names, known := nodeCommandArgFields[op]
	if !known || len(raw) == 0 || len(raw) > nodeCommandMaxArgsBytes || !nodeCommandHasFields(raw, names) {
		return nil, false
	}
	switch op {
	case nodeCommandAgentsList:
		var args struct{}
		return args, decodeNodeCommandStrict(raw, &args)
	case nodeCommandPaneClose:
		var args nodePaneCloseArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && nodePaneClosableStatus[args.AgentStatus] && validNodeCommandPath(args.Cwd)
		return args, valid
	case nodeCommandPaneCapture:
		var args nodePaneCaptureArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && validNodeCommandPath(args.Cwd) && args.MergedAfter >= 0
		return args, valid
	case nodeCommandWorktreeInspect:
		var args nodeWorktreeInspectArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeCommandPath(args.Cwd)
	case nodeCommandWorktreePrepare:
		var args nodeWorktreePrepareArgs
		if !decodeNodeCommandStrict(raw, &args) {
			return nil, false
		}
		if _, ok := nodeOriginRepo(args.Origin); !ok || !validNodeJobName(args.Dir) {
			return nil, false
		}
		switch args.Mode {
		case "branch":
			// Branch mode tracks origin/<branch> and always fetches the
			// whole remote, so fetch must be stated empty; sha only names
			// the expected tip.
			valid := validNodeRefName(args.Branch) && (args.Sha == "" || nodeShaPattern.MatchString(args.Sha)) && args.Fetch == ""
			return args, valid
		case "detached":
			valid := args.Branch == "" && nodeShaPattern.MatchString(args.Sha) && validNodeFetchRef(args.Fetch)
			return args, valid
		}
		return nil, false
	case nodeCommandWorktreeRemove:
		var args nodeWorktreeRemoveArgs
		valid := decodeNodeCommandStrict(raw, &args) && validNodeCommandPath(args.Cwd) &&
			nodeShaPattern.MatchString(args.ExpectHead) && validNodeRefName(args.Base) && args.Regenerable != nil
		for _, glob := range args.Regenerable {
			valid = valid && validNodeGlob(glob)
		}
		return args, valid
	case nodeCommandQuotaRefresh:
		var args nodeQuotaRefreshArgs
		return args, decodeNodeCommandStrict(raw, &args) && nodeQuotaPoolPattern.MatchString(args.Pool)
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
	case nodeCommandWorktreePrepare:
		var result nodeWorktreePrepareResult
		if !decodeNodeCommandStrict(raw, &result) {
			break
		}
		switch result.Outcome {
		case nodePreparePrepared, nodePrepareReused:
			// Success must name the commit it prepared; an unread HEAD is not success.
			valid = result.Worktree != nil && result.Worktree.Head != ""
		case nodeOutcomeBusy:
			valid = result.RetryAfterS > 0 && result.Worktree == nil
		case nodePrepareBranchElsewhere, nodePrepareShaMissing, nodePrepareOriginMismatch,
			nodeOutcomeDirty, nodeOutcomeGitFailed, nodeOutcomeGitTooOld:
			valid = result.Worktree == nil && result.RetryAfterS == 0
		}
		target = result
	case nodeCommandWorktreeRemove:
		var result nodeWorktreeRemoveResult
		if !decodeNodeCommandStrict(raw, &result) {
			break
		}
		switch result.Outcome {
		case nodeRemoveRemoved:
			valid = result.Worktree != nil && result.RetryAfterS == 0
		case nodeOutcomeBusy:
			valid = result.RetryAfterS > 0 && result.Worktree == nil && result.Files == nil
		case nodeRemoveIgnoredFiles:
			valid = len(result.Files) > 0 && result.Worktree == nil
		case nodeRemoveWorktreeGone, nodeRemoveWorktreeChanged, nodeRemoveUnregistered,
			nodeRemoveLocked, nodeRemoveDetached, nodeRemoveStatusCheckFailed,
			nodeOutcomeDirty, nodeRemovePaneInUse, nodeRemovePaneScanFailed,
			nodeRemoveUnmerged, nodeRemoveOutsideWorkRoot, nodeOutcomeGitFailed,
			nodeOutcomeGitTooOld:
			valid = result.Worktree == nil && result.Files == nil && result.RetryAfterS == 0
		}
		target = result
	case nodeCommandQuotaRefresh:
		var result nodeQuotaRefreshResult
		valid = decodeNodeCommandStrict(raw, &result) && result.Outcome == nodeQuotaRefreshed
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
	case nodeWorktreePrepareArgs:
		return client.nodeWorktreePrepare(ctx, args)
	case nodeWorktreeRemoveArgs:
		return client.nodeWorktreeRemove(ctx, args)
	case nodeQuotaRefreshArgs:
		return client.nodeQuotaRefresh(ctx, args)
	}
	return client.nodeAgentsList(ctx)
}

// nodeHerdrRow is a herdr list row plus the node-local fields pane.close
// compares; StateChangeSeq is nil when herdr does not report it.
type nodeHerdrRow struct {
	nodeAgentRow
	StateChangeSeq *int64 `json:"state_change_seq"`
}

func nodeHerdrRows(output []byte, key string) ([]nodeHerdrRow, error) {
	var envelope struct {
		Result map[string]json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(output, &envelope); err != nil {
		return nil, err
	}
	var rows []nodeHerdrRow
	if err := json.Unmarshal(envelope.Result[key], &rows); err != nil || rows == nil {
		return nil, errors.New("herdr list: missing " + key)
	}
	return rows, nil
}

func nodeAgentRows(rows []nodeHerdrRow) []nodeAgentRow {
	out := make([]nodeAgentRow, len(rows))
	for index, row := range rows {
		out[index] = row.nodeAgentRow
	}
	return out
}

func (client *HubClient) readNodeAgents(ctx context.Context) ([]nodeHerdrRow, error) {
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
	result := nodeAgentsListResult{Agents: nodeAgentRows(agents)}
	if output, err := client.nodeHerdrRunner()(ctx, "pane", "list"); err == nil {
		if rows, err := nodeHerdrRows(output, "panes"); err == nil {
			panes := nodeAgentRows(rows)
			for index := range panes {
				// pane list carries a label, not the agent name.
				panes[index].Name = ""
			}
			result.Panes, result.PanesOK = panes, true
		}
	}
	return result, ""
}

// nodePaneMatch re-reads herdr and classifies the caller's expected pane; on
// a match it also returns the row, for its state_change_seq.
func (client *HubClient) nodePaneMatch(ctx context.Context, paneID, name, status, cwd string, checkStatus bool) (string, nodeHerdrRow, error) {
	agents, err := client.readNodeAgents(ctx)
	if err != nil {
		return "", nodeHerdrRow{}, err
	}
	for _, agent := range agents {
		if agent.PaneID != paneID {
			continue
		}
		if agent.Name != name || agent.Cwd != cwd || (checkStatus && agent.AgentStatus != status) {
			return nodePaneChanged, nodeHerdrRow{}, nil
		}
		return "", agent, nil
	}
	return nodePaneGone, nodeHerdrRow{}, nil
}

// nodePaneClose closes a pane only after two matching re-reads a settle
// interval apart, the second immediately before the close. herdr has no
// conditional close, so a pane can still go back to work between the last
// read and `herdr pane close`; requiring the finished state to hold across
// the interval — with an unchanged state_change_seq where herdr reports one,
// so an idle→working→idle bounce also refuses — leaves only that read-to-
// close gap, which is no wider than the ssh reap's re-read then close.
func (client *HubClient) nodePaneClose(ctx context.Context, args nodePaneCloseArgs) (any, string) {
	outcome, first, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, args.AgentStatus, args.Cwd, true)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCloseResult{Outcome: outcome}, ""
	}
	settle := client.nodeCloseSettle
	if settle == 0 {
		settle = nodePaneCloseSettle
	}
	timer := time.NewTimer(settle)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return nil, "herdr_failed"
	}
	outcome, second, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, args.AgentStatus, args.Cwd, true)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCloseResult{Outcome: outcome}, ""
	}
	if first.StateChangeSeq != nil && second.StateChangeSeq != nil && *first.StateChangeSeq != *second.StateChangeSeq {
		return nodePaneCloseResult{Outcome: nodePaneChanged}, ""
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
	// Every read and write below goes through an os.Root on the jobs
	// directory, so no symlink — at the job dir, report.md, pane-screen.txt
	// or anywhere between — can lead outside it. The job dir itself must be
	// a real directory: a symlinked one is not something the fleet created.
	jobsRoot := filepath.Join(client.jobsInboxRoot, "jobs")
	jobs, err := os.OpenRoot(jobsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nodePaneCaptureResult{Outcome: nodeCaptureNotAJob}, ""
	} else if err != nil {
		return nil, "capture_failed"
	}
	defer jobs.Close()
	info, err := jobs.Lstat(args.Name)
	switch {
	case errors.Is(err, os.ErrNotExist) || (err == nil && !info.IsDir()):
		return nodePaneCaptureResult{Outcome: nodeCaptureNotAJob}, ""
	case err != nil:
		return nil, "capture_failed"
	}
	job, err := jobs.OpenRoot(args.Name)
	if err != nil {
		return nil, "capture_failed"
	}
	defer job.Close()
	dir := filepath.Join(jobsRoot, args.Name)
	report := filepath.Join(dir, "report.md")
	if args.MergedAfter > 0 {
		// Lstat: a symlinked report.md is not the job's own report.
		info, err := job.Lstat("report.md")
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
	outcome, _, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, "", args.Cwd, false)
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
	if err := replaceFileAtomicallyInRoot(job, nodeCaptureScreenTxt, output, 0600); err != nil {
		return nil, "capture_failed"
	}
	return nodePaneCaptureResult{Outcome: nodeCaptureScreen, Saved: screen}, ""
}

// nodeWorktreeInspect resolves a cwd to a linked git worktree. A path that is
// not one — an ordinary directory, a bare repo, a clone's canonical checkout,
// or one git cannot read — reports a null worktree.
func nodeWorktreeInspect(ctx context.Context, git relayCommandRunner, cwd string) nodeWorktreeInspectResult {
	// --path-format=absolute (git 2.31+) makes git resolve the common dir
	// itself: a relative one is relative to different bases depending on
	// git version and on how the worktree's commondir is recorded. An older
	// git rejects the option, which reads as not-a-worktree, never a guess.
	output, err := git(ctx, "-C", cwd, "rev-parse", "--path-format=absolute", "--absolute-git-dir", "--git-common-dir", "--show-toplevel")
	if err != nil {
		return nodeWorktreeInspectResult{}
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) != 3 {
		return nodeWorktreeInspectResult{}
	}
	gitDir, common, top := lines[0], lines[1], lines[2]
	if !filepath.IsAbs(gitDir) || !filepath.IsAbs(common) || !filepath.IsAbs(top) {
		return nodeWorktreeInspectResult{}
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
// or signed token embedded in a remote never leaves the node; owner/name
// matching needs only host and path. An scp-style remote
// ([user[:secret]@]host:path) loses everything up to its last '@' before
// the first '/', which removes the user and any secret beside it.
func redactNodeOriginURL(raw string) string {
	if !strings.Contains(raw, "://") {
		head := raw
		if slash := strings.IndexByte(raw, '/'); slash >= 0 {
			head = raw[:slash]
		}
		if at := strings.LastIndexByte(head, '@'); at >= 0 {
			return raw[at+1:]
		}
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
