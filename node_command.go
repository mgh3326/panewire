package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Node commands (docs/node-commands.md) are operator-authenticated hub→node
// requests that read or act on this node's herdr panes and git worktrees for a
// fleet tool such as mergewake reap, so no tool needs ssh to a session host.
// The op set is closed. pane.close never trusts the caller's snapshot: the
// node re-reads herdr and closes only a pane whose id, name (or explicitly
// permitted unnamed identity), status and cwd still match.
const (
	nodeCommandAgentsList      = "agents.list"
	nodeCommandPaneClose       = "pane.close"
	nodeCommandPaneCapture     = "pane.capture"
	nodeCommandWorktreeInspect = "worktree.inspect"
	nodeCommandWorktreeList    = "worktree.list"
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
	nodePaneCloseSettle = fleet.FailedSpawnCloseSettle
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
	nodeCaptureScreenTxt = fleet.PaneScreenFile
)

var (
	nodeCommandPaneIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	// pane.close never closes a working pane: the expected status must be one
	// of these, and the node re-reads it before closing. blocked is a pane
	// stopped at a prompt; the node closes it only for a reviewer job
	// (nodePaneClose), the pane the hub's finished-reviewer release targets
	// (hub_spawn_reviewer_release.go).
	nodePaneClosableStatus = map[string]bool{"idle": true, "done": true, "blocked": true}
	// nodePaneIdleStatus is the finished subset, for a release that must not
	// close a pane waiting on a prompt.
	nodePaneIdleStatus = map[string]bool{"idle": true, "done": true}
)

func knownNodeCommandOp(op string) bool {
	switch op {
	case nodeCommandAgentsList, nodeCommandPaneClose, nodeCommandPaneCapture, nodeCommandPaneRead,
		nodeCommandWorktreeInspect, nodeCommandWorktreeList, nodeCommandWorktreePrepare, nodeCommandWorktreeRemove,
		nodeCommandQuotaRefresh, nodeCommandJobDone, nodeCommandJobResult, nodeCommandJobMarkLost, nodeCommandPRStatus, nodeCommandJobClose, nodeCommandJobReap,
		nodeCommandCheckoutPull, nodeCommandPaneSweep, nodeCommandPaneCloseOp, nodeCommandPaneCloseStatus, nodeCommandPaneCloseRelease:
		return true
	}
	return false
}

// validNodeCommandError is the closed set a node may answer instead of a
// result. node_lost and node_timeout are hub-local and never on the wire.
func validNodeCommandError(code string) bool {
	switch code {
	case "invalid_args", "herdr_failed", "close_failed", "capture_failed", "jobs_root_unset", "work_root_unset", "refresh_failed", "done_failed", "result_failed", "mark_lost_failed", "reap_failed", "result_too_large", "sweep_failed", "journal_failed", "record_unavailable":
		return true
	}
	return false
}

type nodePaneCloseArgs struct {
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	AgentStatus  string `json:"agent_status"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed,omitempty"`
	// target, when set, is the job attempt a close operation proved from the
	// job's own ledger: its role is authoritative (an empty role is unknown,
	// never looked up under the label) and its herdr session is used for
	// every read and the close. Never on the wire.
	target *nodeCloseTarget
}

type nodePaneCaptureArgs struct {
	PaneID       string `json:"pane_id"`
	Name         string `json:"name"`
	Cwd          string `json:"cwd"`
	AllowUnnamed bool   `json:"allow_unnamed,omitempty"`
	// MergedAfter is a unix second: report.md counts only when its mtime is
	// strictly later. Zero means no report is fresh enough.
	MergedAfter int64 `json:"merged_after"`
	// Write false reports where the screen would be saved without reading
	// the pane or touching the job directory.
	Write bool `json:"write"`
}

type nodeWorktreeInspectArgs struct {
	Cwd          string `json:"cwd"`
	VerifyBranch bool   `json:"verify_branch,omitempty"`
	ExpectedDir  string `json:"expected_dir,omitempty"`
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
	// Doc is the hk screen document key (fleet.PaneScreenDocKey) once a
	// written screen was uploaded; omitted otherwise, so a result without an
	// upload keeps its earlier bytes (hk 3617). It goes on the wire only to a
	// hub that advertised hubFeaturePaneCaptureDoc (nodeCommandWire).
	Doc string `json:"doc,omitempty"`
}

// hubFeaturePaneCaptureDoc: a hub before hk 3617 decodes a pane.capture
// result strictly and refuses the whole result for an unknown doc, so a node
// sends doc only on a connection whose hub advertised this. The upload to hk
// happens either way.
const hubFeaturePaneCaptureDoc = "pane_capture_doc"

type nodeWorktree struct {
	Path      string `json:"path"`
	Branch    string `json:"branch"`
	Head      string `json:"head"`
	OriginURL string `json:"origin_url"`
	Common    string `json:"common"`
}

type nodeWorktreeInspectResult struct {
	Worktree *nodeWorktree `json:"worktree"`
	Check    string        `json:"check,omitempty"`
}

func validNodeJobName(name string) bool {
	if name == "" || len(name) > 128 || name[0] == '-' || name[0] == '.' {
		return false
	}
	return !hasControlByte(name) && !strings.ContainsAny(name, "/\\ ") && !strings.Contains(name, "..")
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
	nodeCommandJobClose:         {"job", "lane", "outcome", "reason", "override"},
	nodeCommandJobReap:          {"job", "lane", "force"},
	nodeCommandPaneSweep:        {"age_s"},
	nodeCommandAgentsList:       {},
	nodeCommandPaneClose:        {"pane_id", "name", "agent_status", "cwd"},
	nodeCommandPaneCapture:      {"pane_id", "name", "cwd", "merged_after", "write"},
	nodeCommandPaneRead:         {"pane_id", "name", "cwd", "tail_lines"},
	nodeCommandJobDone:          {"job", "report"},
	nodeCommandJobResult:        {"job"},
	nodeCommandJobMarkLost:      {"job", "pane_id", "label"},
	nodeCommandWorktreeInspect:  {"cwd"},
	nodeCommandWorktreeList:     {"common"},
	nodeCommandWorktreePrepare:  {"origin", "mode", "branch", "sha", "fetch", "dir"},
	nodeCommandWorktreeRemove:   {"cwd", "expect_head", "base", "regenerable"},
	nodeCommandQuotaRefresh:     {"pool"},
	nodeCommandPRStatus:         {"repo", "pr", "fresh"},
	nodeCommandCheckoutPull:     {"checkout", "rev"},
	nodeCommandPaneCloseOp:      {"request_id", "job", "attempt", "pane_id", "name", "agent_status", "cwd", "allow_unnamed"},
	nodeCommandPaneCloseStatus:  {"request_id", "job"},
	nodeCommandPaneCloseRelease: {"request_id", "job"},
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

func nodeWorktreeInspectFields(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil || fields["cwd"] == nil || isJSONNull(fields["cwd"]) {
		return false
	}
	for key, value := range fields {
		if (key != "cwd" && key != "expected_dir" && key != "verify_branch") || isJSONNull(value) {
			return false
		}
	}
	return true
}

// decodeNodeCommandArgs returns the typed, validated arguments for op, and nil
// when they are refused: a caller can never hold, or log, unvalidated text.
func decodeNodeCommandArgs(op string, raw json.RawMessage) (any, bool) {
	decoded, valid := parseNodeCommandArgs(op, raw)
	if !valid {
		return nil, false
	}
	return decoded, true
}

func parseNodeCommandArgs(op string, raw json.RawMessage) (any, bool) {
	names, known := nodeCommandArgFields[op]
	optionalKind := op == nodeCommandPaneRead && nodeCommandHasFields(raw, append(append([]string(nil), names...), "kind"))
	optionalDetached := op == nodeCommandWorktreeRemove && nodeCommandHasFields(raw, append(append([]string(nil), names...), "detached"))
	optionalUnnamed := (op == nodeCommandPaneClose || op == nodeCommandPaneCapture) && nodeCommandHasFields(raw, append(append([]string(nil), names...), "allow_unnamed"))
	optionalList := op == nodeCommandWorktreeList && nodeWorktreeListFields(raw)
	optionalInspect := op == nodeCommandWorktreeInspect && nodeWorktreeInspectFields(raw)
	optionalEndReason := op == nodeCommandJobMarkLost && nodeCommandHasFields(raw, append(append([]string(nil), names...), "end_reason"))
	if !known || len(raw) == 0 || len(raw) > nodeCommandMaxArgsBytes || (!nodeCommandHasFields(raw, names) && !optionalKind && !optionalDetached && !optionalUnnamed && !optionalList && !optionalInspect && !optionalEndReason) {
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
	case nodeCommandPaneCloseOp:
		var args nodePaneCloseOpArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodePaneCloseOpArgs(args)
	case nodeCommandPaneCloseStatus, nodeCommandPaneCloseRelease:
		var args nodeCloseOpRefArgs
		if !decodeNodeCommandStrict(raw, &args) || !validNodeCloseOpRefArgs(args) {
			return nil, false
		}
		if op == nodeCommandPaneCloseRelease {
			return nodeCloseOpReleaseArgs(args), true
		}
		return args, true
	case nodeCommandPaneCapture:
		var args nodePaneCaptureArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && validNodeCommandPath(args.Cwd) && args.MergedAfter >= 0
		return args, valid
	case nodeCommandPaneRead:
		var args nodePaneReadArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCommandPaneIDPattern.MatchString(args.PaneID) && validNodeJobName(args.Name) && validNodeCommandPath(args.Cwd) && args.TailLines >= 0 && args.TailLines <= nodePaneReadMaxTailLines
		return args, valid
	case nodeCommandJobResult:
		var args nodeJobResultArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeJobName(args.Job)
	case nodeCommandJobMarkLost:
		var args nodeJobMarkLostArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeJobMarkLostArgs(args)
	case nodeCommandJobClose:
		var args hubJobCloseRequest
		if !decodeNodeCommandStrict(raw, &args) {
			return nil, false
		}
		_, err := args.options()
		return args, err == nil
	case nodeCommandJobReap:
		var args nodeJobReapArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeJobReapArgs(args)
	case nodeCommandPaneSweep:
		var args nodePaneSweepArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodePaneSweepArgs(args)
	case nodeCommandJobDone:
		var args nodeJobDoneArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeJobName(args.Job) && validNodeCommandPath(args.Report)
	case nodeCommandWorktreeInspect:
		var args nodeWorktreeInspectArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodeCommandPath(args.Cwd) && (args.ExpectedDir == "" || validNodeJobName(args.ExpectedDir))
	case nodeCommandWorktreeList:
		var args nodeWorktreeListArgs
		valid := decodeNodeCommandStrict(raw, &args) && validNodeCommandPath(args.Common) && (args.Limit == nil || (*args.Limit >= 1 && *args.Limit <= 100))
		return args, valid
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
	case nodeCommandPRStatus:
		var args nodePRStatusArgs
		return args, decodeNodeCommandStrict(raw, &args) && validNodePRStatusArgs(args)
	case nodeCommandCheckoutPull:
		var args nodeCheckoutPullArgs
		valid := decodeNodeCommandStrict(raw, &args) && nodeCheckoutNamePattern.MatchString(args.Checkout) &&
			(args.Rev == "" || nodeCheckoutRevPattern.MatchString(args.Rev))
		return args, valid
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
	case nodeCommandPaneCloseOp:
		var result nodePaneCloseOpResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodePaneCloseOpResult(result)
		target = result
	case nodeCommandPaneCloseStatus, nodeCommandPaneCloseRelease:
		var result nodeCloseOpStatusResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodeCloseOpStatusResult(result)
		target = result
	case nodeCommandPaneCapture:
		var result nodePaneCaptureResult
		switch {
		case !decodeNodeCommandStrict(raw, &result):
		case result.Outcome == nodeCaptureReport || result.Outcome == nodeCaptureScreen || result.Outcome == nodeCaptureWould:
			valid = validNodeCommandPath(result.Saved) && (result.Doc == "" || validNodeCaptureDoc(result))
		case result.Outcome == nodeCaptureNotAJob || result.Outcome == nodePaneChanged || result.Outcome == nodePaneGone:
			valid = result.Saved == "" && result.Doc == ""
		}
		target = result
	case nodeCommandPaneRead:
		var result PaneReadResult
		valid = decodeNodeCommandStrict(raw, &result) && validPaneReadResult(result)
		target = result
	case nodeCommandJobResult:
		var result nodeJobResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodeJobResult(result)
		target = result
	case nodeCommandJobMarkLost:
		var result nodeJobMarkLostResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodeJobMarkLostResult(result)
		target = result
	case nodeCommandJobDone:
		var result JobDoneResult
		valid = decodeNodeCommandStrict(raw, &result) && validJobDoneResult(result)
		target = result
	case nodeCommandWorktreeInspect:
		var result nodeWorktreeInspectResult
		valid = decodeNodeCommandStrict(raw, &result)
		target = result
	case nodeCommandWorktreeList:
		var result nodeWorktreeListResult
		valid = decodeNodeCommandStrict(raw, &result) && result.Worktrees != nil && result.Total >= len(result.Worktrees)
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
			nodeOutcomeDirty, nodeRemoveLocked, nodeOutcomeGitFailed, nodeOutcomeGitTooOld,
			nodeOutcomeCancelled:
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
			valid = result.Worktree != nil && result.Worktree.Head != "" && result.RetryAfterS == 0 && result.Files == nil
		case nodeOutcomeBusy:
			valid = result.RetryAfterS > 0 && result.Worktree == nil && result.Files == nil
		case nodeRemoveIgnoredFiles:
			valid = len(result.Files) > 0 && result.Worktree == nil && result.RetryAfterS == 0
		case nodeRemoveWorktreeGone, nodeRemoveWorktreeChanged, nodeRemoveUnregistered,
			nodeRemoveLocked, nodeRemoveDetached, nodeRemoveNotDetached, nodeRemoveStatusCheckFailed,
			nodeOutcomeDirty, nodeRemovePaneInUse, nodeRemovePaneScanFailed,
			nodeRemoveUnmerged, nodeRemoveOutsideWorkRoot, nodeOutcomeGitFailed,
			nodeOutcomeGitTooOld, nodeOutcomeCancelled:
			valid = result.Worktree == nil && result.Files == nil && result.RetryAfterS == 0
		}
		target = result
	case nodeCommandQuotaRefresh:
		var result nodeQuotaRefreshResult
		valid = decodeNodeCommandStrict(raw, &result) && result.Outcome == nodeQuotaRefreshed
		target = result
	case nodeCommandJobClose:
		var result hubJobCloseResult
		valid = decodeNodeCommandStrict(raw, &result) && ((result.Refusal != "" && result.Failure == "" && result.Name == "") || (result.Failure == "close_failed" && result.Refusal == "" && result.Name == "") || (result.Refusal == "" && result.Failure == "" && result.Name != "" && result.Record.Kind == jobCloseKind))
		target = result
	case nodeCommandJobReap:
		var result nodeJobReapResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodeJobReapResult(result)
		target = result
	case nodeCommandPaneSweep:
		var result nodePaneSweepResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodePaneSweepResult(result)
		target = result
	case nodeCommandPRStatus:
		var result nodePRStatusResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodePRStatusResult(result)
		target = result
	case nodeCommandCheckoutPull:
		var result nodeCheckoutPullResult
		valid = decodeNodeCommandStrict(raw, &result) && validNodeCheckoutPullResult(result)
		target = result
	}
	if !valid {
		return nil, false
	}
	encoded, err := marshalNodeCommandJSON(target)
	return encoded, err == nil
}

func defaultNodeCommandRunner(name string) relayCommandRunner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		if name == "git" {
			env := make([]string, 0, len(os.Environ())+1)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "LC_ALL=") {
					env = append(env, value)
				}
			}
			cmd.Env = append(env, "LC_ALL=C")
		}
		cmd.WaitDelay = 100 * time.Millisecond
		out, err := cmd.Output()
		if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
			return out, nil
		}
		return out, err
	}
}

// nodeHerdrRunner runs herdr in the session the node spawns into (spawn.json
// herdr_session, else "default"), so pane.read, pane.close and agents.list see
// the panes the node launched. A plain herdr call would read the daemon's own
// session and report a landed pane in another session as gone (hk 2966).
func (client *HubClient) nodeHerdrRunner() relayCommandRunner {
	return client.nodeHerdrRunnerFor("", "")
}

func (client *HubClient) nodeHerdrRunnerForSession(session string) relayCommandRunner {
	if client != nil && client.nodeHerdr != nil {
		return client.nodeHerdr
	}
	return func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, herdrExecutable(), args...)
		cmd.Env = hubSpawnEnvironment(os.Environ(), session)
		return cmd.Output()
	}
}

// nodeHerdrRunnerFor returns a herdr runner for the specific job and/or pane,
// using that job's recorded herdr_session, else the node's configured session.
func (client *HubClient) nodeHerdrRunnerFor(jobID, pane string) relayCommandRunner {
	if client != nil && client.nodeHerdr != nil {
		return client.nodeHerdr
	}
	session := client.herdrSessionFor(jobID, pane)
	return client.nodeHerdrRunnerForSession(session)
}

// herdrExecutable returns the executable to invoke for herdr.
func herdrExecutable() string {
	if nodeHerdrExecutable != "" {
		return nodeHerdrExecutable
	}
	if bin := os.Getenv("HERDR_BIN"); bin != "" {
		return bin
	}
	return "herdr"
}

// herdrSessionFor returns that pane or job's recorded herdr_session, else the node's
// configured herdr_session, never silently "default".
func (client *HubClient) herdrSessionFor(jobID, pane string) string {
	if client == nil {
		return firstNonempty(os.Getenv("HERDR_SESSION"), hubSpawnDefaultSession)
	}
	setPaneSessionResolver(func(p string) string {
		return client.herdrSessionFor("", p)
	})
	if pane != "" {
		if session := client.nodeJobHerdrSessionByPane(pane); session != "" {
			return session
		}
	}
	if jobID != "" {
		if session, err := client.nodeJobHerdrSession(jobID); err == nil && session != "" {
			return session
		}
	}
	return client.nodeSpawnSession()
}

// nodePaneBinding is the verified binding of a pane to a job and herdr session.
type nodePaneBinding struct {
	jobID   string
	session string
}

// nodeJobHerdrSessionByPane searches this node's jobs for the one bound to
// pane and returns its recorded herdr_session. It reads the inbox index, which
// is refreshed from each job's events directory on every call, so rebound
// panes across jobs and sessions are observed at once (docs/job-event-index.md).
func (client *HubClient) nodeJobHerdrSessionByPane(pane string) string {
	if pane == "" || client == nil || client.jobsInboxRoot == "" {
		return ""
	}
	binding, found := nodeJobHerdrBindingByPaneFromRoot(client.jobsInboxRoot, pane)
	if !found {
		return ""
	}
	return binding.session
}

// nodeJobHerdrBindingByPaneFromRoot inspects the jobs under inboxRoot to find
// the binding event for this pane, preferring live (un-ended) jobs and the newest binding event.
func nodeJobHerdrBindingByPaneFromRoot(inboxRoot, pane string) (nodePaneBinding, bool) {
	if pane == "" || inboxRoot == "" {
		return nodePaneBinding{}, false
	}
	type paneCandidate struct {
		jobID     string
		session   string
		live      bool
		createdAt string
		seq       int64
	}
	var best paneCandidate
	found := false
	err := hubIndexFor(inboxRoot).visitJobs(nil, true, func(job *hubIndexedJob) bool {
		// The newest binding event of the job that names this pane.
		var match *hubPaneBindingFact
		for i := range job.bindings {
			if job.bindings[i].pane == pane {
				match = &job.bindings[i]
			}
		}
		if match == nil {
			return true
		}
		cand := paneCandidate{jobID: job.id, session: match.session, live: !job.ended, createdAt: match.createdAt, seq: match.seq}
		if !found {
			best = cand
			found = true
		} else if (!best.live && cand.live) ||
			(best.live == cand.live && (cand.createdAt > best.createdAt || (cand.createdAt == best.createdAt && cand.seq > best.seq))) {
			best = cand
		}
		return true
	})
	if err != nil || !found {
		return nodePaneBinding{}, false
	}
	return nodePaneBinding{jobID: best.jobID, session: best.session}, true
}

// nodeJobHerdrSessionByPaneFromRoot inspects job directories under inboxRoot to find
// the session recorded on the event binding this pane, preferring live (un-ended)
// jobs and the newest binding event.
func nodeJobHerdrSessionByPaneFromRoot(inboxRoot, pane string) string {
	if b, ok := nodeJobHerdrBindingByPaneFromRoot(inboxRoot, pane); ok {
		return b.session
	}
	return ""
}

// nodeHerdrExecutable is the herdr binary node commands run; tests point it at
// a fake so they never reach a real herdr on the host. When empty, HERDR_BIN
// or "herdr" is used.
var nodeHerdrExecutable string

// nodeSpawnSession is the herdr session spawns on this node use.
func (client *HubClient) nodeSpawnSession() string {
	if config, _, err := client.resolveHubSpawnConfig(); err == nil && config.HERDRSession != "" {
		return config.HERDRSession
	}
	return hubSpawnDefaultSession
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
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
	Detail    string          `json:"detail,omitempty"`
}

// nodeCommandRunBudget is the run deadline for op: a checkout pull fetches and
// may run an install, so it gets nodeCheckoutPullRunTimeout.
func nodeCommandRunBudget(op string) time.Duration {
	if op == nodeCommandCheckoutPull {
		return nodeCheckoutPullRunTimeout
	}
	return nodeCommandRunTimeout
}

func (client *HubClient) handleNodeCommand(ctx context.Context, peer *hubClientConnection, message hubOutboundMessage) {
	// A command such as job.done uploads a result; a stop waits for it.
	endWork, _ := client.beginWork(false)
	defer endWork()
	runContext, cancel := context.WithTimeout(ctx, nodeCommandRunBudget(message.CommandOp))
	defer cancel()
	reply := nodeCommandReply{Type: "node.command.result", RequestID: message.RequestID, Op: message.CommandOp}
	result, code := client.runNodeCommand(runContext, message.CommandOp, message.CommandArgs)
	result = peer.nodeCommandWire(result)
	if nodeCommandAudited(message.CommandOp) {
		decoded, _ := decodeNodeCommandArgs(message.CommandOp, message.CommandArgs)
		logNodeReapCommand(message.CommandOp, decoded, result, code)
	}
	if code == "" {
		encoded, err := marshalNodeCommandJSON(result)
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
	if code == "done_failed" {
		reply.Detail, _ = result.(string)
	}
	if message.CommandOp == nodeCommandJobResult {
		_ = peer.writeNodeResult(ctx, reply)
	} else {
		_ = peer.write(ctx, reply)
	}
}

// nodeCommandWire is a node command result's wire form on this connection.
// It is shaped here, at reply time, because the command ran without knowing
// which hub it answers: pane.capture's doc reaches only a hub that advertised
// hubFeaturePaneCaptureDoc.
func (peer *hubClientConnection) nodeCommandWire(result any) any {
	if capture, ok := result.(nodePaneCaptureResult); ok && !peer.acceptsPaneCaptureDoc.Load() {
		capture.Doc = ""
		return capture
	}
	return result
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
	case nodePaneCloseOpArgs:
		// The same one-at-a-time lock as pane.close: the journal and the
		// close it records must not interleave with another acting command.
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneCloseOp(ctx, args)
	case nodeCloseOpRefArgs:
		// Under the acting lock too: a status read never sees a journal
		// state its writer has not yet made durable.
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneCloseStatus(args)
	case nodeCloseOpReleaseArgs:
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneCloseRelease(nodeCloseOpRefArgs(args))
	case nodePaneCaptureArgs:
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodePaneCapture(ctx, args)
	case nodePaneReadArgs:
		return client.nodePaneRead(ctx, args)
	case nodeJobResultArgs:
		return client.nodeJobResult(args)
	case nodeJobMarkLostArgs:
		// One acting command at a time per node: the herdr re-check and the
		// hk write must not interleave with a pane close.
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodeJobMarkLost(ctx, args)
	case hubJobCloseRequest:
		return client.nodeJobClose(args)
	case nodeJobReapArgs:
		// A job close acts on herdr, so it takes the same one-at-a-time lock
		// pane.close and job.done take.
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodeJobReap(ctx, args)
	case nodePaneSweepArgs:
		// No nodeCommandMu here: each close takes it through the sweeper's
		// Exclusive, as the local pass does (node_pane_sweep.go).
		return client.nodePaneSweep(ctx, args)
	case nodeJobDoneArgs:
		client.nodeCommandMu.Lock()
		defer client.nodeCommandMu.Unlock()
		return client.nodeJobDone(ctx, args)
	case nodeWorktreeInspectArgs:
		result := nodeWorktreeInspect(ctx, client.nodeGitRunner(), args.Cwd)
		if args.ExpectedDir != "" {
			root, err := client.nodeWorkRoot()
			if err != nil || result.Worktree == nil || !nodeSamePath(result.Worktree.Path, filepath.Join(root, args.ExpectedDir)) {
				return nodeWorktreeInspectResult{Check: "mismatch"}, ""
			}
		}
		if args.VerifyBranch {
			result.Check = nodeWorktreeVerifyReadOnly(ctx, client.nodeGitRunner(), result.Worktree)
		}
		return result, ""
	case nodeWorktreeListArgs:
		return nodeWorktreeList(ctx, client.nodeGitRunner(), args)
	case nodeWorktreePrepareArgs:
		return client.nodeWorktreePrepare(ctx, args)
	case nodeWorktreeRemoveArgs:
		return client.nodeWorktreeRemove(ctx, args)
	case nodeQuotaRefreshArgs:
		return client.nodeQuotaRefresh(ctx, args)
	case nodePRStatusArgs:
		// Read-only: no acting-command lock, so a status read never queues
		// behind a pane close.
		return client.nodePRStatus(ctx, args)
	case nodeCheckoutPullArgs:
		// Serialized by the per-clone git-op lock, not nodeCommandMu, so a
		// slow install never queues a pane close behind it.
		return client.nodeCheckoutPull(ctx, args)
	}
	return client.nodeAgentsList(ctx)
}

// nodeHerdrRow is a herdr list row plus the node-local fields pane.close
// compares and pane.read reports; each is nil when herdr does not report it.
type nodeHerdrRow struct {
	nodeAgentRow
	StateChangeSeq *int64 `json:"state_change_seq"`
	Focused        *bool  `json:"focused"`
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

func (client *HubClient) readNodeAgentsInSession(ctx context.Context, session string) ([]nodeHerdrRow, error) {
	output, err := client.nodeHerdrRunnerForSession(session)(ctx, "agent", "list")
	if err != nil {
		return nil, err
	}
	return nodeHerdrRows(output, "agents")
}

func (client *HubClient) readNodeAgents(ctx context.Context) ([]nodeHerdrRow, error) {
	return client.readNodeAgentsInSession(ctx, client.nodeSpawnSession())
}

func (client *HubClient) nodeAgentsListInSession(ctx context.Context, session string) (nodeAgentsListResult, string) {
	agents, err := client.readNodeAgentsInSession(ctx, session)
	if err != nil {
		return nodeAgentsListResult{}, "herdr_failed"
	}
	result := nodeAgentsListResult{Agents: nodeAgentRows(agents)}
	if output, err := client.nodeHerdrRunnerForSession(session)(ctx, "pane", "list"); err == nil {
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

func (client *HubClient) nodeAgentsList(ctx context.Context) (any, string) {
	return client.nodeAgentsListInSession(ctx, client.nodeSpawnSession())
}

// nodePaneMatch re-reads herdr and classifies the caller's expected pane; on
// a match it also returns the row, for its state_change_seq. Names must match
// unless the caller explicitly opts into the unnamed-pane reclaim path.
func (client *HubClient) nodePaneMatch(ctx context.Context, paneID, name, status, cwd string, checkStatus bool) (string, nodeHerdrRow, error) {
	return client.nodePaneMatchWithUnnamed(ctx, paneID, name, status, cwd, checkStatus, false)
}

// nodePaneMatchWithUnnamed optionally permits an unlabelled row only when its
// pane id and cwd still match. The hub uses this for a failed launch whose
// created pane never acquired the agent label; every other caller remains
// label-strict through nodePaneMatch.
func (client *HubClient) nodePaneMatchWithUnnamed(ctx context.Context, paneID, name, status, cwd string, checkStatus, allowUnnamed bool) (string, nodeHerdrRow, error) {
	return client.nodePaneMatchInSession(ctx, client.herdrSessionFor(name, paneID), paneID, name, status, cwd, checkStatus, allowUnnamed)
}

// nodePaneMatchInSession is nodePaneMatchWithUnnamed in a given herdr session.
func (client *HubClient) nodePaneMatchInSession(ctx context.Context, session, paneID, name, status, cwd string, checkStatus, allowUnnamed bool) (string, nodeHerdrRow, error) {
	agents, err := client.readNodeAgentsInSession(ctx, session)
	if err != nil {
		return "", nodeHerdrRow{}, err
	}
	for _, agent := range agents {
		if agent.PaneID != paneID {
			continue
		}
		if (agent.Name != name && !(allowUnnamed && agent.Name == "")) || agent.Cwd != cwd || (checkStatus && agent.AgentStatus != status) {
			return nodePaneChanged, nodeHerdrRow{}, nil
		}
		return "", agent, nil
	}
	return nodePaneGone, nodeHerdrRow{}, nil
}

// nodePaneClose closes a pane only after two matching re-reads a settle
// interval apart, each followed by a proven-empty visible composer read.
// The second composer read is immediately before the close. herdr has no
// conditional close, so a pane can still go back to work between the last
// read and `herdr pane close`; requiring the finished state to hold across
// the interval — with an unchanged state_change_seq where herdr reports one,
// so an idle→working→idle bounce also refuses — leaves only that read-to-
// close gap, which is no wider than the ssh reap's re-read then close.
func (client *HubClient) nodePaneClose(ctx context.Context, args nodePaneCloseArgs) (any, string) {
	result, code := client.nodePaneCloseWith(ctx, args, nil)
	if code != "" {
		return nil, code
	}
	return result, ""
}

// nodePaneCloseWith is nodePaneClose with beforeClose run after the first
// guarded look and the settle, before the final look and `herdr pane close`;
// an error from it leaves the pane open and answers close_failed. A close operation records its
// closing state there (node_pane_close_op.go).
func (client *HubClient) nodePaneCloseWith(ctx context.Context, args nodePaneCloseArgs, beforeClose func() error) (nodePaneCloseResult, string) {
	// A blocked pane may be a worker waiting on an ESC answer, so the node
	// closes one only when the job's own ledger names the reviewer role; the
	// operator-credential command path cannot widen that (hk 3315).
	session := client.herdrSessionFor(args.Name, args.PaneID)
	if args.target != nil {
		// The receipt's session; a close op never binds a receipt that
		// names none (closeTargetBound).
		session = args.target.session
	}
	if args.AgentStatus == "blocked" {
		var role string
		var err error
		if args.target != nil {
			role = args.target.role
		} else {
			role, err = client.nodeJobRole(args.Name)
		}
		if err != nil || role != "reviewer" {
			return nodePaneCloseResult{Outcome: nodePaneChanged}, ""
		}
	}
	outcome, first, err := client.nodePaneMatchInSession(ctx, session, args.PaneID, args.Name, args.AgentStatus, args.Cwd, true, args.AllowUnnamed)
	if err != nil {
		return nodePaneCloseResult{}, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCloseResult{Outcome: outcome}, ""
	}
	composerEmpty := func(row nodeHerdrRow) bool {
		screen, err := client.nodeHerdrRunnerForSession(session)(ctx, "pane", "read", args.PaneID, "--source", "visible", "--format", "ansi")
		return err == nil && nodeComposerState(row.Agent, string(screen)) == nodeComposerEmpty
	}
	if !composerEmpty(first) {
		return nodePaneCloseResult{Outcome: nodePaneChanged}, ""
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
		return nodePaneCloseResult{}, "herdr_failed"
	}
	// The hook runs before the final look, so whatever it costs (a durable
	// journal write) never widens the gap between that look and the close.
	if beforeClose != nil {
		if err := beforeClose(); err != nil {
			var stop nodeCloseHookStop
			if errors.As(err, &stop) {
				if stop.code != "" {
					return nodePaneCloseResult{}, stop.code
				}
				return nodePaneCloseResult{Outcome: stop.outcome}, ""
			}
			return nodePaneCloseResult{}, "journal_failed"
		}
	}
	outcome, second, err := client.nodePaneMatchInSession(ctx, session, args.PaneID, args.Name, args.AgentStatus, args.Cwd, true, args.AllowUnnamed)
	if err != nil {
		return nodePaneCloseResult{}, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCloseResult{Outcome: outcome}, ""
	}
	if first.StateChangeSeq != nil && second.StateChangeSeq != nil && *first.StateChangeSeq != *second.StateChangeSeq {
		return nodePaneCloseResult{Outcome: nodePaneChanged}, ""
	}
	if !composerEmpty(second) {
		return nodePaneCloseResult{Outcome: nodePaneChanged}, ""
	}
	if _, err := client.nodeHerdrRunnerForSession(session)(ctx, "pane", "close", args.PaneID); err != nil {
		return nodePaneCloseResult{}, "close_failed"
	}
	return nodePaneCloseResult{Outcome: nodePaneClosed}, ""
}

// nodePaneCapture saves a pane's output into its job directory before a close,
// with the same rules mergewake reap applies over ssh: the job directory must
// already exist (proof the fleet spawned the pane), and report.md wins only
// when it is newer than the merge.
func (client *HubClient) nodePaneCapture(ctx context.Context, args nodePaneCaptureArgs) (any, string) {
	// Every read and write below goes through an os.Root on the job directory,
	// so no symlink — at the job dir, report.md, pane-screen.txt or anywhere
	// between — can lead outside it.
	job, dir, found, err := client.openNodeJobDir(args.Name)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "capture_failed"
	case !found:
		return nodePaneCaptureResult{Outcome: nodeCaptureNotAJob}, ""
	}
	defer job.Close()
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
	outcome, _, err := client.nodePaneMatchWithUnnamed(ctx, args.PaneID, args.Name, "", args.Cwd, false, args.AllowUnnamed)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return nodePaneCaptureResult{Outcome: outcome}, ""
	}
	output, err := client.nodeHerdrRunnerFor(args.Name, args.PaneID)(ctx, "pane", "read", args.PaneID)
	if err != nil {
		return nil, "capture_failed"
	}
	if err := replaceFileAtomicallyInRoot(job, nodeCaptureScreenTxt, output, 0600); err != nil {
		return nil, "capture_failed"
	}
	// The saved screen also goes to hk (hk 3617), under what is left of the
	// command's bound. A failed upload is one warning and an empty doc: the
	// screen is saved, so the outcome stays screen.
	doc, err := fleet.UploadPaneScreen(ctx, args.Name, nodeJobTask(job), output)
	if err != nil {
		slog.Warn("pane screen upload failed; local screen retained", "job", args.Name, "err", err)
	}
	return nodePaneCaptureResult{Outcome: nodeCaptureScreen, Saved: screen, Doc: doc}, ""
}

// validNodeCaptureDoc reports whether a pane.capture result's doc is the
// screen document of the job its saved screen belongs to: only a written
// screen is uploaded.
func validNodeCaptureDoc(result nodePaneCaptureResult) bool {
	job := filepath.Base(filepath.Dir(result.Saved))
	return result.Outcome == nodeCaptureScreen && filepath.Base(result.Saved) == nodeCaptureScreenTxt &&
		validNodeJobName(job) && result.Doc == fleet.PaneScreenDocKey(job)
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

// nodeWorktreeVerifyReadOnly checks the checkout without changing it or its refs.
func nodeWorktreeVerifyReadOnly(ctx context.Context, git relayCommandRunner, tree *nodeWorktree) string {
	if tree == nil || !validNodeRefName(tree.Branch) || !nodeShaPattern.MatchString(tree.Head) {
		return "unreadable"
	}
	entries, err := nodeListWorktrees(ctx, git, tree.Common)
	if err != nil {
		return "unreadable"
	}
	found := false
	for _, entry := range entries {
		if nodeSamePath(entry.Path, tree.Path) {
			found = true
			if entry.Locked {
				return "locked"
			}
		}
	}
	if !found {
		return "unreadable"
	}
	status, err := git(ctx, "--no-optional-locks", "-C", tree.Path, "status", "--porcelain")
	if err != nil {
		return "unreadable"
	}
	if strings.TrimSpace(string(status)) != "" {
		return "dirty"
	}
	out, err := git(ctx, "-C", tree.Path, "ls-remote", "--heads", "origin", "refs/heads/"+tree.Branch)
	if err != nil {
		return "unreadable"
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 || fields[1] != "refs/heads/"+tree.Branch || !nodeShaPattern.MatchString(fields[0]) {
		return "unpushed"
	}
	if !strings.EqualFold(fields[0], tree.Head) {
		if _, err := git(ctx, "--no-optional-locks", "-C", tree.Path, "merge-base", "--is-ancestor", tree.Head, fields[0]); err != nil {
			return "unpushed"
		}
	}
	return "ok"
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
