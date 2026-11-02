package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// pane.read and job.done are the two node commands mergewake reap needs for a
// job-bound lead: the composer guard before a close and `runtug done` after it
// (docs/node-commands.md). The hub admits them for a scoped service only.
const (
	nodeCommandPaneRead = "pane.read"
	nodeCommandJobDone  = "job.done"

	nodePaneReadOK = "read"

	nodeComposerEmpty   = "empty"
	nodeComposerHasText = "has_text"
	nodeComposerUnknown = "unknown"

	nodePaneReadMaxTailLines = 200
	// nodePaneReadMaxTailBytes keeps every tail inside
	// nodeCommandMaxResultBytes even when each byte JSON-escapes to six.
	nodePaneReadMaxTailBytes = 4 << 10
	nodePaneReadMaxStatus    = 64

	nodeJobDoneOK          = "done"
	nodeJobAlreadyDone     = "already-done"
	nodeJobReportInvalid   = "report-invalid"
	nodeJobDoneMaxLine     = 1 << 10
	nodeJobDoneMarker      = ".node-job-done"
	nodeJobReportFile      = "report.md"
	nodeRuntugDuplicateTag = " duplicate=yes"
)

type nodePaneReadArgs struct {
	PaneID string `json:"pane_id"`
	Name   string `json:"name"`
	Cwd    string `json:"cwd"`
	// TailLines is how many trailing screen lines to return; zero returns none.
	TailLines int `json:"tail_lines"`
}

type nodeJobDoneArgs struct {
	Job    string `json:"job"`
	Report string `json:"report"`
}

// PaneReadResult is pane.read's answer. Outcome is "read", "pane-changed" or
// "pane-gone"; every other field is meaningful only for "read". Composer is
// "empty", "has_text" or "unknown", and only "empty" means nothing is unsent.
type PaneReadResult struct {
	Outcome     string `json:"outcome"`
	AgentStatus string `json:"agent_status,omitempty"`
	Focused     bool   `json:"focused"`
	Composer    string `json:"composer,omitempty"`
	ScreenTail  string `json:"screen_tail,omitempty"`
}

// JobDoneResult is job.done's answer. Outcome is "done", "already-done",
// "not-a-job" or "report-invalid"; Line is runtug's result line, and is empty
// for the two refusals.
type JobDoneResult struct {
	Outcome string `json:"outcome"`
	Line    string `json:"line,omitempty"`
}

// nodeCommandReapOnly names the ops the hub admits for a scoped service alone,
// never the operator credential.
func nodeCommandReapOnly(op string) bool {
	return op == nodeCommandPaneRead || op == nodeCommandJobDone
}

func validPaneReadResult(result PaneReadResult) bool {
	switch result.Outcome {
	case nodePaneReadOK:
		composer := result.Composer == nodeComposerEmpty || result.Composer == nodeComposerHasText || result.Composer == nodeComposerUnknown
		return composer && result.AgentStatus != "" && len(result.AgentStatus) <= nodePaneReadMaxStatus && len(result.ScreenTail) <= nodePaneReadMaxTailBytes
	case nodePaneChanged, nodePaneGone:
		return result == PaneReadResult{Outcome: result.Outcome}
	}
	return false
}

func validJobDoneResult(result JobDoneResult) bool {
	switch result.Outcome {
	case nodeJobDoneOK, nodeJobAlreadyDone:
		return len(result.Line) <= nodeJobDoneMaxLine
	case nodeCaptureNotAJob, nodeJobReportInvalid:
		return result.Line == ""
	}
	return false
}

// nodeComposerState classifies a pane's input line from its visible screen.
// Anything but a composer located and seen empty is never "empty", because
// that answer lets a caller close the pane: a dim placeholder or a prompt
// suggestion reads as text, which keeps the pane.
func nodeComposerState(harness, screen string) string {
	screen = strings.ReplaceAll(screen, "\r", "")
	if strings.EqualFold(harness, "codex") {
		return nodeCodexComposerState(screen)
	}
	if strings.EqualFold(harness, "claude") {
		lines := strings.Split(screen, "\n")
		var dividers []int
		for i, line := range lines {
			if strings.Contains(line, "─") && strings.Trim(line, " \t─") == "" {
				dividers = append(dividers, i)
			}
		}
		if len(dividers) < 2 {
			return nodeComposerUnknown
		}
		for i, line := range lines[dividers[len(dividers)-2]+1 : dividers[len(dividers)-1]] {
			line = strings.TrimSpace(line)
			if i == 0 {
				line = strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, "❯"), ">"))
			}
			if line != "" {
				return nodeComposerHasText
			}
		}
		return nodeComposerEmpty
	}
	content, located := relayComposer(harness, screen)
	switch {
	case !located:
		return nodeComposerUnknown
	case content != "":
		return nodeComposerHasText
	}
	return nodeComposerEmpty
}

// nodeCodexComposerState follows mergewake's rule for codex, which draws no
// dividers: the input starts on the last › line and runs, wrapped or
// multi-line, until the first blank line above the footer.
func nodeCodexComposerState(screen string) string {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		rest, found := strings.CutPrefix(strings.TrimSpace(lines[index]), "›")
		if !found {
			continue
		}
		if strings.TrimSpace(rest) != "" {
			return nodeComposerHasText
		}
		for _, next := range lines[index+1:] {
			if strings.TrimSpace(next) == "" {
				break
			}
			return nodeComposerHasText
		}
		return nodeComposerEmpty
	}
	return nodeComposerUnknown
}

// nodeScreenTail returns the last lines of screen within the tail byte bound.
func nodeScreenTail(screen string, lines int) string {
	if lines <= 0 {
		return ""
	}
	rows := strings.Split(strings.TrimRight(screen, "\n"), "\n")
	if len(rows) > lines {
		rows = rows[len(rows)-lines:]
	}
	return utf8SuffixWithin(strings.Join(rows, "\n"), nodePaneReadMaxTailBytes)
}

// nodePaneRead reports one pane. Matching name and cwd as well as the id is
// what confines it to this node: pane ids are host-local, so an id meant for
// another node can name an unrelated pane here, and that pane's data must not
// come back.
func (client *HubClient) nodePaneRead(ctx context.Context, args nodePaneReadArgs) (any, string) {
	outcome, row, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, "", args.Cwd, false)
	if err != nil {
		return nil, "herdr_failed"
	}
	if outcome != "" {
		return PaneReadResult{Outcome: outcome}, ""
	}
	// A herdr that does not report focus cannot answer the question, and a
	// missing flag must not read as "the operator is not looking at it".
	if row.Focused == nil {
		return nil, "herdr_failed"
	}
	result := PaneReadResult{Outcome: nodePaneReadOK, AgentStatus: row.AgentStatus, Focused: *row.Focused, Composer: nodeComposerUnknown}
	if result.AgentStatus == "" {
		result.AgentStatus = "unknown"
	}
	result.AgentStatus = utf8PrefixWithin(result.AgentStatus, nodePaneReadMaxStatus)
	output, err := client.nodeHerdrRunner()(ctx, "pane", "read", args.PaneID, "--source", "visible")
	if err != nil {
		return result, ""
	}
	screen := string(output)
	result.Composer = nodeComposerState(row.Agent, screen)
	result.ScreenTail = nodeScreenTail(screen, args.TailLines)
	return result, ""
}

// nodeJobDone runs `runtug done` once for a job whose directory exists here.
// runtug uploads the report it is given, so the report can only be one of the
// two files reap saves in that job's directory; any other path would let the
// caller read arbitrary node files back out of the document store.
func (client *HubClient) nodeJobDone(ctx context.Context, args nodeJobDoneArgs) (any, string) {
	job, dir, found, err := client.openNodeJobDir(args.Job)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if err != nil {
		return nil, "done_failed"
	}
	if !found {
		return JobDoneResult{Outcome: nodeCaptureNotAJob}, ""
	}
	defer job.Close()
	name := filepath.Base(args.Report)
	if args.Report != filepath.Join(dir, name) || (name != nodeJobReportFile && name != nodeCaptureScreenTxt) {
		return JobDoneResult{Outcome: nodeJobReportInvalid}, ""
	}
	if info, err := job.Lstat(name); err != nil || !info.Mode().IsRegular() {
		return JobDoneResult{Outcome: nodeJobReportInvalid}, ""
	}
	report, err := job.ReadFile(name)
	if err != nil {
		return nil, "done_failed"
	}
	sum := sha256.Sum256(report)
	digest := hex.EncodeToString(sum[:])
	if line, recorded := readNodeJobDoneMarker(job, digest); recorded {
		return JobDoneResult{Outcome: nodeJobAlreadyDone, Line: line}, ""
	}
	output, err := client.nodeRuntugRunner()(ctx, "done", args.Job, "--report", args.Report)
	if err != nil {
		return nil, "done_failed"
	}
	line := nodeJobDoneLine(output)
	result := JobDoneResult{Outcome: nodeJobDoneOK, Line: line}
	if strings.HasSuffix(line, nodeRuntugDuplicateTag) {
		result.Outcome = nodeJobAlreadyDone
	}
	// runtug has finished, so a failed marker costs only a repeat run, which
	// runtug itself answers as a duplicate of the same report.
	if err := replaceFileAtomicallyInRoot(job, nodeJobDoneMarker, []byte(digest+"\n"+line+"\n"), 0600); err != nil {
		slog.Warn("job.done marker write failed", "job", args.Job)
	}
	return result, ""
}

// readNodeJobDoneMarker answers the recorded result line when this report's
// digest is the one last completed. The digest is the key, as in runtug's own
// duplicate check, because a released job id can be claimed again: a marker
// that only said "done" would swallow the later round's completion.
func readNodeJobDoneMarker(job *os.Root, digest string) (string, bool) {
	if info, err := job.Lstat(nodeJobDoneMarker); err != nil || !info.Mode().IsRegular() {
		return "", false
	}
	contents, err := job.ReadFile(nodeJobDoneMarker)
	if err != nil {
		return "", false
	}
	recorded, line, _ := strings.Cut(string(contents), "\n")
	if recorded != digest {
		return "", false
	}
	return nodeJobDoneLine([]byte(line)), true
}

// nodeJobDoneLine is the last non-blank line of runtug's output, bounded.
func nodeJobDoneLine(output []byte) string {
	lines := strings.Split(strings.ToValidUTF8(string(output), ""), "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		if line := strings.TrimSpace(lines[index]); line != "" {
			return utf8PrefixWithin(line, nodeJobDoneMaxLine)
		}
	}
	return ""
}

var errNodeJobsRootUnset = errors.New("jobs root unset")

// openNodeJobDir opens <inbox>/jobs/<name> as an os.Root, so no symlink at or
// below it can lead outside. found is false when the job directory is missing
// or is not a real directory: the fleet did not create it.
func (client *HubClient) openNodeJobDir(name string) (*os.Root, string, bool, error) {
	if client.jobsInboxRoot == "" {
		return nil, "", false, errNodeJobsRootUnset
	}
	jobsRoot := filepath.Join(client.jobsInboxRoot, "jobs")
	jobs, err := os.OpenRoot(jobsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", false, nil
	} else if err != nil {
		return nil, "", false, err
	}
	defer jobs.Close()
	info, err := jobs.Lstat(name)
	switch {
	case errors.Is(err, os.ErrNotExist) || (err == nil && !info.IsDir()):
		return nil, "", false, nil
	case err != nil:
		return nil, "", false, err
	}
	job, err := jobs.OpenRoot(name)
	if err != nil {
		return nil, "", false, err
	}
	return job, filepath.Join(jobsRoot, name), true, nil
}

func (client *HubClient) nodeRuntugRunner() relayCommandRunner {
	if client.nodeRuntug != nil {
		return client.nodeRuntug
	}
	return func(ctx context.Context, args ...string) ([]byte, error) {
		// A node with spawn disabled has no usable config and still resolves
		// the conventional install, like every other runtug caller here.
		config, err := loadHubSpawnConfig(client.spawnConfigPath)
		if err != nil {
			config = hubSpawnConfig{}
		}
		home, _ := os.UserHomeDir()
		path := hubSpawnRuntugPath(config.RuntugPath, home, hubSpawnExecutable, exec.LookPath)
		if path == "" {
			return nil, errors.New("runtug missing")
		}
		// runtug starts its own children, so a timeout kills the whole group.
		command := exec.CommandContext(ctx, path, args...)
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		command.WaitDelay = time.Second
		command.Cancel = func() error {
			return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		}
		return command.Output()
	}
}

// logNodeReapCommand records one reap call on the node. It never receives
// screen contents or runtug's output.
func logNodeReapCommand(op string, decoded any, result any, code string) {
	pane, job := nodeReapCommandSubject(decoded)
	outcome := code
	switch typed := result.(type) {
	case PaneReadResult:
		outcome = typed.Outcome
	case JobDoneResult:
		outcome = typed.Outcome
	}
	slog.Info("node command", "op", op, "pane", pane, "job", job, "outcome", outcome)
}

func nodeReapCommandSubject(decoded any) (pane, job string) {
	switch args := decoded.(type) {
	case nodePaneReadArgs:
		return args.PaneID, args.Name
	case nodeJobDoneArgs:
		return "", args.Job
	}
	return "", ""
}
