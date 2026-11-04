package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/mgh3326/panewire/internal/fleet"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// pane.read and job.done are the two node commands mergewake reap needs for a
// job-bound lead: the composer guard before a close and `panewire done` after it
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

	nodeJobDoneOK             = "done"
	nodeJobAlreadyDone        = "already-done"
	nodeJobReportInvalid      = "report-invalid"
	nodeJobDoneMaxLine        = 1 << 10
	nodeJobDoneMarker         = ".node-job-done"
	nodeJobReportFile         = "report.md"
	nodeLifecycleDuplicateTag = " duplicate=yes"
)

type nodePaneReadArgs struct {
	Kind   string `json:"kind,omitempty"`
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
// "not-a-job" or "report-invalid"; Line is the lifecycle engine's result line, and is empty
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

// The idle placeholders a harness draws in an empty composer. "only `empty`
// permits closing" (docs/node-commands.md, "Reap composer adapter"), and a
// visible read cannot see that a placeholder is dim, so each one is a whole
// composer row matched exactly, and only where it proves nothing is unsent. A
// new or unlisted placeholder reads as text and keeps the pane. The sources are
// in the same section of the docs.
const (
	nodeCodexIdlePlaceholder = "Ask Codex to do anything"
	nodeDevinIdlePlaceholder = "Ask Devin to build features, fix bugs, or work on your code"
)

// nodeCodexQueueHeadings title the rows codex draws above its composer for
// messages it holds unsent; the composer shows its placeholder under them.
var nodeCodexQueueHeadings = []string{
	"• Messages to be submitted after next tool call",
	"• Messages to be submitted at end of turn",
	"• Queued follow-up inputs",
}

// nodeComposerState classifies a pane's input line from its visible screen.
// Anything but a composer located and seen empty is never "empty", because
// that answer lets a caller close the pane: any placeholder other than a
// harness's idle one reads as text, which keeps the pane. The screen is an ANSI
// read (nodeReadScreen). A Claude composer whose every visible character is
// dim (SGR 2) is a prompt suggestion, which is not unsent text, so it reads
// empty; one non-dim character, typed text, keeps the pane.
func nodeComposerState(harness, screen string) string {
	rows := parseNodeScreen(screen)
	screen = nodeScreenPlain(screen)
	if strings.EqualFold(harness, "codex") {
		return nodeCodexComposerState(screen)
	}
	if strings.EqualFold(harness, "claude") {
		var dividers []int
		for i, row := range rows {
			if line := row.String(); strings.Contains(line, "─") && strings.Trim(line, " \t─") == "" {
				dividers = append(dividers, i)
			}
		}
		if len(dividers) < 2 {
			return nodeComposerUnknown
		}
		for i, row := range rows[dividers[len(dividers)-2]+1 : dividers[len(dividers)-1]] {
			from := 0
			if i == 0 {
				for from < len(row.text) && unicode.IsSpace(row.text[from]) {
					from++
				}
				if from < len(row.text) && row.text[from] == '❯' {
					from++
				}
				if from < len(row.text) && row.text[from] == '>' {
					from++
				}
			}
			if row.solid(from) {
				return nodeComposerHasText
			}
		}
		return nodeComposerEmpty
	}
	content, located := relayComposer(harness, screen)
	switch {
	case !located:
		return nodeComposerUnknown
	case content == "" || (strings.EqualFold(harness, "devin") && nodeDevinComposerIdle(screen)):
		return nodeComposerEmpty
	}
	return nodeComposerHasText
}

// nodeDevinComposerIdle reports whether devin's composer region is exactly its
// idle placeholder with no queue above it. It lives here, not in relayComposer,
// because the relay's return guard must keep reading the placeholder as text.
func nodeDevinComposerIdle(screen string) bool {
	region, located := composerRegionWith(screen, isDevinDividerLine)
	if !located || devinQueued(screen) {
		return false
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(region), "❭")) == nodeDevinIdlePlaceholder
}

// nodeCodexComposerState follows mergewake's rule for codex, which draws no
// dividers: the input starts on the last › line and runs, wrapped or
// multi-line, until the first blank line above the footer. That input may also
// be exactly codex's idle placeholder, unless messages are queued above it.
func nodeCodexComposerState(screen string) string {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		rest, found := strings.CutPrefix(strings.TrimSpace(lines[index]), "›")
		if !found {
			continue
		}
		if rest = strings.TrimSpace(rest); rest != "" && (rest != nodeCodexIdlePlaceholder || nodeCodexQueued(lines[:index])) {
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

// nodeCodexQueued reports whether the rows above the composer open a queue
// section. A heading quoted in the transcript also matches, which keeps the pane.
func nodeCodexQueued(above []string) bool {
	for _, line := range above {
		line = strings.TrimSpace(line)
		for _, heading := range nodeCodexQueueHeadings {
			if strings.HasPrefix(line, heading) {
				return true
			}
		}
	}
	return false
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

// nodeReadScreen reads a pane's visible screen with its SGR attributes, which
// nodeComposerState needs to tell a dim prompt suggestion from typed text.
func (client *HubClient) nodeReadScreen(ctx context.Context, paneID string) (string, error) {
	output, err := client.nodeHerdrRunner()(ctx, "pane", "read", paneID, "--source", "visible", "--format", "ansi")
	return string(output), err
}

// nodePaneRead reports one pane. Matching name and cwd as well as the id is
// what confines it to this node: pane ids are host-local, so an id meant for
// another node can name an unrelated pane here, and that pane's data must not
// come back.
func (client *HubClient) nodePaneRead(ctx context.Context, args nodePaneReadArgs) (any, string) {
	outcome, row, err := client.nodePaneMatch(ctx, args.PaneID, args.Name, args.Kind, args.Cwd, false)
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
	screen, err := client.nodeReadScreen(ctx, args.PaneID)
	if err != nil {
		return result, ""
	}
	result.Composer = nodeComposerState(row.Agent, screen)
	result.ScreenTail = nodeScreenTail(nodeScreenPlain(screen), args.TailLines)
	return result, ""
}

// nodeJobDone runs `panewire done` once for a job whose directory exists here.
// panewire uploads the report it is given, so the report can only be one of the
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
	output, err := client.nodeLifecycleRunner()(ctx, "done", args.Job, "--report", args.Report)
	if err != nil {
		return nil, "done_failed"
	}
	line := nodeJobDoneLine(output)
	result := JobDoneResult{Outcome: nodeJobDoneOK, Line: line}
	if strings.HasSuffix(line, nodeLifecycleDuplicateTag) {
		result.Outcome = nodeJobAlreadyDone
	}
	// panewire has finished, so a failed marker costs only a repeat run, which
	// panewire itself answers as a duplicate of the same report.
	if err := replaceFileAtomicallyInRoot(job, nodeJobDoneMarker, []byte(digest+"\n"+line+"\n"), 0600); err != nil {
		slog.Warn("job.done marker write failed", "job", args.Job)
	}
	return result, ""
}

// readNodeJobDoneMarker answers the recorded result line when this report's
// digest is the one last completed. The digest is the key, as in panewire's own
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

// nodeJobDoneLine is the last non-blank line of panewire's output, bounded.
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

func (client *HubClient) nodeLifecycleRunner() relayCommandRunner {
	if client.nodeLifecycle != nil {
		return client.nodeLifecycle
	}
	return func(ctx context.Context, args ...string) ([]byte, error) {
		var output, errors bytes.Buffer
		err := fleet.ExecuteLifecycle(ctx, client.jobsInboxRoot, args, &output, &errors)
		return output.Bytes(), err
	}

}

// logNodeReapCommand records one reap call on the node. It never receives
// screen contents or panewire's output.
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
