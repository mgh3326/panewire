package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

// `panewire job done|escalate|joined` is the Go form of `wrk done|escalate|
// joined` (agent-skills bin/wrk). wrk keeps its own implementation and
// delegates here only after `panewire job probe` answers with jobProbeToken, so
// the two must stay interchangeable: every byte they leave behind - the event
// record, the socket request, the handoffkeep call, the OK line, the failure
// logs - is pinned against the wrk path by testdata/job_golden.
//
// Nothing here normalizes relay text. The 240-rune bound, newline removal and
// the empty job.completed reason are applied once, on the node, when the wire
// form is built (relayEventWireForm / relayEventOutboxKeyFor). The only
// shortening done here is wrk's own: the report's last line and the document
// suffix, reproduced exactly as wrk computes them.
const jobProbeToken = "panewire-job/1"

// The shell utilities wrk runs are locale and platform dependent outside ASCII:
// BSD tr and cut count characters and stop at an invalid byte in a UTF-8
// locale, GNU ones count bytes. Rather than guess the host, text that is not
// plain ASCII goes through the very pipeline wrk runs. jobBash is a variable
// only so tests can point it at a specific shell.
var jobBash = "bash"

const jobEmitTimeout = 2 * time.Second

type jobCLI struct {
	stdout, stderr io.Writer
	socket         string
	now            func() time.Time
}

func runJobCLI(args []string, stdout, stderr io.Writer, cfg CLIConfig) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: panewire job probe|done|escalate|joined ...")
		return ExitUsage
	}
	socket := cfg.SocketPath
	if socket == "" {
		socket = socketPathFromEnv()
	}
	c := &jobCLI{stdout: stdout, stderr: stderr, socket: socket, now: time.Now}
	switch args[0] {
	case "probe":
		if len(args) != 1 {
			return ExitUsage
		}
		fmt.Fprintln(stdout, jobProbeToken)
		return ExitOK
	case "done":
		return c.done(args[1:])
	case "escalate":
		return c.escalate(args[1:])
	case "joined":
		return c.joined(args[1:])
	}
	fmt.Fprintln(stderr, "usage: panewire job probe|done|escalate|joined ...")
	return ExitUsage
}

// jobExit carries wrk's exit status out of a nested helper: `die` is 2, a
// failing pipeline under `set -e` is its own status.
type jobExit struct {
	code    int
	message string
}

func (e *jobExit) Error() string { return e.message }

func jobDie(format string, args ...any) error {
	return &jobExit{code: 2, message: fmt.Sprintf(format, args...)}
}

func (c *jobCLI) exit(err error) int {
	var exit *jobExit
	if errors.As(err, &exit) {
		if exit.message != "" {
			fmt.Fprintf(c.stderr, "panewire job: %s\n", exit.message)
		}
		return exit.code
	}
	fmt.Fprintf(c.stderr, "panewire job: %v\n", err)
	return ExitInternal
}

func (c *jobCLI) warn(format string, args ...any) {
	fmt.Fprintf(c.stderr, "panewire job: warning: "+format+"\n", args...)
}

func (c *jobCLI) done(args []string) int {
	job, report := "", ""
	if len(args) > 0 {
		job = args[0]
		args = args[1:]
	}
	if job == "" || strings.HasPrefix(job, "-") {
		return c.exit(jobDie("done requires a job id"))
	}
	for len(args) > 0 {
		switch args[0] {
		case "--report":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--report requires a path"))
			}
			report, args = args[1], args[2:]
		default:
			return c.exit(jobDie("unknown done option '%s'", args[0]))
		}
	}
	jobsRoot := jobJobsRoot()
	fields, err := jobCompletionMetadata(jobsRoot + "/" + job + "/events")
	if err != nil {
		return c.exit(jobDie("done cannot find job.claim/job.spawned metadata: %s", job))
	}
	// wrk reads five tab-separated fields into three names; pane_id is the
	// third field exactly as `read` splits it.
	read := bashReadTab(fields, 4)
	owner, label, pane := read[0], read[1], read[2]
	if report == "" {
		report = jobFindLatestReport(jobsRoot + "/" + job)
	}
	if report == "" || !jobIsRegularFile(report) {
		return c.exit(jobDie("done requires an existing --report or report*.md in the job directory"))
	}
	suffix := c.uploadReportDocument(job, report)
	host := jobHost()
	status := c.completionEvent(jobsRoot, job, owner, label, pane, host, report, suffix)
	if status == "failed" {
		// wrk's `completion_status="$(completion_event ...)"` carries the
		// failing script's status out of the substitution, and `set -e` ends
		// the command there: no notification, no OK line.
		return 1
	}
	if status == "duplicate" {
		jobNoteDuplicateCompletion(jobsRoot, job, report, c.now())
		c.warn("job.completed already recorded for this report; suppressed duplicate (job=%s)", job)
		fmt.Fprintf(c.stdout, "OK job=%s report=%s\n", job, report)
		return ExitOK
	}
	last, _ := c.reportLastLine(report, suffix)
	c.emit("job.completed", jobsRoot, job, owner, label, pane, host, report, last, "", "", "", "")
	fmt.Fprintf(c.stdout, "OK job=%s report=%s\n", job, report)
	return ExitOK
}

const jobEscalateHelp = `Usage: wrk escalate JOB --question TEXT [--report PATH]

Write a builder escalation record and relay it to the parent lane.

Options:
  --question TEXT    required escalation question
  --report PATH      optional existing report to attach and upload as a document
  -h, --help         show this usage
`

func (c *jobCLI) escalate(args []string) int {
	if len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
		if len(args) != 1 {
			return c.exit(jobDie("unexpected argument '%s'", args[1]))
		}
		fmt.Fprint(c.stdout, jobEscalateHelp)
		return ExitOK
	}
	job, question, report := "", "", ""
	if len(args) > 0 {
		job = args[0]
		args = args[1:]
	}
	if job == "" || strings.HasPrefix(job, "-") {
		return c.exit(jobDie("escalate requires a job id"))
	}
	for len(args) > 0 {
		switch args[0] {
		case "--question":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--question requires text"))
			}
			question, args = args[1], args[2:]
		case "--report":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--report requires a path"))
			}
			report, args = args[1], args[2:]
		default:
			return c.exit(jobDie("unknown escalate option '%s'", args[0]))
		}
	}
	if question == "" {
		return c.exit(jobDie("escalate requires --question"))
	}
	if report != "" && !jobIsRegularFile(report) {
		return c.exit(jobDie("escalate requires an existing --report"))
	}
	jobsRoot := jobJobsRoot()
	owner, label, pane, parent, role, err := jobBuilderMetadata(jobsRoot, job)
	if err != nil {
		return c.exit(err)
	}
	reason := "builder escalation"
	if role == "captain" {
		reason = "captain escalation"
	}
	suffix := ""
	if report != "" {
		suffix = c.uploadReportDocument(job, report)
	}
	host := jobHost()
	if err := c.builderFlatEvent(jobsRoot, job, owner, label, pane, parent, host, "job.escalate", report, suffix, reason, question, "", ""); err != nil {
		return c.exit(err)
	}
	last, _ := c.reportLastLine(report, suffix)
	c.emit("job.escalate", jobsRoot, job, owner, label, pane, host, report, last, reason, question, "", "")
	fmt.Fprintf(c.stdout, "OK job=%s owner_lane=%s kind=job.escalate\n", job, owner)
	return ExitOK
}

func (c *jobCLI) joined(args []string) int {
	job, pr, head, report := "", "", "", ""
	if len(args) > 0 {
		job = args[0]
		args = args[1:]
	}
	if job == "" || strings.HasPrefix(job, "-") {
		return c.exit(jobDie("joined requires a job id"))
	}
	for len(args) > 0 {
		switch args[0] {
		case "--pr":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--pr requires a URL"))
			}
			pr, args = args[1], args[2:]
		case "--head":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--head requires a SHA"))
			}
			head, args = args[1], args[2:]
		case "--report":
			if len(args) < 2 || args[1] == "" {
				return c.exit(jobDie("--report requires a path"))
			}
			report, args = args[1], args[2:]
		default:
			return c.exit(jobDie("unknown joined option '%s'", args[0]))
		}
	}
	if pr == "" || head == "" || report == "" || !jobIsRegularFile(report) {
		return c.exit(jobDie("joined requires --pr, --head and an existing --report"))
	}
	jobsRoot := jobJobsRoot()
	owner, label, pane, parent, role, err := jobBuilderMetadata(jobsRoot, job)
	if err != nil {
		return c.exit(err)
	}
	reason := "builder joined PR"
	if role == "captain" {
		reason = "captain joined PR"
	}
	suffix := c.uploadReportDocument(job, report)
	host := jobHost()
	if err := c.builderFlatEvent(jobsRoot, job, owner, label, pane, parent, host, "job.joined", report, suffix, reason, "", pr, head); err != nil {
		return c.exit(err)
	}
	last, _ := c.reportLastLine(report, suffix)
	c.emit("job.joined", jobsRoot, job, owner, label, pane, host, report, last, reason, "", pr, head)
	fmt.Fprintf(c.stdout, "OK job=%s owner_lane=%s kind=job.joined pr=%s head=%s report=%s\n", job, owner, pr, head, report)
	return ExitOK
}

// jobJobsRoot mirrors wrk_jobs_root: ARBITER_INBOX_ROOT names the jobs
// directory itself, and an empty value counts as unset (`${VAR:-default}`).
// wrk only ever uses it through command substitution, which drops trailing
// newlines.
func jobJobsRoot() string {
	root := os.Getenv("ARBITER_INBOX_ROOT")
	if root == "" {
		root = os.Getenv("HOME") + "/work/herdr-inbox/jobs"
	}
	return strings.TrimRight(root, "\n")
}

// jobHost is `${HOSTNAME:-unknown}`. bash sets HOSTNAME only when the
// environment does not already carry it, so an exported value wins, an
// exported empty value becomes "unknown", and otherwise it is gethostname().
// wrk passes its own value explicitly when it delegates.
func jobHost() string {
	if value, ok := os.LookupEnv("HOSTNAME"); ok {
		if value == "" {
			return "unknown"
		}
		return value
	}
	if name, err := os.Hostname(); err == nil && name != "" {
		return name
	}
	return "unknown"
}

// posixDirname is dirname(1): trailing slashes are not a component, and a
// path without a slash lives in ".".
func posixDirname(path string) string {
	if path == "" {
		return "."
	}
	trimmed := strings.TrimRight(path, "/")
	if trimmed == "" {
		return "/"
	}
	index := strings.LastIndexByte(trimmed, '/')
	if index < 0 {
		return "."
	}
	parent := strings.TrimRight(trimmed[:index], "/")
	if parent == "" {
		return "/"
	}
	return parent
}

func jobIsRegularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

// jobCompletionMetadata mirrors wrk's completion_metadata: the newest
// job.claim/job.reclaim establishes ownership, any record's string pane_id
// replaces the pane. It returns the five fields joined by tabs exactly as the
// Python print produced them, for bashReadTab to split the way wrk's `read`
// did. A file Python could not decode as UTF-8, or whose JSON or payload is
// not an object, crashed that script; that is an error here too.
func jobCompletionMetadata(eventsDir string) (string, error) {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	var owner, label, pane, parent, role string
	for _, name := range names {
		contents, err := os.ReadFile(filepath.Join(eventsDir, name))
		if err != nil {
			continue
		}
		if !utf8.Valid(contents) {
			return "", errors.New("undecodable event file")
		}
		value, ok := pythonJSONLoad(contents)
		if !ok {
			continue
		}
		object, ok := value.(map[string]any)
		if !ok {
			return "", errors.New("event file is not an object")
		}
		payload := object
		if raw, present := object["payload"]; present {
			inner, ok := raw.(map[string]any)
			if !ok {
				return "", errors.New("event payload is not an object")
			}
			payload = inner
		}
		if kind, _ := object["kind"].(string); kind == "job.claim" || kind == "job.reclaim" {
			owner = jobStringField(payload, "owner_lane")
			candidate, present := payload["label"]
			if !present {
				candidate = payload["agent_label"]
			}
			label, _ = candidate.(string)
			parent = jobStringField(payload, "parent_lane")
			role = jobStringField(payload, "role")
		}
		if value, ok := payload["pane_id"].(string); ok {
			pane = value
		}
	}
	if owner == "" || label == "" || pane == "" {
		return "", errors.New("incomplete job metadata")
	}
	// Command substitution drops NUL bytes and trailing newlines.
	line := strings.Join([]string{owner, label, pane, parent, role}, "\t")
	return strings.ReplaceAll(line, "\x00", ""), nil
}

// pythonJSONLoad is json.load on an already UTF-8-valid file: one value,
// surrounded only by JSON whitespace. Numbers stay textual so a value Python
// reads is never refused for its size.
func pythonJSONLoad(contents []byte) (any, bool) {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return nil, false
	}
	if len(bytes.Trim(contents[decoder.InputOffset():], " \t\n\r")) != 0 {
		return nil, false
	}
	return value, true
}

func jobStringField(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return value
}

// bashReadTab is `IFS=$'\t' read -r a b c ... <<<"$line"` for count names:
// only the first line is read, tab runs separate fields (tab is IFS
// whitespace, so empty fields collapse), and the last name takes the rest of
// the line minus trailing tabs.
func bashReadTab(line string, count int) []string {
	line = strings.TrimRight(line, "\n")
	if index := strings.IndexByte(line, '\n'); index >= 0 {
		line = line[:index]
	}
	fields := make([]string, count)
	rest := strings.TrimLeft(line, "\t")
	for i := 0; i < count-1; i++ {
		if rest == "" {
			return fields
		}
		index := strings.IndexByte(rest, '\t')
		if index < 0 {
			fields[i] = rest
			return fields
		}
		fields[i] = rest[:index]
		rest = strings.TrimLeft(rest[index:], "\t")
	}
	fields[count-1] = strings.TrimRight(rest, "\t")
	return fields
}

func jobBuilderMetadata(jobsRoot, job string) (owner, label, pane, parent, role string, err error) {
	fields, metadataErr := jobCompletionMetadata(jobsRoot + "/" + job + "/events")
	if metadataErr != nil {
		return "", "", "", "", "", jobDie("cannot find job.claim/job.spawned metadata: %s", job)
	}
	read := bashReadTab(fields, 5)
	owner, label, pane, parent, role = read[0], read[1], read[2], read[3], read[4]
	if (role != "builder" && role != "captain") || parent == "" {
		return "", "", "", "", "", jobDie("%s is not a builder claim with a parent lane (legacy captain alias is accepted)", job)
	}
	return owner, label, pane, parent, role, nil
}

// jobFindLatestReport mirrors find_latest_report: report*.md regular files in
// the job directory, newest modification time first; "" when there is none.
func jobFindLatestReport(directory string) string {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return ""
	}
	best, bestTime := "", time.Time{}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if matched, _ := filepath.Match("report*.md", name); !matched {
			continue
		}
		path := directory + "/" + name
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if best == "" || info.ModTime().After(bestTime) {
			best, bestTime = path, info.ModTime()
		}
	}
	return best
}

// uploadReportDocument mirrors upload_report_document and returns the
// document suffix ("" when nothing was uploaded). Every failure is a warning.
func (c *jobCLI) uploadReportDocument(job, report string) string {
	name := os.Getenv("HANDOFFKEEP_BIN")
	if name == "" {
		name = "handoffkeep"
	}
	binary, err := exec.LookPath(name)
	if err != nil {
		c.warn("handoffkeep not found; report document not uploaded (job=%s)", job)
		return ""
	}
	key := "reports/" + job + "/" + strings.TrimRight(filepath.Base(report), "\n")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "doc", "put", "--key", key, "--kind", "report", "--job", job, "--file", report)
	command.Stdout, command.Stderr = nil, nil
	rc := 0
	if err := command.Run(); err != nil {
		rc = 1
		if ctx.Err() != nil {
			rc = 124
		} else {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && exitErr.ExitCode() > 0 {
				rc = exitErr.ExitCode()
			} else if errors.As(err, &exitErr) {
				rc = 128 + int(exitErr.Sys().(syscall.WaitStatus).Signal())
			}
		}
	}
	if rc != 0 {
		c.warn("handoffkeep document upload failed (rc=%d job=%s); continuing without document link", rc, job)
		return ""
	}
	suffix := " doc:" + key
	if c.charLength(suffix) > 240 {
		c.warn("handoffkeep document key is too long for report link (job=%s)", job)
		return ""
	}
	return suffix
}

// reportLastLine is wrk's
//
//	tail -n 1 REPORT | tr '\r\n' ' ' | cut -c1-240
//
// followed by append_document_suffix, taken through command substitution. The
// status is the pipeline's under pipefail: wrk's builder commands run it
// under `set -e`, so a failing tr or cut ends them before any record exists.
func (c *jobCLI) reportLastLine(report, suffix string) (string, int) {
	last, rc := "", 0
	if report != "" && jobIsRegularFile(report) {
		contents, err := os.ReadFile(report)
		if err != nil {
			rc = 1
		} else {
			last, rc = c.shapeLine(tailLastLine(contents))
		}
	}
	return c.appendDocumentSuffix(last, suffix), rc
}

func tailLastLine(contents []byte) []byte {
	if len(contents) == 0 {
		return nil
	}
	end := len(contents)
	if contents[end-1] == '\n' {
		end--
	}
	return contents[bytes.LastIndexByte(contents[:end], '\n')+1:]
}

// plainASCII reports whether every tool on every host treats the text as one
// byte per character: no NUL (which command substitution drops) and nothing
// at or above 0x80 (which is where BSD and GNU tools part ways).
func plainASCII(text []byte) bool {
	for _, b := range text {
		if b == 0 || b >= 0x80 {
			return false
		}
	}
	return true
}

func (c *jobCLI) shapeLine(line []byte) (string, int) {
	if plainASCII(line) {
		shaped := bytes.Map(func(r rune) rune {
			if r == '\r' || r == '\n' {
				return ' '
			}
			return r
		}, line)
		if len(shaped) > 240 {
			shaped = shaped[:240]
		}
		return string(shaped), 0
	}
	return c.hostPipeline(line, `set -o pipefail; tr '\r\n' ' ' | cut -c1-240`)
}

// appendDocumentSuffix mirrors append_document_suffix: the line keeps
// 240 minus the suffix's length in characters, then the suffix follows.
func (c *jobCLI) appendDocumentSuffix(last, suffix string) string {
	if suffix == "" {
		return last
	}
	limit := 240 - c.charLength(suffix)
	shortened := ""
	if limit > 0 {
		if plainASCII([]byte(last)) {
			shortened = last
			if len(shortened) > limit {
				shortened = shortened[:limit]
			}
		} else {
			// wrk runs this cut inside its own command substitution, where
			// errexit does not apply: its status never matters.
			shortened, _ = c.hostPipeline([]byte(last), "cut -c1-"+strconv.Itoa(limit))
		}
	}
	return shortened + suffix
}

// charLength is bash's ${#value} in the current locale.
func (c *jobCLI) charLength(value string) int {
	if plainASCII([]byte(value)) {
		return len(value)
	}
	command := exec.Command(jobBash, "-c", `printf %s "${#1}"`, "bash", value)
	command.Stderr = c.stderr
	out, err := command.Output()
	if n, parseErr := strconv.Atoi(string(out)); err == nil && parseErr == nil {
		return n
	}
	return utf8.RuneCountInString(value)
}

// hostPipeline feeds text through the host's own tools and returns what
// command substitution would keep: NUL bytes and trailing newlines dropped.
func (c *jobCLI) hostPipeline(text []byte, script string) (string, int) {
	command := exec.Command(jobBash, "-c", script)
	command.Stdin = bytes.NewReader(text)
	command.Stderr = c.stderr
	out, err := command.Output()
	rc := 0
	if err != nil {
		rc = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() > 0 {
			rc = exitErr.ExitCode()
		}
	}
	out = bytes.ReplaceAll(out, []byte{0}, nil)
	return strings.TrimRight(string(out), "\n"), rc
}

// completionEvent mirrors wrk's completion_event for job.completed and
// returns "created", "duplicate" or "failed". "failed" is every case in which
// wrk's Python exits non-zero; it has written no record by then.
func (c *jobCLI) completionEvent(jobsRoot, job, owner, label, pane, host, report, suffix string) string {
	eventsDir := jobsRoot + "/" + job + "/events"
	if err := os.MkdirAll(eventsDir, 0o777); err != nil {
		c.warn("completion record not written: %v", err)
		return "failed"
	}
	last, _ := c.reportLastLine(report, suffix)
	digest := ""
	if contents, err := os.ReadFile(report); err == nil {
		sum := sha256.Sum256(contents)
		digest = hex.EncodeToString(sum[:])
	}
	lock, err := os.OpenFile(filepath.Join(posixDirname(eventsDir), ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		c.warn("completion record not written: %v", err)
		return "failed"
	}
	defer lock.Close()
	// The completion sentinel still writes through wrk; both take this lock
	// around the duplicate check and the write.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		c.warn("completion record not written: %v", err)
		return "failed"
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if digest != "" {
		entries, err := os.ReadDir(eventsDir)
		if err != nil {
			c.warn("completion record not written: %v", err)
			return "failed"
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			if !strings.HasSuffix(name, "-job.completed.json") {
				continue
			}
			contents, err := os.ReadFile(filepath.Join(eventsDir, name))
			if err != nil || !utf8.Valid(contents) {
				continue
			}
			value, ok := pythonJSONLoad(contents)
			if !ok {
				continue
			}
			prior, ok := value.(map[string]any)
			if !ok {
				c.warn("completion record not written: unreadable prior record %s", name)
				return "failed"
			}
			if kind, _ := prior["kind"].(string); kind != "job.completed" {
				continue
			}
			// A record without content identity is never a suppression basis.
			if priorDigest, ok := prior["report_sha256"].(string); ok && priorDigest == digest {
				return "duplicate"
			}
		}
	}
	fields := []jobField{
		{"kind", "job.completed"}, {"job_id", job}, {"owner_lane", owner}, {"label", label},
		{"pane_id", pane}, {"host", host}, {"report_path", report}, {"report_last_line", last},
		{"epoch", 1},
	}
	if digest != "" {
		fields = append(fields, jobField{"report_sha256", digest})
	}
	if err := writeJobRecord(eventsDir, "job.completed", fields); err != nil {
		c.warn("completion record not written: %v", err)
		return "failed"
	}
	return "created"
}

// builderFlatEvent mirrors builder_flat_event. wrk calls it directly under
// `set -e`, so a failing last-line pipeline ends the command (with the
// pipeline's status) before anything is written.
func (c *jobCLI) builderFlatEvent(jobsRoot, job, owner, label, pane, parent, host, kind, report, suffix, reason, question, pr, head string) error {
	eventsDir := jobsRoot + "/" + job + "/events"
	if err := os.MkdirAll(eventsDir, 0o777); err != nil {
		return &jobExit{code: 1, message: err.Error()}
	}
	last, rc := c.reportLastLine(report, suffix)
	if rc != 0 {
		return &jobExit{code: rc}
	}
	fields := []jobField{
		{"kind", kind}, {"job_id", job}, {"owner_lane", owner}, {"label", label},
		{"pane_id", pane}, {"host", host}, {"report_path", report}, {"report_last_line", last},
		{"reason", reason}, {"parent_lane", parent}, {"epoch", 1},
	}
	if question != "" {
		fields = append(fields, jobField{"question", question})
	}
	if pr != "" {
		fields = append(fields, jobField{"pr", pr})
	}
	if head != "" {
		fields = append(fields, jobField{"head", head})
	}
	if err := writeJobRecord(eventsDir, kind, fields); err != nil {
		return &jobExit{code: 1, message: err.Error()}
	}
	return nil
}

type jobField struct {
	key   string
	value any // string or int
}

// writeJobRecord writes the record exactly as wrk's Python does:
// json.dump(record, separators=(',', ':')) plus a newline, mode 0600, named
// one past the highest numeric prefix, landed by rename.
func writeJobRecord(eventsDir, kind string, fields []jobField) error {
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return err
	}
	highest := big.NewInt(0)
	for _, entry := range entries {
		prefix, _, _ := strings.Cut(entry.Name(), "-")
		if value, ok := pythonInt(prefix); ok && value.Cmp(highest) > 0 {
			highest = value
		}
	}
	var body bytes.Buffer
	body.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(pythonJSONString(field.key))
		body.WriteByte(':')
		switch value := field.value.(type) {
		case string:
			body.WriteString(pythonJSONString(value))
		case int:
			body.WriteString(strconv.Itoa(value))
		default:
			return fmt.Errorf("unsupported record field %s", field.key)
		}
	}
	body.WriteString("}\n")
	temporary, err := os.CreateTemp(eventsDir, ".completion-")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(body.Bytes()); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(name, 0o600); err != nil {
		return err
	}
	next := new(big.Int).Add(highest, big.NewInt(1))
	return os.Rename(name, filepath.Join(eventsDir, fmt.Sprintf("%05s-%s.json", next.String(), kind)))
}

// pythonInt accepts what Python's int() accepts from an ASCII string: outer
// whitespace, one sign, digits with single underscores between them.
func pythonInt(text string) (*big.Int, bool) {
	text = strings.Trim(text, " \t\n\v\f\r")
	digits := strings.TrimLeft(text, "+-")
	if len(text)-len(digits) > 1 || digits == "" || digits[0] == '_' || digits[len(digits)-1] == '_' || strings.Contains(digits, "__") {
		return nil, false
	}
	digits = strings.ReplaceAll(digits, "_", "")
	for _, b := range []byte(digits) {
		if b < '0' || b > '9' {
			return nil, false
		}
	}
	value, ok := new(big.Int).SetString(digits, 10)
	if ok && strings.HasPrefix(text, "-") {
		value.Neg(value)
	}
	return value, ok
}

// pythonJSONString is json.dumps(str) with ensure_ascii for a str that Python
// decoded from argv with surrogateescape: an undecodable byte b becomes the
// lone surrogate U+DC00+b.
func pythonJSONString(value string) string {
	var out strings.Builder
	out.WriteByte('"')
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && size <= 1 {
			fmt.Fprintf(&out, `\u%04x`, 0xdc00+int(value[i]))
			i++
			continue
		}
		i += size
		switch {
		case r == '"':
			out.WriteString(`\"`)
		case r == '\\':
			out.WriteString(`\\`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r == '\t':
			out.WriteString(`\t`)
		case r == '\b':
			out.WriteString(`\b`)
		case r == '\f':
			out.WriteString(`\f`)
		case r < 0x20 || r == 0x7f:
			fmt.Fprintf(&out, `\u%04x`, r)
		case r < 0x7f:
			out.WriteRune(r)
		case r < 0x10000:
			fmt.Fprintf(&out, `\u%04x`, r)
		default:
			r -= 0x10000
			fmt.Fprintf(&out, `\u%04x\u%04x`, 0xd800+(r>>10), 0xdc00+(r&0x3ff))
		}
	}
	out.WriteByte('"')
	return out.String()
}

// emit mirrors emit_relay_event: the same call `panewire emit` would make
// with wrk's arguments, fail-open, with a failure marker line on any non-zero
// status. The emit's own stderr was discarded by wrk and is here too.
func (c *jobCLI) emit(kind, jobsRoot, job, owner, label, pane, host, report, last, reason, question, pr, head string) {
	record := emitRecord{
		Type: kind, JobID: job, Epoch: 1, CreatedAt: c.now().UTC().Format(time.RFC3339),
		AgentLabel: label, OwnerLane: owner, Label: label, Host: host, ReportPath: report,
		ReportLastLine: last, Reason: reason, Question: question, PR: pr, Head: head, PaneID: pane,
	}
	rc := emitJobRecord(record, posixDirname(jobsRoot), c.socket, jobEmitTimeout, io.Discard)
	if rc != ExitOK {
		c.warn("panewire emit failed (rc=%d job=%s kind=%s); relay event left as file only", rc, job, kind)
		jobRecordEmitFailure(jobsRoot, job, kind, strconv.Itoa(rc), c.now())
	}
}

// jobRecordEmitFailure mirrors record_emit_failure: append-only, best effort.
func jobRecordEmitFailure(jobsRoot, job, kind, rc string, now time.Time) {
	jobAppendLog(jobsRoot+"/"+job, "emit-failures.log", fmt.Sprintf("%s kind=%s rc=%s\n", now.UTC().Format("2006-01-02T15:04:05Z"), kind, rc))
}

func jobNoteDuplicateCompletion(jobsRoot, job, report string, now time.Time) {
	jobAppendLog(jobsRoot+"/"+job, "completion-suppressed.log", fmt.Sprintf("%s suppressed-duplicate report=%s\n", now.UTC().Format("2006-01-02T15:04:05Z"), report))
}

func jobAppendLog(directory, name, line string) {
	if os.MkdirAll(directory, 0o777) != nil {
		return
	}
	file, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return
	}
	_, _ = file.WriteString(line)
	_ = file.Close()
}
