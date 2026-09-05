package panewire

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// emitRelayKinds is the closed set of relay record kinds. It matches both the
// node scanner and handoffkeep's own CHECK constraint.
var emitRelayKinds = map[string]bool{"job.completed": true, "job.escalate": true, "job.joined": true}

// relayEventPathFallbackKinds are the kinds whose own event file is the durable
// record when no separate report exists. `panewire emit` and the node scanner
// both consult this map: if only one of them substituted the event path, the
// same event would carry two different report paths and so two dedupe keys.
var relayEventPathFallbackKinds = map[string]bool{"job.escalate": true, "job.joined": true}

var errEmitDuplicateOutboxKey = errors.New("duplicate outbox key")

// emitInboxRoot resolves the same namespace the daemon watches, so an event a
// worker writes is the event the node later scans.
func emitInboxRoot(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if root := os.Getenv("PANEWIRE_INBOX_ROOT"); root != "" {
		return root
	}
	return defaultInboxRoot()
}

func defaultInboxRoot() string {
	return filepath.Join(filepath.Dir(defaultSocketPath()), "inbox")
}

// emitRecord is the flat local event form hubInboxEvent already reads. The
// file is the offline fallback, so it must stay readable without the daemon.
type emitRecord struct {
	Type           string `json:"type"`
	JobID          string `json:"job_id"`
	Epoch          uint64 `json:"epoch"`
	CreatedAt      string `json:"created_at"`
	AgentLabel     string `json:"agent_label,omitempty"`
	OwnerLane      string `json:"owner_lane,omitempty"`
	Label          string `json:"label,omitempty"`
	Host           string `json:"host,omitempty"`
	ReportPath     string `json:"report_path,omitempty"`
	ReportLastLine string `json:"report_last_line,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Question       string `json:"question,omitempty"`
	PR             string `json:"pr,omitempty"`
	Head           string `json:"head,omitempty"`
	PaneID         string `json:"pane_id,omitempty"`
}

func runEmitCLI(args []string, stdout, stderr io.Writer, cfg CLIConfig) int {
	fs := flag.NewFlagSet("panewire emit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	kind := fs.String("kind", "", "job.completed | job.escalate | job.joined")
	job := fs.String("job", "", "job id")
	report := fs.String("report", "", "report path")
	ownerLane := fs.String("owner-lane", "", "owning lane")
	epoch := fs.Uint64("epoch", 0, "job epoch (defaults to 1)")
	label := fs.String("label", "", "agent label for the relay note")
	host := fs.String("host", "", "originating host label")
	pane := fs.String("pane", "", "originating pane id")
	reportLastLine := fs.String("report-last-line", "", "last line of the report")
	reason := fs.String("reason", "", "escalation or join reason")
	question := fs.String("question", "", "escalation question")
	pr := fs.String("pr", "", "pull request URL")
	head := fs.String("head", "", "head SHA")
	inboxRoot := fs.String("inbox-root", "", "inbox root containing jobs/<job>/events")
	timeout := fs.Duration("timeout", 2*time.Second, "local daemon call timeout")
	if fs.Parse(args) != nil || fs.NArg() != 0 {
		return ExitUsage
	}
	if !emitRelayKinds[*kind] || *job == "" || *timeout <= 0 || !hubJobIDPattern.MatchString(*job) {
		return ExitUsage
	}
	// A completion is meaningless without the report it announces. An escalation
	// or a join carries its own question in the event file, so an empty report is
	// the normal shape there and the event file stands in for the report path.
	if *report == "" && !relayEventPathFallbackKinds[*kind] {
		return ExitUsage
	}
	if *epoch == 0 {
		// The node scanner normalizes a missing epoch to 1; write what it reads
		// so the dedupe key is identical on both sides.
		*epoch = 1
	}
	record := emitRecord{
		Type: *kind, JobID: *job, Epoch: *epoch, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		AgentLabel: *label, OwnerLane: *ownerLane, Label: *label, Host: *host, ReportPath: *report,
		ReportLastLine: *reportLastLine, Reason: *reason, Question: *question, PR: *pr, Head: *head, PaneID: *pane,
	}
	if !hubAgentLabelPattern.MatchString(record.AgentLabel) {
		record.AgentLabel = ""
	}
	root := emitInboxRoot(*inboxRoot)
	// The file lands before the socket call: a dead daemon must never cost the
	// record, and the node outbox is what picks it up afterwards.
	eventPath, err := writeEmitRecord(root, record)
	if err != nil {
		if errors.Is(err, errEmitDuplicateOutboxKey) {
			fmt.Fprintln(stderr, "emit: duplicate outbox key")
			return ExitDeliveryFailure
		}
		fmt.Fprintln(stderr, "emit: event file could not be written:", err)
		return ExitInternal
	}
	// Only the pushed record is rewritten; the file on disk keeps its empty
	// report_path so the scanner derives the very same substitution from the
	// same root, byte for byte.
	if record.ReportPath == "" && relayEventPathFallbackKinds[record.Type] {
		record.ReportPath = eventPath
	}
	socket := cfg.SocketPath
	if socket == "" {
		socket = socketPathFromEnv()
	}
	// The push carries the namespace the file was written in. A daemon that
	// watches a different root refuses it, so a run against a temporary inbox
	// root cannot reach the operator's live relay outbox.
	if !pushEmitRecord(socket, record, root, *timeout) {
		fmt.Fprintln(stderr, "emit: panewired unavailable; event recorded to file only")
		return ExitOK
	}
	return ExitOK
}

// writeEmitRecord rejects a duplicate event key before it reaches the socket.
// Empty-report escalations compare question too: their distinct event files
// become distinct report_path values on the scanner path without new key fields.
func writeEmitRecord(inboxRoot string, record emitRecord) (string, error) {
	eventsDir := filepath.Join(inboxRoot, "jobs", record.JobID, "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	key := relayEventOutboxKey(record.Type, record.JobID, record.Epoch, record.ReportPath, record.Reason)
	var highest uint64
	for _, entry := range entries {
		if seq, ok := hubEventSequence(entry.Name()); ok && seq > highest {
			highest = seq
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		existing, ok := readEmitDedupeKey(eventsDir, entry.Name(), record.JobID)
		if !ok || existing.key != key {
			continue
		}
		if record.Type == "job.escalate" && record.ReportPath == "" && existing.question != record.Question {
			continue
		}
		return "", errEmitDuplicateOutboxKey
	}
	contents, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp(eventsDir, ".emit-*")
	if err != nil {
		return "", err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	final := filepath.Join(eventsDir, fmt.Sprintf("%05d-%s.json", highest+1, record.Type))
	if err := os.Rename(name, final); err != nil {
		return "", err
	}
	return final, nil
}

type emitDedupeRecord struct {
	key      string
	question string
}

// readEmitDedupeKey reads one existing event file into the dedupe record of the
// record it holds. The event-path fallback is deliberately not applied here:
// suppressing a duplicate file compares what the files carry, and a record that
// has not been named yet has no event path to compare against.
func readEmitDedupeKey(eventsDir, name, jobID string) (emitDedupeRecord, bool) {
	contents, err := os.ReadFile(filepath.Join(eventsDir, name))
	if err != nil || len(contents) > 16<<10 {
		return emitDedupeRecord{}, false
	}
	var event hubInboxEvent
	if json.Unmarshal(contents, &event) != nil {
		return emitDedupeRecord{}, false
	}
	kind := event.eventKind()
	if kind == "job.completion" {
		kind = "job.completed"
	}
	if !emitRelayKinds[kind] {
		return emitDedupeRecord{}, false
	}
	epoch := event.Epoch
	if epoch == 0 {
		epoch = 1
	}
	return emitDedupeRecord{key: relayEventOutboxKey(kind, jobID, epoch, event.reportPath(), event.reason()), question: event.question()}, true
}

func pushEmitRecord(socket string, record emitRecord, inboxRoot string, timeout time.Duration) bool {
	connection, err := net.DialTimeout("unix", socket, timeout)
	if err != nil {
		return false
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(timeout))
	request := localRequest{
		Op: "emit", Kind: record.Type, JobID: record.JobID, Epoch: record.Epoch, OwnerLane: record.OwnerLane,
		Label: record.Label, Host: record.Host, PaneID: record.PaneID, ReportPath: record.ReportPath,
		ReportLastLine: record.ReportLastLine, Reason: record.Reason, Question: record.Question,
		PR: record.PR, Head: record.Head, AgentLabel: record.AgentLabel, InboxRoot: inboxRoot, TimeoutMS: timeout.Milliseconds(),
	}
	body, _ := json.Marshal(request)
	if _, err := fmt.Fprintf(connection, "%s\n", body); err != nil {
		return false
	}
	scanner := bufio.NewScanner(connection)
	if !scanner.Scan() {
		return false
	}
	var response localResponse
	return json.Unmarshal(scanner.Bytes(), &response) == nil && response.OK
}
