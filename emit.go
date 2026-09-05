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
	"unicode/utf8"
)

// emitRelayKinds is the closed set of relay record kinds. It matches both the
// node scanner and handoffkeep's own CHECK constraint.
var emitRelayKinds = map[string]bool{"job.completed": true, "job.escalate": true, "job.joined": true, "lane.event": true}

const laneEventTextLimit = 2048

var errDuplicateLaneEventID = errors.New("duplicate lane event id")

// errEmitDuplicateOutboxKey means an existing file has the same relay key but
// represents different metadata. Unlike a wrk-written record followed by its
// immediate notification, that second record would be lost if treated as an
// ordinary duplicate.
var errEmitDuplicateOutboxKey = errors.New("duplicate outbox key")

// relayEventPathFallbackKinds are the kinds whose own event file is the durable
// record when no separate report exists. `panewire emit` and the node scanner
// both consult this map: if only one of them substituted the event path, the
// same event would carry two different report paths and so two dedupe keys.
var relayEventPathFallbackKinds = map[string]bool{"job.escalate": true, "job.joined": true}

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
	EventID        string `json:"event_id,omitempty"`
	Text           string `json:"text,omitempty"`
	Truncated      bool   `json:"truncated,omitempty"`
}

func runEmitCLI(args []string, stdout, stderr io.Writer, cfg CLIConfig) int {
	fs := flag.NewFlagSet("panewire emit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	kind := fs.String("kind", "", "job.completed | job.escalate | job.joined | lane.event")
	job := fs.String("job", "", "job id")
	lane := fs.String("lane", "", "direct destination lane for lane.event")
	eventID := fs.String("event-id", "", "producer event id for lane.event")
	text := fs.String("text", "", "payload for lane.event")
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
	if !emitRelayKinds[*kind] || *timeout <= 0 {
		return ExitUsage
	}
	if *kind == "lane.event" {
		if !hubAgentLabelPattern.MatchString(*lane) || !validLaneEventID(*eventID) || !validLaneEventText(*text) {
			return ExitUsage
		}
		finalText, truncated := truncateLaneEventText(*text)
		record := emitRecord{Type: *kind, Epoch: *epoch, CreatedAt: time.Now().UTC().Format(time.RFC3339), OwnerLane: *lane, Label: *label, Host: *host, PaneID: *pane, EventID: *eventID, Text: finalText, Truncated: truncated}
		if record.Epoch == 0 {
			record.Epoch = 1
		}
		root := emitInboxRoot(*inboxRoot)
		if _, err := writeLaneEmitRecord(root, record); err != nil {
			if errors.Is(err, errDuplicateLaneEventID) {
				fmt.Fprintln(stderr, "emit: duplicate event_id")
				return ExitUsage
			}
			fmt.Fprintln(stderr, "emit: event file could not be written:", err)
			return ExitInternal
		}
		socket := cfg.SocketPath
		if socket == "" {
			socket = socketPathFromEnv()
		}
		if !pushEmitRecord(socket, record, root, *timeout) {
			fmt.Fprintln(stderr, "emit: panewired unavailable; event recorded to file only")
		}
		return ExitOK
	}
	if *job == "" || !hubJobIDPattern.MatchString(*job) {
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

// writeEmitRecord recognizes a byte-for-byte equivalent relay record already
// written by wrk as the durable half of the same event. It returns that path so
// emit can still make the immediate socket notification. A same-key record with
// different metadata is an explicit conflict: silently reusing its file would
// discard a real event.
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
		// Empty-report escalations derive their eventual relay key from their
		// own event-file path. Different questions must therefore receive
		// separate files, even though the pre-file key is the same.
		if record.Type == "job.escalate" && record.ReportPath == "" && existing.question != record.Question {
			continue
		}
		if existing.matches(record) {
			return filepath.Join(eventsDir, entry.Name()), nil
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

// writeLaneEmitRecord intentionally uses events-lane rather than jobs/<id>.
// These records have no job lifecycle and must never enter active-job scans,
// late registration, or orphan sweeping. The producer-visible key is the
// direct (owner_lane,event_id) pair, so duplicates are an error rather than
// the silent job.* file reuse contract.
func writeLaneEmitRecord(inboxRoot string, record emitRecord) (string, error) {
	eventsDir := filepath.Join(inboxRoot, "events-lane")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		return "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	var highest uint64
	key := relayLaneEventOutboxKey(record.OwnerLane, record.EventID)
	for _, entry := range entries {
		if seq, ok := hubEventSequence(entry.Name()); ok && seq > highest {
			highest = seq
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if existing, ok := readEmitLaneEventDedupeKey(eventsDir, entry.Name()); ok && existing == key {
			return "", errDuplicateLaneEventID
		}
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
	for {
		highest++
		final := filepath.Join(eventsDir, fmt.Sprintf("%05d-lane.event.json", highest))
		// Link publishes the already-closed temporary file without replacing an
		// existing name. Rename would let two concurrent producers select the
		// same sequence and overwrite one another, which is unacceptable for a
		// durable lane event. A collision is either this key's duplicate or a
		// different producer that won the sequence, in which case try the next.
		if err := os.Link(name, final); err == nil {
			return final, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		if existing, ok := readEmitLaneEventDedupeKey(eventsDir, filepath.Base(final)); ok && existing == key {
			return "", errDuplicateLaneEventID
		}
	}
}

func readEmitLaneEventDedupeKey(eventsDir, name string) (string, bool) {
	contents, err := os.ReadFile(filepath.Join(eventsDir, name))
	if err != nil || len(contents) > 16<<10 {
		return "", false
	}
	var event emitRecord
	if json.Unmarshal(contents, &event) != nil || event.Type != "lane.event" || !validLaneEventID(event.EventID) || !hubAgentLabelPattern.MatchString(event.OwnerLane) {
		return "", false
	}
	return relayLaneEventOutboxKey(event.OwnerLane, event.EventID), true
}

func validLaneEventID(value string) bool {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

func validLaneEventText(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

func validLaneRelayText(value string) bool {
	if value == "" || len(value) > 4096 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return false
		}
	}
	return true
}

// truncateLaneEventText is the only truncation point for lane.event text.
// It runs on the producer node before the file, outbox key, and wire payload
// are made; the hub only validates this final form so acknowledgements cannot
// name a different record after a restart.
func truncateLaneEventText(value string) (string, bool) {
	if len(value) <= laneEventTextLimit {
		return value, false
	}
	const marker = "[truncated]"
	limit := laneEventTextLimit - len(marker)
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit] + marker, true
}

type emitDedupeRecord struct {
	key            string
	kind           string
	epoch          uint64
	ownerLane      string
	label          string
	host           string
	reportPath     string
	reportLastLine string
	reason         string
	question       string
	pr             string
	head           string
	paneID         string
}

// matches intentionally omits CreatedAt and AgentLabel. CreatedAt is local
// write time, while older wrk records do not carry AgentLabel; neither changes
// the relay event wrk asks emit to notify immediately.
func (existing emitDedupeRecord) matches(record emitRecord) bool {
	return existing.kind == record.Type &&
		existing.epoch == record.Epoch &&
		existing.ownerLane == record.OwnerLane &&
		existing.label == record.Label &&
		existing.host == record.Host &&
		existing.reportPath == record.ReportPath &&
		existing.reportLastLine == record.ReportLastLine &&
		existing.reason == record.Reason &&
		existing.question == record.Question &&
		existing.pr == record.PR &&
		existing.head == record.Head &&
		existing.paneID == record.PaneID
}

// readEmitDedupeKey reads one existing event file into the dedupe record it
// holds. The event-path fallback is deliberately not applied here: comparing a
// file written before it was named must use the fields it actually carries.
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
	return emitDedupeRecord{
		key:            relayEventOutboxKey(kind, jobID, epoch, event.reportPath(), event.reason()),
		kind:           kind,
		epoch:          epoch,
		ownerLane:      event.ownerLane(),
		label:          event.label(),
		host:           event.host(),
		reportPath:     event.reportPath(),
		reportLastLine: event.reportLastLine(),
		reason:         event.reason(),
		question:       event.question(),
		pr:             event.pr(),
		head:           event.head(),
		paneID:         event.paneID(),
	}, true
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
		PR: record.PR, Head: record.Head, AgentLabel: record.AgentLabel, EventID: record.EventID, Text: record.Text, Truncated: record.Truncated, InboxRoot: inboxRoot, TimeoutMS: timeout.Milliseconds(),
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
