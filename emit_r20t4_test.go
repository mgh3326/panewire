package panewire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// r20t4Socket answers every emit push with OK and records what it was given, so
// a test can compare the pushed payload against what the scanner would build.
func r20t4Socket(t *testing.T, handle func(localRequest)) string {
	t.Helper()
	socket := filepath.Join(r20SocketRoot(t), "emit.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				for scanner.Scan() {
					var request localRequest
					if json.Unmarshal(scanner.Bytes(), &request) == nil {
						handle(request)
					}
					body, _ := json.Marshal(localResponse{OK: true})
					_, _ = connection.Write(append(body, '\n'))
				}
			}()
		}
	}()
	return socket
}

type r20t4Pushes struct {
	mu      sync.Mutex
	records []localRequest
}

func (p *r20t4Pushes) add(request localRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, request)
}

func (p *r20t4Pushes) await(t *testing.T, want int) []localRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		got := append([]localRequest(nil), p.records...)
		p.mu.Unlock()
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket pushes=%d, want %d", len(got), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func r20t4EventFileReportPath(t *testing.T, path string) (string, bool) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(contents, &raw); err != nil {
		t.Fatal(err)
	}
	value, present := raw["report_path"]
	if !present {
		return "", false
	}
	var reportPath string
	if err := json.Unmarshal(value, &reportPath); err != nil {
		t.Fatal(err)
	}
	return reportPath, true
}

// TK1/TK3: an escalation with no separate report is accepted, leaves the file's
// report_path empty, and pushes the exact path the node scanner would build.
func TestR20T4EscalateWithoutReportPushesScannerIdenticalPath(t *testing.T) {
	inbox := t.TempDir()
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	args := []string{"--kind", "job.escalate", "--job", "r20t4-esc", "--owner-lane", "lane-a", "--label", "lane-a", "--host", "host-a", "--reason", "needs captain", "--question", "which branch?", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("code=%d, want ExitOK", code)
	}
	names := r20EventFiles(t, inbox, "r20t4-esc")
	if len(names) != 1 {
		t.Fatalf("event files=%v", names)
	}
	eventPath := filepath.Join(inbox, "jobs", "r20t4-esc", "events", names[0])
	// AC3: the file format is unchanged, so wrk and emit stay one producer.
	if reportPath, present := r20t4EventFileReportPath(t, eventPath); reportPath != "" || present {
		t.Fatalf("the event file gained a report_path %q (present=%t)", reportPath, present)
	}
	// AC4: the push carries the event file instead of an empty report path.
	pushed := pushes.await(t, 1)[0]
	if pushed.Op != "emit" || pushed.Kind != "job.escalate" || pushed.ReportPath != eventPath {
		t.Fatalf("push report_path=%q, want the event file %q", pushed.ReportPath, eventPath)
	}
	// AC5: the same root must yield the same string on the slow scan path.
	scanned := scanHubRelayEvents(inbox)
	if len(scanned) != 1 || scanned[0].Kind != "job.escalate" {
		t.Fatalf("scanned=%+v", scanned)
	}
	if scanned[0].ReportPath != pushed.ReportPath {
		t.Fatalf("emit pushed %q but the scanner built %q; the dedupe key would split", pushed.ReportPath, scanned[0].ReportPath)
	}
	if relayEventOutboxKey("job.escalate", "r20t4-esc", 1, pushed.ReportPath, pushed.Reason) != relayEventOutboxKey(scanned[0].Kind, scanned[0].JobID, scanned[0].Epoch, scanned[0].ReportPath, scanned[0].Reason) {
		t.Fatalf("emit and scan disagree on the dedupe key for the same event")
	}
}

// TK1b: an identical emit after the durable record exists must not add a
// second file, but it still sends the immediate notification.
func TestR20T4EscalateWithoutReportStillPushesRecordedEvent(t *testing.T) {
	inbox := t.TempDir()
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	args := []string{"--kind", "job.escalate", "--job", "r20t4-dupe", "--owner-lane", "lane-a", "--reason", "needs captain", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("first emit code=%d", code)
	}
	var stderr bytes.Buffer
	if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: socket}); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("repeat code=%d stderr=%q", code, stderr.String())
	}
	names := r20EventFiles(t, inbox, "r20t4-dupe")
	if len(names) != 1 {
		t.Fatalf("a repeated empty-report escalation wrote a second file: %v", names)
	}
	records := pushes.await(t, 2)
	if len(records) != 2 {
		t.Fatalf("repeat produced %d pushes, want two", len(records))
	}
	if records[0].ReportPath != records[1].ReportPath {
		t.Fatalf("repeat paths=%q and %q, want the same durable event path", records[0].ReportPath, records[1].ReportPath)
	}
}

// TK4/AC1: a completion still has to name the report it announces.
func TestR20T4CompletedStillRequiresReport(t *testing.T) {
	inbox := t.TempDir()
	code := runEmitCLI([]string{"--kind", "job.completed", "--job", "r20t4-done", "--owner-lane", "lane-a", "--inbox-root", inbox}, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: filepath.Join(t.TempDir(), "absent.sock")})
	if code != ExitUsage {
		t.Fatalf("code=%d, want ExitUsage", code)
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs")); err == nil {
		t.Fatal("a rejected completion wrote into the inbox")
	}
}

// TK5/AC7: a report that was given is never replaced by the event path.
func TestR20T4EscalateWithReportIsNotSubstituted(t *testing.T) {
	inbox := t.TempDir()
	report := filepath.Join(t.TempDir(), "report.md")
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	args := []string{"--kind", "job.escalate", "--job", "r20t4-kept", "--report", report, "--owner-lane", "lane-a", "--reason", "needs captain", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	if pushed := pushes.await(t, 1)[0]; pushed.ReportPath != report {
		t.Fatalf("push report_path=%q, want the given report %q", pushed.ReportPath, report)
	}
	if scanned := scanHubRelayEvents(inbox); len(scanned) != 1 || scanned[0].ReportPath != report {
		t.Fatalf("scanned=%+v, want the given report %q", scanned, report)
	}
}

// TK6/AC6: joined follows escalate on both sides. In practice wrk joined always
// carries a report; this pins that the coherence fix leaves that case alone.
func TestR20T4JoinedMatchesScannerWithAndWithoutReport(t *testing.T) {
	inbox := t.TempDir()
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	empty := []string{"--kind", "job.joined", "--job", "r20t4-join-empty", "--owner-lane", "lane-a", "--reason", "joined the lane", "--inbox-root", inbox}
	if code := runEmitCLI(empty, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("empty-report joined code=%d, want ExitOK", code)
	}
	report := filepath.Join(t.TempDir(), "report.md")
	withReport := []string{"--kind", "job.joined", "--job", "r20t4-join-report", "--report", report, "--owner-lane", "lane-a", "--reason", "joined the lane", "--inbox-root", inbox}
	if code := runEmitCLI(withReport, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("joined with a report code=%d", code)
	}
	pushedByJob := map[string]string{}
	for _, record := range pushes.await(t, 2) {
		pushedByJob[record.JobID] = record.ReportPath
	}
	scannedByJob := map[string]string{}
	for _, event := range scanHubRelayEvents(inbox) {
		scannedByJob[event.JobID] = event.ReportPath
	}
	names := r20EventFiles(t, inbox, "r20t4-join-empty")
	if len(names) != 1 {
		t.Fatalf("event files=%v", names)
	}
	eventPath := filepath.Join(inbox, "jobs", "r20t4-join-empty", "events", names[0])
	if pushedByJob["r20t4-join-empty"] != eventPath || scannedByJob["r20t4-join-empty"] != eventPath {
		t.Fatalf("empty-report joined: push=%q scan=%q, want %q", pushedByJob["r20t4-join-empty"], scannedByJob["r20t4-join-empty"], eventPath)
	}
	if pushedByJob["r20t4-join-report"] != report || scannedByJob["r20t4-join-report"] != report {
		t.Fatalf("joined with a report changed behavior: push=%q scan=%q, want %q", pushedByJob["r20t4-join-report"], scannedByJob["r20t4-join-report"], report)
	}
}

// TK7/AC8: end to end, the pane note for an empty-report escalation now points
// operators at the durable event file instead of at nothing.
func TestR20T4EmptyReportEscalationReachesRelayTextWithEventPath(t *testing.T) {
	inbox := t.TempDir()
	client := &HubClient{events: make(chan hubClientEvent, 4), completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}}
	daemon := NewDaemon(Config{Hub: HubDaemonConfig{Enabled: true, Client: client}})
	var emitErr error
	var mu sync.Mutex
	socket := r20t4Socket(t, func(request localRequest) {
		mu.Lock()
		defer mu.Unlock()
		emitErr = daemon.emitRelayEvent(request)
	})
	args := []string{"--kind", "job.escalate", "--job", "r20t4-e2e", "--owner-lane", "lane-a", "--label", "lane-a", "--host", "host-a", "--reason", "needs captain", "--question", "which branch?", "--inbox-root", inbox}
	if code := runEmitCLI(args, &bytes.Buffer{}, &bytes.Buffer{}, CLIConfig{SocketPath: socket}); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	var event hubClientEvent
	select {
	case event = <-client.events:
	case <-time.After(2 * time.Second):
		mu.Lock()
		err := emitErr
		mu.Unlock()
		t.Fatalf("the daemon queued no relay event for an empty-report escalation: err=%v", err)
	}
	names := r20EventFiles(t, inbox, "r20t4-e2e")
	if len(names) != 1 {
		t.Fatalf("event files=%v", names)
	}
	eventPath := filepath.Join(inbox, "jobs", "r20t4-e2e", "events", names[0])
	routes := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(routes, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-parent"},"lane-parent":{"machine":"host-a","pane":"w1:p2"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token"}, ReportRelayPath: routes})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	hub.connect("host-a", "test", "remote-a", agent, true)
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: event.Kind, Payload: event.Payload})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "remote-a", agent, wire)
	select {
	case relay := <-agent.relays:
		if relay.Pane != "w1:p2" || !strings.HasSuffix(relay.Text, " (전문: "+eventPath+")") || !strings.Contains(relay.Text, " … → "+eventPath) {
			t.Fatalf("relay=%+v, want the event path %q", relay, eventPath)
		}
	case <-time.After(time.Second):
		t.Fatal("an empty-report escalation queued no relay note")
	}
}
