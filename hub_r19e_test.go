package panewire

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func r19eDialOptions(machineID, token string) *websocket.DialOptions {
	headers := make(http.Header)
	headers.Set(hubMachineIDHeader, machineID)
	headers.Set(hubAuthorizationHeader, "Bearer "+token)
	return &websocket.DialOptions{HTTPHeader: headers}
}

// R19e covers the separation between R14 epoch fencing (which decides whether a
// job may be redispatched) and R18 relay (which decides whether a report
// reaches its lane). A job that finishes before any heartbeat advertised it was
// previously counted as an unknown message and its report was lost.

const (
	r19eWorkerNode = "node-a"
	r19eLaneNode   = "node-b"
)

type r19eFixture struct {
	hub     *HubServer
	server  *httptest.Server
	relays  chan hubRelayInjectEvent
	inbox   string
	cleanup []func()
}

func newR19eFixture(t *testing.T) *r19eFixture {
	t.Helper()
	lanes := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(lanes, []byte(`{"lanes":{"lane-a":{"machine":"`+r19eLaneNode+`","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{"operator": r6OperatorToken, r19eWorkerNode: r6NodeAToken, r19eLaneNode: r6NodeBToken},
		ReportRelayPath:   lanes,
		KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The lane target is an ordinary connected node; only its relay channel is
	// observed here.
	relays := make(chan hubRelayInjectEvent, 8)
	hub.connect(r19eLaneNode, "r19e", "fixture", &hubAgent{relays: relays}, false)
	server := httptest.NewServer(hub.Handler())
	fixture := &r19eFixture{hub: hub, server: server, relays: relays, inbox: t.TempDir()}
	t.Cleanup(func() {
		for i := len(fixture.cleanup) - 1; i >= 0; i-- {
			fixture.cleanup[i]()
		}
		server.Close()
	})
	return fixture
}

// connectWorker runs a real node client over the hub websocket handler and
// returns once its first message bundle (hello, heartbeat, terminal records)
// has been written. Each call is a distinct connection, so a second call models
// a node restart.
func (f *r19eFixture) connectWorker(t *testing.T) func() {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{
		URL:                   r6WSURL(f.server.URL, ""),
		MachineID:             r19eWorkerNode,
		Token:                 r6NodeAToken,
		JobsInboxRoot:         f.inbox,
		PingInterval:          time.Hour,
		AllowInsecureForTests: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, r19eDialOptions(r19eWorkerNode, r6NodeAToken))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.serve(ctx, connection) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("node client did not stop")
			}
			connection.CloseNow()
		})
	}
	f.cleanup = append(f.cleanup, stop)
	return stop
}

func (f *r19eFixture) awaitRelay(t *testing.T, jobID string) hubRelayInjectEvent {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case directive := <-f.relays:
			if directive.JobID == jobID {
				return directive
			}
		case <-deadline:
			t.Fatalf("no relay.inject for job %s (unknown_messages=%d unfenced=%d)", jobID, f.hub.UnknownMessageCount(), f.hub.UnfencedCompletionCount())
		}
	}
}

func r19eWriteEvent(t *testing.T, inbox, jobID, name, body string) {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func r19eClaim(t *testing.T, inbox, jobID, label string, at time.Time) {
	t.Helper()
	r19eWriteEvent(t, inbox, jobID, "00001-job.claim.json",
		`{"created_at":"`+at.UTC().Format(time.RFC3339)+`","kind":"job.claim","epoch":1,"payload":{"agent_label":"`+label+`","owner_lane":"lane-a"}}`)
}

func r19eCompletion(t *testing.T, inbox, jobID, label string, at time.Time) {
	t.Helper()
	r19eWriteEvent(t, inbox, jobID, "00002-job.completed.json",
		`{"created_at":"`+at.UTC().Format(time.RFC3339)+`","kind":"job.completed","epoch":1,"owner_lane":"lane-a","label":"`+label+`","host":"host-a","report_path":"/inbox/`+jobID+`/report.md","report_last_line":"VERDICT: DONE(pr=#1)"}`)
}

// (a) A short job claimed and completed between two heartbeats never appears in
// an active-job list, so the hub has no record to fence against. Its report
// must still be relayed.
func TestR19eCompletionBeforeAnyHeartbeatIsRelayed(t *testing.T) {
	f := newR19eFixture(t)
	now := time.Now()
	r19eClaim(t, f.inbox, "job-short", "wrk-short", now)
	r19eCompletion(t, f.inbox, "job-short", "wrk-short", now)

	f.connectWorker(t)
	directive := f.awaitRelay(t, "job-short")
	if directive.Pane != "w1:p1" || directive.Text == "" {
		t.Fatalf("relay directive=%+v", directive)
	}
	r6Eventually(t, "unfenced completion counted", func() bool { return f.hub.UnfencedCompletionCount() == 1 })
	if count := f.hub.UnknownMessageCount(); count != 0 {
		t.Fatalf("unfenced completion was counted as an unknown message: %d", count)
	}
	// Late registration keeps the job visible to operators without making it a
	// redispatch candidate.
	f.hub.mu.Lock()
	record := f.hub.jobs["job-short"]
	f.hub.mu.Unlock()
	if record == nil || !record.Completed || record.AgentLabel != "wrk-short" || record.Node != r19eWorkerNode {
		t.Fatalf("late registration=%+v", record)
	}
	if _, ok := f.hub.reassignJob("job-short", r19eLaneNode); ok {
		t.Fatal("late-registered completion became a redispatch candidate")
	}
}

// (b) After a node restart the first bundle is hello, heartbeat, then the
// terminal records. The heartbeat cannot carry an already-completed job, so
// relay must not depend on it.
func TestR19eRestartFirstBundleRelaysCompletion(t *testing.T) {
	f := newR19eFixture(t)
	stop := f.connectWorker(t)
	r6Eventually(t, "worker connected", func() bool {
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		return f.hub.nodes[r19eWorkerNode] != nil && f.hub.nodes[r19eWorkerNode].state == "connected"
	})
	stop()

	now := time.Now()
	r19eClaim(t, f.inbox, "job-restart", "wrk-restart", now)
	r19eCompletion(t, f.inbox, "job-restart", "wrk-restart", now)

	f.connectWorker(t) // new connection, fresh client state: a node restart.
	if directive := f.awaitRelay(t, "job-restart"); directive.Pane != "w1:p1" {
		t.Fatalf("relay directive=%+v", directive)
	}
	if count := f.hub.UnknownMessageCount(); count != 0 {
		t.Fatalf("restart completion was counted as an unknown message: %d", count)
	}
}

// (c) The node-side active scan is capped at 32 newest-first. A job outside the
// cap is never registered by the hub, and previously its completion vanished.
func TestR19eCompletionOutsideActiveScanCapIsRelayed(t *testing.T) {
	f := newR19eFixture(t)
	now := time.Now()
	// The target job is the oldest of 40, so it is ranked out of the 32-entry
	// heartbeat window even before it completes.
	target := "job-capped"
	r19eClaim(t, f.inbox, target, "wrk-capped", now.Add(-40*time.Minute))
	for i := 0; i < 39; i++ {
		r19eClaim(t, f.inbox, fmt.Sprintf("job-busy-%02d", i), fmt.Sprintf("wrk-busy-%02d", i), now.Add(-time.Duration(i)*time.Minute))
	}
	active := scanHubActiveJobs(f.inbox)
	if len(active) != 32 {
		t.Fatalf("active scan cap changed: %d", len(active))
	}
	for _, job := range active {
		if job.JobID == target {
			t.Fatal("fixture does not exercise the cap: target job is inside the heartbeat window")
		}
	}
	r19eCompletion(t, f.inbox, target, "wrk-capped", now)

	f.connectWorker(t)
	if directive := f.awaitRelay(t, target); directive.Pane != "w1:p1" {
		t.Fatalf("relay directive=%+v", directive)
	}
	if count := f.hub.UnknownMessageCount(); count != 0 {
		t.Fatalf("capped completion was counted as an unknown message: %d", count)
	}
}

// (3) A node restart re-sends terminal records it has already delivered. The
// hub-side dedupe is what keeps the owner pane from being injected twice.
func TestR19eRestartResendIsDedupedNotReinjected(t *testing.T) {
	f := newR19eFixture(t)
	now := time.Now()
	r19eClaim(t, f.inbox, "job-resend", "wrk-resend", now)
	r19eCompletion(t, f.inbox, "job-resend", "wrk-resend", now)

	// Count job.completed as the hub observes it, so the assertion below runs
	// only after the resent record has actually been processed.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 64)}
	f.hub.mu.Lock()
	f.hub.subscribers[subscriber] = struct{}{}
	f.hub.mu.Unlock()
	observed := make(chan struct{}, 8)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-subscriber.messages:
				if message.event != nil && message.event.Kind == "job.completed" {
					observed <- struct{}{}
				}
			}
		}
	}()

	stop := f.connectWorker(t)
	f.awaitRelay(t, "job-resend")
	r19eAwaitSignal(t, observed, "first completion observed")
	stop()

	f.connectWorker(t) // same inbox, fresh client dedupe state: resends it.
	r19eAwaitSignal(t, observed, "resent completion observed")
	select {
	case directive := <-f.relays:
		t.Fatalf("relay dedupe removed: duplicate injection %+v", directive)
	case <-time.After(250 * time.Millisecond):
	}
}

func r19eAwaitSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

// job.escalate and job.joined were never gated on fencing. This pins that
// behaviour so the two relay paths cannot drift apart again.
func TestR19eEscalationRelayIsIndependentOfJobRegistration(t *testing.T) {
	lanes := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(lanes, []byte(`{"lanes":{"lane-a":{"machine":"`+r19eLaneNode+`","pane":"w1:p1","parent":"lane-cap"},"lane-cap":{"machine":"`+r19eLaneNode+`","pane":"w9:p9"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, r19eLaneNode: r6NodeBToken}, ReportRelayPath: lanes})
	if err != nil {
		t.Fatal(err)
	}
	relays := make(chan hubRelayInjectEvent, 2)
	hub.connect(r19eLaneNode, "r19e", "fixture", &hubAgent{relays: relays}, false)
	if len(hub.jobs) != 0 {
		t.Fatalf("fixture registered a job: %+v", hub.jobs)
	}
	hub.relayJobEvent("job.escalate", hubJobEventPayload{JobID: "job-esc", Epoch: 7, OwnerLane: "lane-a", Label: "wrk", Host: "host-a", ReportPath: "/inbox/job-esc/report.md", Reason: "needs captain", Question: "which branch?"})
	select {
	case directive := <-relays:
		if directive.Pane != "w9:p9" {
			t.Fatalf("escalation directive=%+v", directive)
		}
	case <-time.After(time.Second):
		t.Fatal("escalation relay depended on job registration")
	}
}

// A higher epoch on a first-seen job cannot mint a hub record, but the report is
// still delivered.
func TestR19eLateRegistrationRejectsUnprovableEpoch(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, r19eWorkerNode: r6NodeAToken}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if hub.lateRegisterJobCompletion(r19eWorkerNode, hubJobEventPayload{JobID: "job-x", Epoch: 4, AgentLabel: "wrk"}, now) {
		t.Fatal("self-promoted epoch was late-registered")
	}
	if hub.lateRegisterJobCompletion(r19eWorkerNode, hubJobEventPayload{JobID: "job-x", Epoch: 1}, now) {
		t.Fatal("completion without a claim label was late-registered")
	}
	if !hub.lateRegisterJobCompletion(r19eWorkerNode, hubJobEventPayload{JobID: "job-x", Epoch: 1, AgentLabel: "wrk"}, now) {
		t.Fatal("valid late registration refused")
	}
	if hub.lateRegisterJobCompletion(r19eWorkerNode, hubJobEventPayload{JobID: "job-x", Epoch: 1, AgentLabel: "other"}, now) {
		t.Fatal("late registration overwrote an existing record")
	}
}

// The node carries the claim's agent_label onto the terminal record; the hub
// payload contract accepts it and rejects a malformed one.
func TestR19eTerminalRecordCarriesClaimLabel(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	r19eClaim(t, root, "job-meta", "wrk-meta", now)
	r19eCompletion(t, root, "job-meta", "wrk-meta", now)
	client := &HubClient{jobsInboxRoot: root, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}}
	events := client.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("events=%+v", events)
	}
	completion, ok := decodeHubJobCompletionPayload(events[0].Payload)
	if !ok || completion.AgentLabel != "wrk-meta" {
		t.Fatalf("payload=%s ok=%v", events[0].Payload, ok)
	}
	if _, ok := decodeHubJobCompletionPayload([]byte(`{"job_id":"job-meta","epoch":1,"agent_label":"bad label"}`)); ok {
		t.Fatal("malformed agent_label accepted")
	}
	if _, ok := decodeHubJobCompletionPayload([]byte(`{"job_id":"job-meta","epoch":1,"agent_labels":"wrk"}`)); ok {
		t.Fatal("unknown field accepted")
	}
}
