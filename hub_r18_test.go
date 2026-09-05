package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR18CompletionPayloadPreservesReportAndDedupesPath(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "r18", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.completed.json"), []byte(`{"kind":"job.completed","epoch":1,"owner_lane":"lane","label":"worker","host":"host","report_path":"report.md","report_last_line":"done"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := &HubClient{jobsInboxRoot: root, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}}
	eventsOut := c.jobCompletionEvents()
	if len(eventsOut) != 1 {
		t.Fatalf("events=%d", len(eventsOut))
	}
	var payload hubJobEventPayload
	if err := json.Unmarshal(eventsOut[0].Payload, &payload); err != nil || payload.ReportPath != "report.md" || payload.OwnerLane != "lane" {
		t.Fatalf("payload=%s err=%v", eventsOut[0].Payload, err)
	}
	if got := c.jobCompletionEvents(); len(got) != 0 {
		t.Fatalf("duplicate=%d", len(got))
	}
}

func TestR18RelayDedupeAndUnroutedEvent(t *testing.T) {
	routes := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(routes, []byte(`{"routes":{"lane-a":{"machine":"node-a","pane":"test-pane"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "node-a": "node"}, ReportRelayPath: routes})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 2)}
	h.nodes["node-a"] = &hubNodeRecord{agent: agent}
	completed := hubJobEventPayload{JobID: "r18", Epoch: 1, OwnerLane: "lane-a", Label: "worker", Host: "host", ReportPath: "report.md", ReportLastLine: "done"}
	h.relayJobCompletion(completed)
	select {
	case directive := <-agent.relays:
		if directive.Pane != "test-pane" {
			t.Fatalf("directive=%+v", directive)
		}
	case <-time.After(time.Second):
		t.Fatal("missing directive")
	}
	h.relayJobCompletion(completed)
	select {
	case <-agent.relays:
		t.Fatal("dedupe removed: duplicate directive")
	default:
	}

	// An absent mapping is a visible relay.unrouted result, never a silent drop.
	h2, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 1)}
	h2.subscribers[sub] = struct{}{}
	h2.relayJobCompletion(completed)
	select {
	case result := <-sub.messages:
		if result.event == nil || result.event.Kind != "relay.unrouted" {
			t.Fatalf("event=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("unrouted completion was silently dropped")
	}
}

func TestR18EnvelopeHeartbeatThenFlatCompletionRelays(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-20260101-0000", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(dir, "00001-job.claim.json"), []byte(`{"created_at":"`+now+`","job_id":"job-20260101-0000","kind":"job.claim","payload":{"agent_label":"wrk-a","owner_lane":"lane-a","t_level":"T1"},"seq":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	routes := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(routes, []byte(`{"routes":{"lane-a":{"machine":"node-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}, ReportRelayPath: routes})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	h.nodes["node-a"] = &hubNodeRecord{agent: agent}
	client := &HubClient{jobsInboxRoot: root, assignedJobs: map[string]uint64{}, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}}
	heartbeat, ok := decodeHubHeartbeatPayload(client.heartbeatEvent(context.Background()).Payload)
	if !ok || len(heartbeat.ActiveJobs) != 1 {
		t.Fatalf("envelope heartbeat did not produce active job: %+v", heartbeat)
	}
	h.observeActiveJobs("node-a", heartbeat.ActiveJobs, time.Now().UTC())
	if h.jobs["job-20260101-0000"] == nil {
		t.Fatal("hub did not register envelope job")
	}
	if err := os.WriteFile(filepath.Join(dir, "00002-job.completed.json"), []byte(`{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"wrk-a","host":"host-a","report_path":"report.md","report_last_line":"VERDICT: DONE"}`), 0600); err != nil {
		t.Fatal(err)
	}
	events := client.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("flat completion missing: %+v", events)
	}
	completion, ok := decodeHubJobCompletionPayload(events[0].Payload)
	if !ok || !h.observeJobCompletion("node-a", completion, time.Now().UTC()) {
		t.Fatalf("hub fenced flat completion: %+v", completion)
	}
	h.relayJobCompletion(completion)
	select {
	case relay := <-agent.relays:
		if relay.Pane != "w1:p1" {
			t.Fatalf("relay=%+v", relay)
		}
	default:
		t.Fatal("R18 relay.inject not queued")
	}
}

func TestR18RelayDedupeKeyUsesDecimalEpoch(t *testing.T) {
	one := relayDedupeKey(hubJobEventPayload{JobID: "job", Epoch: 1, ReportPath: "report.md"})
	asciiOne := relayDedupeKey(hubJobEventPayload{JobID: "job", Epoch: 0x31, ReportPath: "report.md"})
	// The trailing field is reason: R20T5 widened the key to the same five
	// fields the node outbox and handoffkeep already counted.
	if one != "job\x001\x00report.md\x00" || asciiOne != "job\x0049\x00report.md\x00" || one == asciiOne {
		t.Fatalf("M5: epoch dedupe key is not unambiguous decimal: %q %q", one, asciiOne)
	}
}

func TestR18RelaySubmissionVerificationReturnsOnceThenUnconfirmed(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "herdr.log")
	binary := filepath.Join(dir, "herdr")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho \"$2\" >>\"$R18_HERDR_LOG\"\ncase \"$2\" in read) echo '[Pasted text #1]' ;; esac\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("R18_HERDR_LOG", log)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if defaultHubRelayInject(context.Background(), "test-pane", "one line") {
		t.Fatal("pasted composer reported delivered")
	}
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(b)); strings.Join(got, ",") != "prompt,read,send-keys,read" {
		t.Fatalf("submission verification removed or repeated: %q", b)
	}
}

func TestR18RelayDirectiveValidationAndText(t *testing.T) {
	message, ok := parseHubOutbound([]byte(`{"type":"relay.inject","job_id":"r18","pane":"test-pane","text":"one line"}`))
	if !ok || message.Pane != "test-pane" {
		t.Fatalf("message=%+v ok=%t", message, ok)
	}
	if _, ok := parseHubOutbound([]byte(`{"type":"relay.inject","job_id":"r18","pane":"p","text":"two\nlines"}`)); ok {
		t.Fatal("newline directive accepted")
	}
	text := relayText(hubJobEventPayload{Label: "worker", Host: "host", ReportLastLine: "last", ReportPath: "report.md"})
	if text != "(같은 내용이 두 번 보이면 재실행 금지) [report] worker (host) :: last → report.md" {
		t.Fatalf("text=%q", text)
	}
}
