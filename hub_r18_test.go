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
