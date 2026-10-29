package panewire

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func r19dEscalationFixture(t *testing.T) []byte {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("testdata", "r19d-escalate-fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestR19dLongEscalationIsAcceptedRelayedAndMarked(t *testing.T) {
	payload := r19dEscalationFixture(t)
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil || len(raw) != 12 {
		t.Fatalf("synthetic fixture must retain the 12-field escalation shape: err=%v fields=%d", err, len(raw))
	}
	event, ok := decodeHubJobEscalationPayload(payload)
	if !ok || len([]rune(event.Question)) != hubRelayPayloadTextLimit {
		t.Fatalf("long escalation was not accepted and rune-truncated: %+v ok=%t", event, ok)
	}
	routes := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(routes, []byte(`{"lanes":{"worker-lane":{"machine":"synthetic-node","pane":"synthetic-child","parent":"captain-lane"},"captain-lane":{"machine":"synthetic-node","pane":"synthetic-parent"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "synthetic-node": "node-token"}, ReportRelayPath: routes, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	h.connect("synthetic-node", "test", "synthetic-remote", agent, true)
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: "job.escalate", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	h.handleAgentMessage("synthetic-node", "synthetic-remote", agent, wire)
	select {
	case relay := <-agent.relays:
		if relay.Pane != "synthetic-parent" || !strings.Contains(relay.Text, "… → /synthetic/inbox/jobs/synthetic-r19d-job/report.md") || !strings.HasSuffix(relay.Text, "(전문: /synthetic/inbox/jobs/synthetic-r19d-job/report.md)") || !validHubNoteText(relay.Text) {
			t.Fatalf("relay=%+v", relay)
		}
	default:
		t.Fatal("accepted long escalation did not queue relay.inject")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "field=question") || !strings.Contains(logs.String(), "job=synthetic-r19d-job") {
		t.Fatalf("hub did not log fallback truncation: %s", logs.String())
	}
	if got := h.UnknownMessageCount(); got != 0 {
		t.Fatalf("accepted long escalation counted as unknown: %d", got)
	}
}

func TestR19dNodeCompactsEscalationAndUsesEventPath(t *testing.T) {
	root := t.TempDir()
	eventsDir := filepath.Join(root, "jobs", "synthetic-r19d-job", "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		t.Fatal(err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(r19dEscalationFixture(t), &fixture); err != nil {
		t.Fatal(err)
	}
	fixture["report_path"] = ""
	fixture["reason"] = strings.Repeat("r", 300)
	fixture["report_last_line"] = strings.Repeat("l", 300)
	fixture["question"] = "first line\n" + fixture["question"].(string)
	fixture["type"] = "job.escalate"
	contents, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	eventPath := filepath.Join(eventsDir, "00001-job.escalate.json")
	if err := os.WriteFile(eventPath, contents, 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}}
	events := client.jobCompletionEvents()
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	got, ok := decodeHubJobEscalationPayload(events[0].Payload)
	if !ok || got.ReportPath != eventPath || len([]rune(got.Question)) != hubRelayPayloadTextLimit || strings.ContainsAny(got.Question, "\r\n") || len([]rune(got.Reason)) != hubRelayPayloadTextLimit || len([]rune(got.ReportLastLine)) != hubRelayPayloadTextLimit {
		t.Fatalf("node payload was not compact and safe: %+v ok=%t", got, ok)
	}
	var logs bytes.Buffer
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "synthetic-node": "node-token"}, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	h.connect("synthetic-node", "test", "synthetic-remote", agent, true)
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: "job.escalate", Payload: events[0].Payload})
	if err != nil {
		t.Fatal(err)
	}
	h.handleAgentMessage("synthetic-node", "synthetic-remote", agent, wire)
	if logs.Len() != 0 {
		t.Fatalf("node-side compaction unexpectedly required hub fallback: %s", logs.String())
	}
}

func TestR19dQuestionNewlineNormalizesAndMalformedInputStillRejects(t *testing.T) {
	payload := r19dEscalationFixture(t)
	var fixture map[string]any
	if err := json.Unmarshal(payload, &fixture); err != nil {
		t.Fatal(err)
	}
	fixture["question"] = "synthetic first\nsynthetic second"
	normalized, _ := json.Marshal(fixture)
	event, ok := decodeHubJobEscalationPayload(normalized)
	if !ok || event.Question != "synthetic first synthetic second" {
		t.Fatalf("question newline was not normalized: %+v ok=%t", event, ok)
	}
	fixture["reason"] = "bad\nreason"
	malformed, _ := json.Marshal(fixture)
	if _, ok := decodeHubJobEscalationPayload(malformed); ok {
		t.Fatal("malformed newline-bearing reason accepted")
	}
	fixture["reason"] = "needs a parent decision"
	fixture["question"] = strings.Repeat("가", hubRelayPayloadTextLimit+1)
	runePayload, _ := json.Marshal(fixture)
	event, ok = decodeHubJobEscalationPayload(runePayload)
	if !ok || !utf8.ValidString(event.Question) || len([]rune(event.Question)) != hubRelayPayloadTextLimit {
		t.Fatalf("question truncation split a rune: %+v ok=%t", event, ok)
	}
}
