package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
