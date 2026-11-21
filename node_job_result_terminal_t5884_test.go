package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// job.result reports the newest terminal ledger event as terminal_event (hk 5884).
func TestNodeJobResultTerminalEvent(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	dir := filepath.Dir(report)
	events := filepath.Join(dir, "events")
	write := func(t *testing.T, name, body string) {
		t.Helper()
		if err := os.MkdirAll(events, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	terminal := func(t *testing.T) nodeJobResult {
		t.Helper()
		got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
		if code != "" {
			t.Fatalf("code=%q", code)
		}
		result := got.(nodeJobResult)
		if result.TerminalEvent == nil {
			t.Fatalf("terminal_event unknown: %+v", result)
		}
		return result
	}

	// No events directory: no terminal event.
	if got := terminal(t); *got.TerminalEvent != "" || got.Completed {
		t.Fatalf("no ledger: %+v", got)
	}

	write(t, "00001-job.claim.json", `{"kind":"job.claim"}`)
	write(t, "00002-job.brief_sent.json", `{"kind":"job.brief_sent"}`)
	if got := terminal(t); *got.TerminalEvent != "" {
		t.Fatalf("bookkeeping only: terminal_event=%q", *got.TerminalEvent)
	}

	write(t, "00003-job.completed.json", `{"kind":"job.completed"}`)
	if got := terminal(t); *got.TerminalEvent != "job.completed" || !got.Completed {
		t.Fatalf("completed only: %+v", got)
	}

	// The newest terminal event wins even when an earlier one is a completion.
	write(t, "00004-job.lost.json", `{"kind":"job.lost"}`)
	if got := terminal(t); *got.TerminalEvent != "job.lost" {
		t.Fatalf("lost after completed: terminal_event=%q", *got.TerminalEvent)
	}

	// A hub revocation record carries type, not kind; it still counts.
	write(t, "00005-job.revoked.json", `{"type":"job.revoked","epoch":1}`)
	write(t, "00006-job.reaped.json", `{"kind":"job.reaped"}`)
	if got := terminal(t); *got.TerminalEvent != "job.reaped" {
		t.Fatalf("revoked then reaped: terminal_event=%q", *got.TerminalEvent)
	}

	// A non-terminal event after the terminal one does not replace it.
	write(t, "00007-job.claim.json", `{"kind":"job.claim"}`)
	if got := terminal(t); *got.TerminalEvent != "job.reaped" {
		t.Fatalf("claim after reaped: terminal_event=%q", *got.TerminalEvent)
	}
}

// A ledger the shared reader cannot parse leaves terminal_event unknown, not
// empty, and job.result still answers the rest of the job.
func TestNodeJobResultTerminalEventUnreadable(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	events := filepath.Join(filepath.Dir(report), "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00001-job.completed.json"), []byte(`{"kind":"job.completed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00002-job.note.txt"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	got, code := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if code != "" {
		t.Fatalf("code=%q", code)
	}
	result := got.(nodeJobResult)
	if result.TerminalEvent != nil || !result.Completed || !result.Found {
		t.Fatalf("%+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil || strings.Contains(string(encoded), "terminal_event") {
		t.Fatalf("unknown terminal_event must be omitted: %s err=%v", encoded, err)
	}
}

func TestNodeJobResultTerminalEventValidation(t *testing.T) {
	empty := ""
	for _, kind := range []string{"job.completed", "job.lost", "job.reaped", "job.revoked", ""} {
		kind := kind
		if !validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", TerminalEvent: &kind}) {
			t.Fatalf("terminal_event %q should be valid", kind)
		}
	}
	bad := "job.claim"
	for _, kind := range []string{bad, "job.spawned"} {
		kind := kind
		if validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", TerminalEvent: &kind}) {
			t.Fatalf("terminal_event %q should be rejected", kind)
		}
	}
	if validNodeJobResult(nodeJobResult{TerminalEvent: &empty}) {
		t.Fatal("a missing job cannot carry terminal_event")
	}
	if !validNodeJobResult(nodeJobResult{}) {
		t.Fatal("a missing job without terminal_event is valid")
	}
}

// A job whose newest end record is a handoff or a settled record reports that
// kind, not an earlier completion or "" (hk 5884 review).
func TestNodeJobResultTerminalEventEndKinds(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	events := filepath.Join(filepath.Dir(report), "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name, kind string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(events, name), []byte(`{"kind":"`+kind+`"}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("00001-job.completed.json", "job.completed")
	write("00002-job.handed_off.json", "job.handed_off")
	got, _ := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if result := got.(nodeJobResult); result.TerminalEvent == nil || *result.TerminalEvent != "job.handed_off" {
		t.Fatalf("completed then handed_off: %+v", result)
	}
	write("00003-job.record_settled.json", "job.record_settled")
	got, _ = client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
	if result := got.(nodeJobResult); result.TerminalEvent == nil || *result.TerminalEvent != "job.record_settled" {
		t.Fatalf("record_settled: %+v", result)
	}
	for _, kind := range []string{"job.handed_off", "job.completion", "job.record_settled"} {
		kind := kind
		if !validNodeJobResult(nodeJobResult{Found: true, FindingsError: "missing", TerminalEvent: &kind}) {
			t.Fatalf("%q should be valid", kind)
		}
	}
}

// terminal_event and the close target read one end-kind set. For every kind
// the two callers agree on whether the job ended, and job.joined is among
// the ended kinds (hk 5884 decision).
func TestNodeJobResultTerminalEventAgreesWithCloseTarget(t *testing.T) {
	client, report := nodeReapJobDir(t, "job-a", "report.md")
	events := filepath.Join(filepath.Dir(report), "events")
	jobRoot, err := os.OpenRoot(filepath.Dir(report))
	if err != nil {
		t.Fatal(err)
	}
	defer jobRoot.Close()
	ledgerWith := func(t *testing.T, kind string) {
		t.Helper()
		if err := os.RemoveAll(events); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(events, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(events, "00001-"+kind+".json"), []byte(`{"kind":"`+kind+`"}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"job.completed", "job.completion", "job.record_settled", "job.joined", "job.revoked", "job.handed_off", "job.escalate", "job.reaped", "job.lost"} {
		ledgerWith(t, kind)
		target, err := readNodeCloseTarget(jobRoot)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
		terminal := *got.(nodeJobResult).TerminalEvent
		if !target.ended || terminal != kind {
			t.Fatalf("%s: ended=%v terminal_event=%q, want ended and terminal_event=%s", kind, target.ended, terminal, kind)
		}
	}
	// Kinds outside the set are not an end on either side.
	for _, kind := range []string{"job.aborted", "worker.complete", "job.claim", "job.spawned"} {
		ledgerWith(t, kind)
		target, err := readNodeCloseTarget(jobRoot)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := client.nodeJobResult(nodeJobResultArgs{Job: "job-a"})
		if target.ended || *got.(nodeJobResult).TerminalEvent != "" {
			t.Fatalf("%s: ended=%v, want neither end", kind, target.ended)
		}
	}
}
