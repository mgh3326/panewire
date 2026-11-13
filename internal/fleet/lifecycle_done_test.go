package fleet

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func (r *doneRig) storePuts() int {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	n := 0
	for _, c := range r.store.calls {
		if strings.HasPrefix(c, "put ") {
			n++
		}
	}
	return n
}

// "Duplicate" is not a branch of `done`: it is the case where every step has
// its receipt, and it calls hk zero times (no document, no record, no task
// comment), writes no event and relays nothing.
func TestDoneFullDuplicateMakesNoHKCall(t *testing.T) {
	r := newDoneRig(t)
	r.write("same\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	events, _ := r.a.events("j")
	calls, relays := r.hkCalls(), len(r.relays())
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	if got := r.hkCalls(); got != calls {
		t.Fatalf("the duplicate made %d hk calls", got-calls)
	}
	if after, _ := r.a.events("j"); len(after) != len(events) {
		t.Fatalf("the duplicate wrote events: %d -> %d", len(events), len(after))
	}
	if got := len(r.relays()); got != relays {
		t.Fatalf("relays %d -> %d", relays, got)
	}
	if out := r.stdout(); !strings.Contains(out, "duplicate=yes") {
		t.Fatalf("out=%q", out)
	}
}

// A later report (other bytes) is a report of its own: its own document, its
// own job.completed and relay. It never writes the record, which keeps the
// completing report's pin.
func TestDoneLaterReportGetsOwnDocumentAndNeverWritesRecord(t *testing.T) {
	r := newDoneRig(t)
	r.write("first\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	first := r.key("first\n")
	puts := r.storePuts()
	r.write("second\n")
	if e := r.done(); e != nil {
		t.Fatal(e)
	}
	second := r.key("second\n")
	if second == first {
		t.Fatal("fixture: both reports have one key")
	}
	if got := r.storePuts(); got != puts {
		t.Fatalf("the later report wrote the record: %d -> %d puts", puts, got)
	}
	if rec := r.store.record(t, "j"); str(rec, "report_doc") != first || str(rec, "report_sha256") != reportDigest([]byte("first\n")) {
		t.Fatalf("record=%v, want the first report's pin", rec)
	}
	var docs []string
	for _, p := range r.events("job.completed") {
		docs = append(docs, str(p, "document"))
	}
	if !slices.Equal(docs, []string{first, second}) {
		t.Fatalf("job.completed documents=%q, want %q", docs, []string{first, second})
	}
	if got := len(r.relays()); got != 2 {
		t.Fatalf("relays=%d, want one per report", got)
	}
	if got := len(r.events(eventRecordSettled)); got != 1 {
		t.Fatalf("%s receipts=%d, want one", eventRecordSettled, got)
	}
	if got := len(r.events(eventReportPublished)); got != 2 {
		t.Fatalf("%s receipts=%d, want one per report", eventReportPublished, got)
	}
}

// hk takes a result only in the spawned→completed write. A `--result` on a
// record that is already completed without one is terminal_without_result on
// both paths: the interactive done fails closed, a headless done warns and goes
// on, and neither pretends to attach it.
func TestDoneResultOnTerminalWithoutResultOnBothPaths(t *testing.T) {
	for _, headless := range []bool{false, true} {
		name := "interactive"
		if headless {
			name = "headless"
		}
		t.Run(name, func(t *testing.T) {
			r := newDoneRig(t)
			if headless {
				putEvent(t, r.a, "j", "job.spawned", map[string]any{"headless": true, "role": "delegate", "pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a"})
			}
			r.write("answer\n")
			if e := r.done(); e != nil {
				t.Fatal(e)
			}
			findings := filepath.Join(r.a.Jobs, "j", "findings.json")
			if e := os.WriteFile(findings, []byte(`{"findings":[]}`), 0600); e != nil {
				t.Fatal(e)
			}
			e := r.done("--result", findings)
			if headless {
				if e != nil || !strings.Contains(r.stderr(), "reason=terminal_without_result") {
					t.Fatalf("error=%v warnings=%q, want a warning with reason=terminal_without_result", e, r.stderr())
				}
			} else if code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=terminal_without_result") {
				t.Fatalf("error=%v, want terminal_without_result", e)
			}
			if rec := r.store.record(t, "j"); rec["result"] != nil {
				t.Fatalf("record=%v, want no result", rec)
			}
		})
	}
}
