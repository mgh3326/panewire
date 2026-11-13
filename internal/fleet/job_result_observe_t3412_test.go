package fleet

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// jobResultEvents collects the job_result fleet events a test produces.
func jobResultEvents(a *App) *[]fleetevent.Event {
	events := &[]fleetevent.Event{}
	a.EventSink = func(ev fleetevent.Event) {
		if ev.Kind == "job_result" {
			*events = append(*events, ev)
		}
	}
	return events
}

// A done --result on a terminal record without a result is refused (hk cannot
// take the result late), so it emits no job_result attached and no Sentry
// event: nothing was attached.
func TestDuplicateDoneWithoutStoredResultEmitsNoAttach(t *testing.T) {
	a, report, _ := watcherCompletes(t)
	events := jobResultEvents(a)
	var warnings []string
	a.SentryWarning = func(fingerprint []string, _ string, _ map[string]string) {
		warnings = append(warnings, strings.Join(fingerprint, "|"))
	}
	findings := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, findings, `{"findings":[{"severity":"high"}]}`)
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", findings}); code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=terminal_without_result") {
		t.Fatalf("error=%v, want terminal_without_result", e)
	}
	if len(*events) != 0 {
		t.Fatalf("job_result events=%+v", *events)
	}
	if len(warnings) != 0 {
		t.Fatalf("refused result sent Sentry warnings: %v", warnings)
	}
}

// Acceptance (c): a conflicting result emits job_result conflict plus one
// warning-level Sentry event.
func TestDuplicateDoneConflictEmitsJobResultAndSentry(t *testing.T) {
	a, report, store := watcherCompletes(t)
	store.mu.Lock()
	store.records["j"]["result"] = map[string]any{"source": "stored"}
	store.mu.Unlock()
	events := jobResultEvents(a)
	var warnings []string
	a.SentryWarning = func(fingerprint []string, _ string, _ map[string]string) {
		warnings = append(warnings, strings.Join(fingerprint, "|"))
	}
	findings := filepath.Join(a.Jobs, "j", "findings.json")
	writeReport(t, findings, `{"source":"later"}`)
	if e := a.lifecycle(context.Background(), "done", []string{"j", "--report", report, "--result", findings}); code(e) != exitStateConflict || !strings.Contains(e.Error(), "reason=result_conflict") {
		t.Fatalf("error=%v, want strict result conflict", e)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "conflict" {
		t.Fatalf("job_result events=%+v", *events)
	}
	if len(warnings) != 1 || warnings[0] != "job-result-conflict" {
		t.Fatalf("Sentry fingerprints=%v", warnings)
	}
}

// A reviewer's done that carries no result is recorded as missing.
func TestReviewerDoneWithoutResultEmitsJobResultMissing(t *testing.T) {
	a, _, report := watchFixture(t)
	seedSpawnedRecord(t, a, "j")
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "report": report, "role": "reviewer"})
	writeReport(t, report, "review found nothing\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	events := jobResultEvents(a)
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "missing" || (*events)[0].Reason != "no_result" {
		t.Fatalf("job_result events=%+v", *events)
	}
}
