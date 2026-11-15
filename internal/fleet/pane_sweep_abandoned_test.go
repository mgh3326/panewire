package fleet

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The node's own end of a reviewer or consult job that never ran `panewire
// done` (hk 4747): a pane both herdr lists lost, and a live pane whose result
// never arrived before the request's time limit plus a grace. The fixture's
// base time is minute 0 (12:00), and a reviewer's default limit is one hour.

// endReasons lists the job's job.lost reasons in ledger order.
func endReasons(t *testing.T, a *App, job string) []string {
	t.Helper()
	events, err := a.events(job)
	if err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, event := range events {
		if event.Kind == "job.lost" {
			reasons = append(reasons, str(event.Payload, "reason"))
		}
	}
	return reasons
}

func (f *paneSweepFixture) writeJobFile(job, name, body string) {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.a.Jobs, job, name), []byte(body), 0600); err != nil {
		f.t.Fatal(err)
	}
}

// askQuestion records a lane event the pane asked, at the given time.
func (f *paneSweepFixture) askQuestion(pane string, at time.Time) {
	f.t.Helper()
	dir := filepath.Join(filepath.Dir(f.a.Jobs), "events-lane")
	if err := os.MkdirAll(dir, 0700); err != nil {
		f.t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"kind": "lane.event", "pane_id": pane, "text": "which approach?", "event_id": "q1", "notify_type": "decision"})
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(dir, "0001-question.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chtimes(path, at, at); err != nil {
		f.t.Fatal(err)
	}
}

// A reviewer whose pane both herdr lists lost is ended as pane_gone and reaped;
// nothing is closed, because there is no pane.
func TestPaneSweepEndsAReviewerWhosePaneIsGone(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	delete(f.status, pane)
	f.at(1)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != SweepPaneGone {
		t.Fatalf("job.lost reasons = %v", got)
	}
	rec := f.store.record(t, "r")
	if str(rec, "state") != "lost" || str(rec, "end_reason") != SweepPaneGone {
		t.Fatalf("hk record = %v", rec)
	}
	reaped := f.reaped("r")
	if reaped == nil || str(reaped, "reason") != SweepPaneGone {
		t.Fatalf("reap = %v", reaped)
	}
	if len(f.closed()) != 0 {
		t.Fatalf("a close ran for a pane herdr does not list: %v", f.closed())
	}
	if !f.infoHas("node ended a reviewer or consult job", "job=r", "reason="+SweepPaneGone) {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A reviewer's valid posted result is the hub's end even when its pane is gone:
// the node never records the finished review as an abandonment.
func TestPaneSweepLeavesAFinishedReviewerWhosePaneIsGone(t *testing.T) {
	for name, file := range map[string][2]string{
		"findings": {"findings.json", `{"findings":[]}`},
		"report":   {"report.md", "# findings\n"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			pane := f.job("j", "reviewer", "")
			f.writeJobFile("j", file[0], file[1])
			delete(f.status, pane)
			f.at(1)
			if got := endReasons(t, f.a, "j"); len(got) != 0 {
				t.Fatalf("a finished review's pane loss was recorded: %v", got)
			}
			if state := str(f.store.record(t, "j"), "state"); state != "spawned" {
				t.Fatalf("hk record was ended: %v", f.store.record(t, "j"))
			}
			if f.reaped("j") != nil {
				t.Fatal("a finished review was reaped as gone")
			}
		})
	}
}

// A consult's posted result stops the quiet timeout but not a proven pane loss:
// no other pass ends a consult, and its consumer reads a lost record as a
// failed consult, so the pane loss is cleaned up as an abandonment.
func TestPaneSweepEndsAConsultWhoseGonePaneHadAPostedResult(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("c", "consult", "")
	f.writeJobFile("c", "report.md", "# the answer\n")
	delete(f.status, pane)
	f.at(1)
	if got := endReasons(t, f.a, "c"); len(got) != 1 || got[0] != SweepPaneGone {
		t.Fatalf("job.lost reasons = %v", got)
	}
	if f.reaped("c") == nil {
		t.Fatal("the consult whose pane vanished was not reaped")
	}
}

// A live reviewer that posted no result is ended once the request's time limit
// (one hour by default) plus the grace passed, and the existing one-hour close
// then takes its pane.
func TestPaneSweepEndsAReviewerWithNoResultPastTheLimit(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	f.at(74)
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("a reviewer inside its limit was ended: %v", got)
	}
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("job.lost reasons = %v", got)
	}
	rec := f.store.record(t, "r")
	if str(rec, "state") != "lost" || str(rec, "end_reason") != ReviewerNoResultTimeout {
		t.Fatalf("hk record = %v", rec)
	}
	if !f.infoHas("node ended a reviewer or consult job", "job=r", "reason="+ReviewerNoResultTimeout) {
		t.Fatalf("info lines = %q", f.info)
	}
	if f.reaped("r") != nil || len(f.closed()) != 0 {
		t.Fatalf("a timed-out reviewer's pane closed before its hour: closed=%v reaped=%v", f.closed(), f.reaped("r"))
	}
	// The existing one-hour close then applies. The end's ledger event is
	// stamped one second after the pass (testApp's arbiter clock), and the
	// deadline reads that stamp with >=, so minute 136 is 59m59s later and the
	// pane closes at 137.
	for _, minute := range []int{135, 136} {
		f.at(minute)
		if len(f.closed()) != 0 {
			t.Fatalf("pane closed %d minutes after its end: %v", minute-76, f.closed())
		}
	}
	f.at(137)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("pane was not closed after its hour: %v", got)
	}
	if f.reaped("r") == nil {
		t.Fatal("the timed-out reviewer was not reaped")
	}
	if got := endReasons(t, f.a, "r"); len(got) != 1 {
		t.Fatalf("the end was written more than once: %v", got)
	}
}

// A consult is ended the same way.
func TestPaneSweepEndsAConsultWithNoResultPastTheLimit(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("c", "consult", "")
	f.status[pane] = "working"
	f.at(76)
	if got := endReasons(t, f.a, "c"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("job.lost reasons = %v", got)
	}
}

// A --keep reviewer is ended the same way, and its live pane then keeps its
// day (decision 268).
func TestPaneSweepKeepsATimedOutReviewersPaneForItsDay(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{"keep": true})
	f.status[pane] = "working"
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("job.lost reasons = %v", got)
	}
	f.at(137)
	if len(f.closed()) != 0 {
		t.Fatalf("a --keep pane closed inside its day: %v", f.closed())
	}
	f.at(1517)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("a --keep pane did not close a day after its end: %v", got)
	}
}

// A reviewer that posted a result is left to the hub's posted-result end
// (hub_spawn_reviewer_end.go): no result needs a second end.
func TestPaneSweepLeavesAReviewerThatPostedItsResult(t *testing.T) {
	for name, file := range map[string][2]string{
		"findings object": {"findings.json", `{"findings":[]}`},
		"report":          {"report.md", "# findings\n"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			pane := f.job("r", "reviewer", "")
			f.status[pane] = "working"
			f.writeJobFile("r", file[0], file[1])
			f.at(76)
			if got := endReasons(t, f.a, "r"); len(got) != 0 {
				t.Fatalf("a reviewer with a result was ended: %v", got)
			}
			if state := str(f.store.record(t, "r"), "state"); state != "spawned" {
				t.Fatalf("a reviewer with a result was ended: %v", f.store.record(t, "r"))
			}
		})
	}
}

// A findings file that is no posted result — JSON null, another shape, not
// JSON, over the hub reader's bound, or not UTF-8 — is absent, as the hub's
// result reader counts it: the job is ended at its limit.
func TestPaneSweepEndsAReviewerWhoseFindingsAreNotAPostedResult(t *testing.T) {
	for name, body := range map[string]string{
		"JSON null":         "null\n",
		"no findings array": `{"verdict":"ok"}`,
		"not JSON":          "findings: none\n",
		"too large":         `{"findings":[],"pad":"` + strings.Repeat("x", paneSweepFindingsMaxBytes) + `"}`,
		"invalid UTF-8":     "{\"findings\":[],\"note\":\"\xff\"}",
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			pane := f.job("r", "reviewer", "")
			f.status[pane] = "working"
			f.writeJobFile("r", "findings.json", body)
			f.at(76)
			if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
				t.Fatalf("a findings file that is no result was read as one: %v", got)
			}
		})
	}
}

// An end whose hk write landed but whose ledger event did not is repaired on
// the next pass even when a result arrived meanwhile: hk already holds the
// end, so no other path can finish it and the ledger would stay live.
func TestPaneSweepRepairsAnInterruptedEnd(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	f.writeJobFile("r", "findings.json", `{"findings":[]}`)
	f.store.records["r"]["state"] = "lost"
	f.store.records["r"]["end_reason"] = ReviewerNoResultTimeout
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("job.lost reasons = %v", got)
	}
	if str(f.store.record(t, "r"), "end_reason") != ReviewerNoResultTimeout {
		t.Fatalf("the repair rewrote hk: %v", f.store.record(t, "r"))
	}
	if f.reaped("r") != nil {
		t.Fatal("a repaired timeout end was reaped as gone")
	}
}

// An ended hk record is repaired with its own reason even when the current
// observation names another: the ledger must agree with hk, and the stored
// reason decides the reap.
func TestPaneSweepRepairsWithTheStoredReason(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	delete(f.status, pane) // gone now; hk holds the earlier timeout end
	f.store.records["r"]["state"] = "lost"
	f.store.records["r"]["end_reason"] = ReviewerNoResultTimeout
	f.at(1)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("job.lost reasons = %v", got)
	}
	if f.reaped("r") != nil {
		t.Fatal("a repaired timeout end was reaped as gone in the same pass")
	}
}

// A reviewer's result written while the pass holds the operation lock cancels
// the fresh end: the hub's posted-result pass owns it.
func TestPaneSweepKeepsAReviewerWhoseResultLandsUnderTheLock(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	delete(f.status, pane)
	paneSweepResultReadHook = func(job string) {
		if job == "r" {
			f.writeJobFile("r", "findings.json", `{"findings":[]}`)
		}
	}
	t.Cleanup(func() { paneSweepResultReadHook = nil })
	f.at(1)
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("a finished review's pane loss was recorded: %v", got)
	}
}

// A consult's result landing under the lock does not hold its proven pane loss:
// the consult is cleaned up as an abandonment either way.
func TestPaneSweepEndsAConsultWhoseResultLandsUnderTheLock(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("c", "consult", "")
	delete(f.status, pane)
	paneSweepResultReadHook = func(job string) {
		if job == "c" {
			f.writeJobFile("c", "report.md", "# the answer\n")
		}
	}
	t.Cleanup(func() { paneSweepResultReadHook = nil })
	f.at(1)
	if got := endReasons(t, f.a, "c"); len(got) != 1 || got[0] != SweepPaneGone {
		t.Fatalf("job.lost reasons = %v", got)
	}
}

// The time limit the spawn record carries wins over the role's default.
func TestPaneSweepUsesTheSpawnRecordsTimeLimit(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{"expires_at": "2026-03-01T15:00:00Z"})
	f.status[pane] = "working"
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("the default limit was used although the spawn record named one: %v", got)
	}
	f.at(196)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != ReviewerNoResultTimeout {
		t.Fatalf("the spawn record's limit was not used: %v", got)
	}
}

// A job whose pane asks an unanswered question is kept (#563), whatever its
// age.
func TestPaneSweepKeepsAReviewerWithAnOpenQuestion(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	f.askQuestion(pane, time.Date(2026, 3, 1, 12, 1, 0, 0, time.UTC))
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("a reviewer with an open question was ended: %v", got)
	}
	if state := str(f.store.record(t, "r"), "state"); state != "spawned" {
		t.Fatalf("a reviewer with an open question was ended: %v", f.store.record(t, "r"))
	}
}

// An implementer is never ended here: its end belongs to its task or its owner.
func TestPaneSweepNeverEndsAnImplementer(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("w", "implementer", "42")
	f.status[pane] = "working"
	f.tasks["42"] = "developing"
	f.at(76)
	if got := endReasons(t, f.a, "w"); len(got) != 0 {
		t.Fatalf("an implementer was ended by the reviewer rule: %v", got)
	}
	if state := str(f.store.record(t, "w"), "state"); state != "spawned" {
		t.Fatalf("an implementer's record was written: %v", f.store.record(t, "w"))
	}
}

// An announced quota stop is the hub's hand-over, not this node's end.
func TestPaneSweepLeavesAnAnnouncedQuotaStop(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.lost", map[string]any{"pane_id": "p-r", "reason": "quota_stop", "pool": "pool-a"})
	f.at(1)
	for _, reason := range endReasons(t, f.a, "r") {
		if reason == ReviewerNoResultTimeout || reason == SweepPaneGone {
			t.Fatalf("the node ended an announced quota stop again: %v", endReasons(t, f.a, "r"))
		}
	}
	if end := str(f.store.record(t, "r"), "end_reason"); end != "" {
		t.Fatalf("hk record was ended by the reviewer rule: %v", f.store.record(t, "r"))
	}
}
