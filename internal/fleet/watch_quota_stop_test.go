package fleet

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// quotaStopFixture is a live job of the given role whose pane reports status
// and shows screen. It records every herdr call so a test can prove the pane
// was only read, never typed into.
func quotaStopFixture(t *testing.T, role, status, screen string) (*App, *fakeRunner, *fakeHK, string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	h := newFakeHK(t)
	h.claimJob("j", float64(42), "test-host")
	h.records["j"]["state"] = "spawned"
	a.JobStore = h.client()
	f.Do = func(c Command) Result {
		if c.Name != "herdr" {
			return Result{}
		}
		switch {
		case strings.Join(c.Args, " ") == "agent list":
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","agent_status":%q,"state_change_seq":7}]}}`, status)}
		case len(c.Args) > 2 && c.Args[0] == "agent" && c.Args[1] == "read":
			return Result{Out: screen}
		}
		t.Errorf("unexpected herdr call %v", c.Args)
		return Result{}
	}
	report := filepath.Join(a.Jobs, "j", "report.md")
	// As in production: the claim always says worker; the spawn receipt names
	// the role.
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "report": report, "role": role})
	seedLedger(t, a, "j")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	return a, f, h, report
}

// announceQuotaStopEvent records the job.lost a quota-stop announcement leaves
// for the hub, as the manager's hand-over of a worker does.
func announceQuotaStopEvent(t *testing.T, a *App, pool string) {
	t.Helper()
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": pool}); e != nil {
		t.Fatal(e)
	}
}

// legacyIntent records the close intent an older watcher left for a reviewer
// stopped on pool: the watcher no longer screen-detects a reviewer (hk 5690), but
// a ledger that holds such an intent is still reconciled, and the recovery tests
// start from it.
func legacyIntent(t *testing.T, a *App, pool string, in time.Duration) {
	t.Helper()
	if e := a.recordQuotaIntent(context.Background(), Job{ID: "j", Pane: "w:p1"}, quotastop.Stop{Pool: pool, Reset: a.Now().Add(in)}); e != nil {
		t.Fatal(e)
	}
}

func devinStopScreen(t *testing.T) string {
	t.Helper()
	b, e := os.ReadFile("../../testdata/quota-stop/devin-something-went-wrong.txt")
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

// devinLongStopScreen is devinStopScreen with its reset two hours away: beyond
// the reviewer wait (hk 5355), so the watcher ends the reviewer at once. The
// five-minute screen is a short reset the watcher holds for the manager.
func devinLongStopScreen(t *testing.T) string {
	t.Helper()
	screen := strings.Replace(devinStopScreen(t), "reset in 5 minutes", "reset in 2 hours", 1)
	if !strings.Contains(screen, "reset in 2 hours") {
		t.Fatal("devin fixture has no five-minute reset to lengthen")
	}
	return screen
}

func lostPayload(t *testing.T, a *App) map[string]any {
	t.Helper()
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	for _, ev := range evs {
		if ev.Kind == "job.lost" {
			return ev.Payload
		}
	}
	return nil
}

func TestWatcherQuotaStopLeavesOtherPanesLive(t *testing.T) {
	for name, tc := range map[string]struct{ role, status, screen string }{
		"consult role":               {"consult", "idle", ""},
		"working pane":               {"implementer", "working", ""},
		"idle, not a quota":          {"implementer", "idle", "I finished the task.\n❭ Ask Devin to build features, fix bugs, or work on your code\n"},
		"stop, then output":          {"implementer", "idle", "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes\nresumed and wrote the report\n"},
		"working pane, claude limit": {"implementer", "working", "You've hit your session limit · resets unknown\n"},
	} {
		t.Run(name, func(t *testing.T) {
			screen := tc.screen
			if screen == "" {
				screen = devinStopScreen(t)
			}
			a, _, h, _ := quotaStopFixture(t, tc.role, tc.status, screen)
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if got := str(h.record(t, "j"), "state"); got == "lost" {
				t.Fatal("job was ended as lost")
			}
			if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
				t.Fatal("job.lost recorded")
			}
			if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
				t.Fatalf("live job lost its lease: %s %v", st, pools)
			}
		})
	}
}

// A report with the end signal is today's done path even when the pane still
// shows the stop screen.
func TestWatcherQuotaStopWithReportKeepsDonePath(t *testing.T) {
	a, f, h, report := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	writeReport(t, report, "final\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if records, relays := completions(t, a, f); records != 1 || relays != 1 {
		t.Fatalf("records=%d relays=%d, want one completion", records, relays)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
		t.Fatal("job with a report was ended as lost")
	}
	if got := str(h.record(t, "j"), "state"); got != "completed" {
		t.Fatalf("hk state=%q, want completed", got)
	}
}

// hk 3334: the watcher no longer ends an implementer on a quota stop; the
// quota-stop manager waits for the reset or hands it over. Dropping this split
// (putting implementer back into quotaStopRoles) fails here.
func TestWatcherQuotaStopLeavesWorkerAndTesterToTheManager(t *testing.T) {
	for _, role := range []string{"implementer"} {
		t.Run(role, func(t *testing.T) {
			a, _, h, _ := quotaStopFixture(t, role, "idle", devinStopScreen(t))
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if got := str(h.record(t, "j"), "state"); got == "lost" {
				t.Fatal("the watcher ended an implementer on a quota stop")
			}
			if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
				t.Fatal("job.lost recorded for an implementer")
			}
			if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
				t.Fatalf("a live worker lost its lease: %s %v", st, pools)
			}
		})
	}
}

// The mergewake contract (hk 3335): replaced_by is the LAST key=value token of
// end_reason and the successor id fits [A-Za-z0-9._-]{1,128}.
func TestQuotaStopEndReasonFormatIsPinned(t *testing.T) {
	reset := time.Date(2026, 11, 7, 3, 4, 5, 0, time.FixedZone("KST", 9*3600))
	for _, tc := range []struct {
		name, key, job string
		reset          time.Time
		want           string
	}{
		{"replaced with reset", "replaced_by", "mwr-1-p1-q1", reset, "quota_stop pool=claude reset_at=2026-11-06T18:04:05Z replaced_by=mwr-1-p1-q1"},
		{"replaced, reset unknown", "replaced_by", "mwr-1-p1-q1-q2", time.Time{}, "quota_stop pool=claude replaced_by=mwr-1-p1-q1-q2"},
		{"handed over", "handed_over_to", "pw-x-q1", time.Time{}, "quota_stop pool=claude handed_over_to=pw-x-q1"},
		{"no successor", "", "", reset, "quota_stop pool=claude reset_at=2026-11-06T18:04:05Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := QuotaStopEndReason("claude", tc.reset, tc.key, tc.job)
			if got != tc.want {
				t.Fatalf("end_reason=%q, want %q", got, tc.want)
			}
			if tc.job == "" {
				return
			}
			fields := strings.Fields(got)
			last := fields[len(fields)-1]
			if last != tc.key+"="+tc.job || !regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString(strings.TrimPrefix(last, tc.key+"=")) {
				t.Fatalf("%q is not the last key=value token of %q", tc.key, got)
			}
		})
	}
}

// A worker the manager handed over announces job.lost reason quota_stop
// (AnnounceQuotaStop) and leaves its hk record to the hub. The watcher does not
// end it at once and does not return early (which would leave its quota
// leased): when the hub does not replace it, the watcher ends it after the
// grace, with the announced pool and reset, and releases the quota.
func TestWatcherEndsAnnouncedHandOverAfterGraceWhenNobodyReplacesIt(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	reset := a.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": "claude", "reset_at": reset}); e != nil {
		t.Fatal(e)
	}
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if a.Now().Sub(started) < 2*time.Minute {
		t.Fatalf("the watcher ended the handed-over job after %v, before the grace", a.Now().Sub(started))
	}
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || str(rec, "end_reason") != "quota_stop pool=claude reset_at="+reset {
		t.Fatalf("hk record %v", rec)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("quota not released: %s %v", st, pools)
	}
}

// When the hub replaced the job it closed the pane and wrote the hk record;
// the watcher then sees the pane gone, leaves that record alone and releases
// the quota.
func TestWatcherReleasesQuotaWhenTheHubClosedTheHandedOverPane(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 10 * time.Minute
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": "claude"}); e != nil {
		t.Fatal(e)
	}
	h.records["j"]["state"] = "lost"
	h.records["j"]["end_reason"] = "quota_stop pool=claude replaced_by=j-q1"
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[]}}`}
		}
		return do(c)
	}
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if a.Now().Sub(started) < 10*time.Minute {
		t.Fatalf("the watcher ended the job %v after the pane vanished, before the grace: the generic lost path wrote the record without the successor pointer", a.Now().Sub(started))
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=claude replaced_by=j-q1" {
		t.Fatalf("end_reason rewritten: %q", got)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("quota not released: %s %v", st, pools)
	}
}

// A pane that goes back to work during the hand-off grace is not stopped any
// more: the job is live again (job.reprompted), the announced job.lost is not
// adopted a second time, the grace does not restart, and the quota stays leased.
func TestWatcherPaneResumedDuringGraceKeepsTheJobLive(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	announceQuotaStopEvent(t, a, "devin-free")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	lists := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			status, seq := "idle", 7
			if lists > 2 {
				status, seq = "working", 8
			}
			return Result{Out: fmt.Sprintf(`{"result":{"agents":[{"pane_id":"w:p1","agent_status":%q,"state_change_seq":%d}]}}`, status, seq)}
		}
		return do(c)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	kinds := eventKindsOf(t, a, "j")
	count := func(kind string) (n int) {
		for _, k := range kinds {
			if k == kind {
				n++
			}
		}
		return
	}
	if count("job.lost") != 1 || count("job.reprompted") != 1 {
		t.Fatalf("events %v, want one job.lost and one job.reprompted (no restarted grace)", kinds)
	}
	if got := str(h.record(t, "j"), "state"); got == "lost" {
		t.Fatal("a job whose pane went back to work was ended as lost")
	}
	if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
		t.Fatalf("a live job lost its lease: %s %v", st, pools)
	}
}

// A pane that vanishes during the grace but whose record the hub never wrote
// (the replacement did not happen) is ended after the grace with the plain
// quota_stop end_reason, not by the generic missing-pane path.
func TestWatcherVanishedPaneWithoutHubRecordEndsAfterGraceWithQuotaReason(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 4 * time.Minute
	announceQuotaStopEvent(t, a, "devin-free")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	lists := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			if lists++; lists > 2 {
				return Result{Out: `{"result":{"agents":[]}}`}
			}
		}
		return do(c)
	}
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if a.Now().Sub(started) < 4*time.Minute {
		t.Fatalf("ended after %v, before the grace", a.Now().Sub(started))
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
		t.Fatalf("hk record %v", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota not released: %s", st)
	}
}

// A stop announced close to the ordinary completion deadline is still watched
// through its grace: the fallback must end the job and release its lease.
func TestWatcherQuotaHandOffOutlivesTheCompletionDeadline(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	announceQuotaStopEvent(t, a, "devin-free")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "30")
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if a.Now().Sub(started) < 2*time.Minute {
		t.Fatalf("the completion deadline ended the watch after %v, inside the grace", a.Now().Sub(started))
	}
	if str(h.record(t, "j"), "state") != "lost" {
		t.Fatalf("hk record %v", h.record(t, "j"))
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("quota not released: %s %v", st, pools)
	}
}

// A watcher that ended before the hand-off (a worker that waited past its
// completion deadline) is started by the announcement, and a node restart
// re-arms a watcher that died during the grace: both keep the fallback that
// ends the job and frees its lease (review finding).
func TestAnnounceQuotaStopStartsAWatcherAndRearmFindsAnnouncedJobs(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	started := []string{}
	old := announceStartWatch
	announceStartWatch = func(_ *App, job string) error { started = append(started, job); return nil }
	t.Cleanup(func() { announceStartWatch = old })
	if e := AnnounceQuotaStop(context.Background(), a.Jobs, "j", "w:p1", mustAttempt(t, a), quotastop.Stop{Pool: "claude"}); e != nil {
		t.Fatal(e)
	}
	if len(started) != 1 || started[0] != "j" {
		t.Fatalf("watchers started: %v", started)
	}
	j, e := a.job("j")
	if e != nil || !j.Terminal {
		t.Fatalf("setup: the announced job should read as ended: %+v %v", j, e)
	}
	var rearmed []string
	if e := a.RearmWatchers(func(id string) error { rearmed = append(rearmed, id); return nil }, func(string) {}); e != nil {
		t.Fatal(e)
	}
	if len(rearmed) != 1 || rearmed[0] != "j" {
		t.Fatalf("re-armed %v, want the announced job", rearmed)
	}
}

// A reviewer that completed while the watcher read its screen has a terminal
// hk record of its own. A rejected lost write must not be taken for this stop's
// record: nothing is announced and the lease is left to the completion's path.
func TestWatcherReviewerQuotaStopYieldsToAnotherTerminalRecord(t *testing.T) {
	for name, tc := range map[string]struct{ state, reason string }{
		"completed":               {"completed", ""},
		"escalated":               {"escalated", ""},
		"lost for another reason": {"lost", "quota_stop pool=claude replaced_by=j-q1"},
	} {
		t.Run(name, func(t *testing.T) {
			a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
			h.mu.Lock()
			h.records["j"]["state"] = tc.state
			if tc.reason != "" {
				h.records["j"]["end_reason"] = tc.reason
			}
			h.putStatuses = []int{http.StatusConflict, http.StatusConflict, http.StatusConflict, http.StatusConflict}
			h.mu.Unlock()
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if p := lostPayload(t, a); p != nil {
				t.Fatalf("job.lost announced over a %s record: %v", tc.state, p)
			}
			if st, _ := ledgerState(t, a, "j"); st != "leased" {
				t.Fatalf("quota %s, want left as it was", st)
			}
			if got := str(h.record(t, "j"), "end_reason"); got != tc.reason {
				t.Fatalf("end_reason rewritten: %q", got)
			}
		})
	}
}

// A watcher that died between the hk write and the local finish finds the
// recorded intent on restart and finishes the job from the hk record alone,
// whatever the screen shows now and whatever a fresh parse would say.
func TestWatcherQuotaIntentFinishesAfterRestart(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", "The reviewer answered and the pane is quiet.\n")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	const reason = "quota_stop pool=devin-free reset_at=2026-01-01T00:05:00Z"
	if e := a.event(context.Background(), "j", quotaIntentKind, map[string]any{"pane_id": "w:p1", "pool": "devin-free", "reset_at": "2026-01-01T00:05:00Z", "end_reason": reason}); e != nil {
		t.Fatal(e)
	}
	h.mu.Lock()
	h.records["j"]["state"] = "lost"
	h.records["j"]["end_reason"] = reason
	h.mu.Unlock()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" || str(p, "reset_at") != "2026-01-01T00:05:00Z" {
		t.Fatalf("job.lost payload=%v, want the frozen stop", p)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s after restart, want released", st)
	}
}

// With hk live, a screen that no longer shows a stop cancels the intent: the
// reviewer resumed, so nothing is written, announced or released.
func TestWatcherQuotaIntentCancelledWhenThePaneMovedOn(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	reads := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "agent" && c.Args[1] == "read" {
			if reads++; reads > 1 {
				return Result{Out: "I resumed and finished the review.\n"}
			}
		}
		return do(c)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if str(h.record(t, "j"), "state") == "lost" || lostPayload(t, a) != nil {
		t.Fatalf("a pane that moved on was ended: %v", h.record(t, "j"))
	}
	if st, _ := ledgerState(t, a, "j"); st != "leased" {
		t.Fatalf("quota %s, want leased", st)
	}
}

// hk unreadable, the pane gone from two agent lists: the generic missing-pane
// path must not end the job (a bare lost write, a job.lost without quota_stop,
// the intent closed by that event). The intent settles against hk first.
func TestWatcherQuotaIntentBlocksTheMissingPaneFallback(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", devinLongStopScreen(t))
	legacyIntent(t, a, "devin-free", 2*time.Hour)
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	h.mu.Lock()
	h.down = true
	h.mu.Unlock()
	lists := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			if lists == 30 {
				if p := lostPayload(t, a); p != nil {
					t.Errorf("job.lost announced with hk unreadable and the pane missing: %v", p)
				}
				if st, _ := ledgerState(t, a, "j"); st != "leased" {
					t.Errorf("quota %s while the intent was unresolved, want leased", st)
				}
				h.mu.Lock()
				h.down = false
				h.mu.Unlock()
			}
			if lists >= 3 {
				return Result{Out: `{"result":{"agents":[]}}`}
			}
		}
		return do(c)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if lists < 30 {
		t.Fatalf("the watcher ended after %d polls, before hk recovered", lists)
	}
	if str(h.record(t, "j"), "state") != "lost" {
		t.Fatalf("hk record %v, want lost once hk is reachable", h.record(t, "j"))
	}
}

// The reviewer resumed and wrote a stable report plus READY while the intent
// could not be settled (hk unreadable): the report must not complete the job
// locally; the intent is settled against hk first.
func TestWatcherQuotaIntentBlocksReportCompletion(t *testing.T) {
	a, f, h, report := quotaStopFixture(t, "reviewer", "idle", devinLongStopScreen(t))
	legacyIntent(t, a, "devin-free", 2*time.Hour)
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	h.mu.Lock()
	h.down = true
	h.mu.Unlock()
	lists := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			switch lists {
			case 3:
				writeReport(t, report, "# review\n")
				putLaneEvent(t, a, "w:p1", "READY", a.Now().Add(time.Minute))
			case 20:
				for _, kind := range eventKindsOf(t, a, "j") {
					if kind == "job.completed" {
						t.Errorf("the report completed the job while the quota-stop intent was unsettled")
					}
				}
				h.mu.Lock()
				h.down = false
				h.mu.Unlock()
			}
		}
		return do(c)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if lists < 20 {
		t.Fatalf("the watcher ended after %d polls, before hk recovered", lists)
	}
}

// The finish releases the lease before it writes the closing job.lost, so a
// crash between the two leaves the intent open and the job is finished again on
// restart instead of staying terminal with a lease nobody retries. Here the
// lease is already free and only the closing event is missing.
func TestWatcherQuotaIntentFinishesWhenTheLeaseWasAlreadyReleased(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", "The pane is quiet.\n")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	const reason = "quota_stop pool=devin-free reset_at=2026-01-01T00:05:00Z"
	if e := a.event(context.Background(), "j", quotaIntentKind, map[string]any{"pane_id": "w:p1", "pool": "devin-free", "reset_at": "2026-01-01T00:05:00Z", "end_reason": reason}); e != nil {
		t.Fatal(e)
	}
	h.mu.Lock()
	h.records["j"]["state"] = "lost"
	h.records["j"]["end_reason"] = reason
	h.mu.Unlock()
	if e := a.releaseQuota(context.Background(), Job{ID: "j"}); e != nil {
		t.Fatal(e)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	p := lostPayload(t, a)
	if p == nil || str(p, "reason") != "quota_stop" || !boolean(p, "final", false) {
		t.Fatalf("job.lost payload=%v, want the final stop closing the intent", p)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
}

// hk 5690: the watcher no longer screen-detects a reviewer's limit stop. The
// quota-stop manager decides it (hand over, or wait and resume once) and announces
// a hand-over through the same path as an implementer's, so a reviewer idle on a
// limit screen with no report is left alone by its watcher: no close intent, no
// lost record, no announcement of its own.
func TestWatcherLeavesAReviewerOnALimitScreenToTheManager(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", devinLongStopScreen(t))
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if str(h.record(t, "j"), "state") == "lost" || lostPayload(t, a) != nil {
		t.Fatalf("the watcher ended or announced a reviewer on its own: %v", h.record(t, "j"))
	}
	for _, kind := range eventKindsOf(t, a, "j") {
		if kind == quotaIntentKind {
			t.Fatal("the watcher recorded a close intent for a reviewer")
		}
	}
	if st, _ := ledgerState(t, a, "j"); st != "leased" {
		t.Fatalf("quota %s, want left leased", st)
	}
}

// hk 5690 review: an adopted announcement keeps the pane state the stop was
// observed in, so a watcher started on it (a restart, or a manager's
// announcement) sees a pane idle again at another sequence as moved on and takes
// the existing resume path, instead of writing it lost at the grace's end.
func TestAdoptedAnnouncementKeepsThePaneStateTheHubFencesWith(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", "The reviewer is back at work.\n")
	a.QuotaHandOffGrace = QuotaStopHandOffGrace
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	// Announced at herdr sequence 3; the pane is idle at 7 (the fixture's).
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": "devin-free", "state_change_seq": 3}); e != nil {
		t.Fatal(e)
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	if stop, ok := announcedQuotaStop(j); !ok || stop.PaneSeq != 3 {
		t.Fatalf("announced stop %+v ok=%v, want PaneSeq 3 restored from the ledger", stop, ok)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if str(h.record(t, "j"), "state") == "lost" {
		t.Fatalf("a pane that moved on (seq 7, announced at 3) was written lost: %v", h.record(t, "j"))
	}
	revived := false
	for _, kind := range eventKindsOf(t, a, "j") {
		if kind == "job.reprompted" {
			revived = true
		}
	}
	if !revived {
		t.Fatal("the changed sequence did not take the existing resume path")
	}
}

// hk 5690 review: a close intent an older watcher left is conditional while hk is
// live: the pane may have moved on since. The watcher no longer detects a
// reviewer's stop, but reconciling an existing intent still reads the screen. A
// reviewer still on the limit screen has its intent finished (the final hk lost
// write); one that moved on has it cancelled and the job left alone.
func TestLegacyCloseIntentIsVerifiedOnTheScreenWhileHkIsLive(t *testing.T) {
	for name, tc := range map[string]struct {
		screen    string
		wantState string
	}{
		"still on the limit screen": {devinLongStopScreen(t), "lost"},
		"moved on":                  {"I resumed and finished the review.\n", "spawned"},
	} {
		t.Run(name, func(t *testing.T) {
			a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", tc.screen)
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
			legacyIntent(t, a, "devin-free", 2*time.Hour)
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			rec := h.record(t, "j")
			if str(rec, "state") != tc.wantState {
				t.Fatalf("hk record %v, want state %s", rec, tc.wantState)
			}
			if tc.wantState == "lost" && !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free reset_at=") {
				t.Fatalf("hk record %v, want the intent's frozen end_reason", rec)
			}
		})
	}
}
