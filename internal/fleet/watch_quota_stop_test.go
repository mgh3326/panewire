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
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "worker"})
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

func devinStopScreen(t *testing.T) string {
	t.Helper()
	b, e := os.ReadFile("../../testdata/quota-stop/devin-something-went-wrong.txt")
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
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

// An idle reviewer pane on devin's rate-limit screen with no report ends the
// job as lost with a reason, instead of idling to the watch deadline (hk
// 3302). A worker or tester is left to the quota-stop manager (hk 3334): see
// TestWatcherQuotaStopLeavesWorkerAndTesterToTheManager.
func TestWatcherQuotaStopEndsIdleJobAsLost(t *testing.T) {
	for _, role := range []string{"reviewer"} {
		t.Run(role, func(t *testing.T) {
			a, f, h, _ := quotaStopFixture(t, role, "idle", devinStopScreen(t))
			reset := a.Now().Add(5 * time.Minute).UTC().Format(time.RFC3339)
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			rec := h.record(t, "j")
			wantEnd := "quota_stop pool=devin-free reset_at=" + reset
			if str(rec, "state") != "lost" || str(rec, "end_reason") != wantEnd || len(wantEnd) > 128 {
				t.Fatalf("hk record state=%v end_reason=%q, want lost / %q", rec["state"], rec["end_reason"], wantEnd)
			}
			if _, ok := rec["result"]; ok {
				t.Fatalf("hk record carries a result: %v", rec["result"])
			}
			p := lostPayload(t, a)
			if p == nil || str(p, "reason") != "quota_stop" || str(p, "pool") != "devin-free" || str(p, "reset_at") != reset || str(p, "pane_id") != "w:p1" {
				t.Fatalf("job.lost payload=%v, want reason quota_stop pool devin-free reset_at %s", p, reset)
			}
			if _, ok := p["owner_lane"]; ok {
				t.Fatalf("quota stop unexpectedly routes a lane relay: %v", p)
			}
			if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
				t.Fatalf("quota not released: %s %v", st, pools)
			}
			for _, c := range f.Calls {
				if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "agent" && c.Args[1] != "list" && c.Args[1] != "read" {
					t.Fatalf("watcher wrote to the pane: %v", c.Args)
				}
			}
		})
	}
}

func TestWatcherQuotaStopLeavesOtherPanesLive(t *testing.T) {
	for name, tc := range map[string]struct{ role, status, screen string }{
		"counsel role":               {"counsel", "idle", ""},
		"working pane":               {"worker", "working", ""},
		"idle, not a quota":          {"worker", "idle", "I finished the task.\n❭ Ask Devin to build features, fix bugs, or work on your code\n"},
		"stop, then output":          {"worker", "idle", "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes\nresumed and wrote the report\n"},
		"working pane, claude limit": {"worker", "working", "You've hit your session limit · resets unknown\n"},
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

// With no parsed reset, reset_at is left out of both the hk end_reason and job.lost.
func TestWatcherQuotaStopUnknownResetOmitsResetAt(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", "▲ Something went wrong — Reached free model rate limit\n❭ Ask Devin to build features, fix bugs, or work on your code\n")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=devin-free" {
		t.Fatalf("end_reason=%q", got)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" || str(p, "pool") != "devin-free" || p["reset_at"] != nil {
		t.Fatalf("job.lost payload=%v", p)
	}
}

// hk 3334: the watcher no longer ends a worker or tester on a quota stop; the
// quota-stop manager waits for the reset or hands it over. Dropping this split
// (putting worker or tester back into quotaStopRoles) fails here.
func TestWatcherQuotaStopLeavesWorkerAndTesterToTheManager(t *testing.T) {
	for _, role := range []string{"worker", "tester"} {
		t.Run(role, func(t *testing.T) {
			a, _, h, _ := quotaStopFixture(t, role, "idle", devinStopScreen(t))
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if got := str(h.record(t, "j"), "state"); got == "lost" {
				t.Fatal("the watcher ended a worker or tester on a quota stop")
			}
			if slices.Contains(eventKindsOf(t, a, "j"), "job.lost") {
				t.Fatal("job.lost recorded for a worker or tester")
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
	a, _, h, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
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
	a, f, h, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
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
	a, f, h, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
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
	a, f, h, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
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
	a, _, h, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
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
	a, _, _, _ := quotaStopFixture(t, "worker", "idle", "I stopped here.\n")
	started := []string{}
	old := announceStartWatch
	announceStartWatch = func(_ *App, job string) error { started = append(started, job); return nil }
	t.Cleanup(func() { announceStartWatch = old })
	if e := AnnounceQuotaStop(context.Background(), a.Jobs, "j", "w:p1", quotastop.Stop{Pool: "claude"}); e != nil {
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

// hk 4507: a reviewer whose pane shows the devin free limit above an injected
// directive and an idle prompt ends at once with job.lost reason quota_stop.
// It does not wait out QuotaStopHandOffGrace for a hub replacement: the hk
// record is ended and the lease released straight away, so mergewake respawns
// the review on another model.
func TestWatcherReviewerOnQuotaScreenEndsAtOnceWithoutGrace(t *testing.T) {
	screen := "❭ [pw-x] Read the brief and review the PR\n\n" +
		"▲ Something went wrong — Reached free model rate limit … Your limit will reset in 13 hours 9 minutes (at 22:01 UTC). Send a message to retry\n\n" +
		"❭ [relay] owner: please report status\n\n" +
		"──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n"
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", screen)
	a.QuotaHandOffGrace = QuotaStopHandOffGrace
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if waited := a.Now().Sub(started); waited >= time.Minute {
		t.Fatalf("the reviewer was ended after %v, want at once (no hand-off grace)", waited)
	}
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free reset_at=") || strings.Contains(str(rec, "end_reason"), "replaced_by") {
		t.Fatalf("hk record %v", rec)
	}
	p := lostPayload(t, a)
	if p == nil || str(p, "reason") != "quota_stop" || str(p, "pool") != "devin-free" {
		t.Fatalf("job.lost payload=%v, want reason quota_stop pool devin-free", p)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("quota not released: %s %v", st, pools)
	}
}

// hk 4507 review: job.lost and the lease release wait for the lost hk record.
// While the write fails the reviewer stays live and leased with no job.lost (a
// hub would take the still-spawned record for a live job to replace); when the
// write lands the job ends once, with the record, the event and the release.
func TestWatcherReviewerQuotaStopWaitsForTheLostRecord(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	statuses := make([]int, 0, 40)
	for range 40 {
		statuses = append(statuses, 503)
	}
	h.mu.Lock()
	h.putStatuses = statuses
	h.mu.Unlock()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
		t.Fatalf("hk record %v, want lost with the quota_stop end_reason", rec)
	}
	lost := 0
	for _, kind := range eventKindsOf(t, a, "j") {
		if kind == "job.lost" {
			lost++
		}
	}
	if lost != 1 {
		t.Fatalf("job.lost recorded %d times, want exactly 1 after the record landed", lost)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("quota not released: %s %v", st, pools)
	}
}

// The retry for a lost record that cannot be written outlives the ordinary
// completion deadline: hk down past it must not leave the reviewer spawned and
// leased for good. When hk recovers the job ends once, with record, event and
// release. Until then it is neither announced nor released.
func TestWatcherReviewerQuotaStopRetriesPastTheCompletionDeadline(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
	h.mu.Lock()
	h.down = true
	h.mu.Unlock()
	lists := 0
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			lists++
			if lists == 40 { // well past the 60 s deadline at 5 s a poll
				if p := lostPayload(t, a); p != nil {
					t.Errorf("job.lost announced with the hk record unwritten: %v", p)
				}
				if st, _ := ledgerState(t, a, "j"); st != "leased" {
					t.Errorf("quota %s while the record could not be written, want leased", st)
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
	if lists < 40 {
		t.Fatalf("the watcher gave up after %d polls, before hk recovered", lists)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
		t.Fatalf("hk record %v, want lost with the quota_stop end_reason", rec)
	}
	if p := lostPayload(t, a); p == nil || str(p, "reason") != "quota_stop" {
		t.Fatalf("job.lost payload=%v", p)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s after the record landed, want released", st)
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

// A failed screen read or a failed agent list while a close intent is open
// proves nothing: the intent stays, and the job ends once hk and herdr recover.
// A read error used to be remembered as the episode's observation and ended the
// retry; an agent-list error used to return from the watcher.
func TestWatcherQuotaIntentSurvivesHerdrErrors(t *testing.T) {
	for name, fail := range map[string]func(c Command) (Result, bool){
		"screen read": func(c Command) (Result, bool) {
			return Result{Code: 1, Err: "boom"}, len(c.Args) > 1 && c.Args[0] == "agent" && c.Args[1] == "read"
		},
		"agent list": func(c Command) (Result, bool) {
			return Result{Code: 1, Err: "boom"}, strings.Join(c.Args, " ") == "agent list"
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
			t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "60")
			h.mu.Lock()
			h.down = true
			h.mu.Unlock()
			lists := 0
			do := f.Do
			f.Do = func(c Command) Result {
				if c.Name != "herdr" {
					return do(c)
				}
				if strings.Join(c.Args, " ") == "agent list" {
					lists++
				}
				// Once the intent is recorded, herdr fails for a while and hk
				// stays down; both recover together at poll 40.
				if lists >= 3 && lists < 40 {
					if r, hit := fail(c); hit {
						return r
					}
				}
				if lists == 40 {
					h.mu.Lock()
					h.down = false
					h.mu.Unlock()
				}
				return do(c)
			}
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if lists < 40 {
				t.Fatalf("the watcher gave up after %d polls, before herdr and hk recovered", lists)
			}
			if rec := h.record(t, "j"); str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
				t.Fatalf("hk record %v, want lost with the quota_stop end_reason", rec)
			}
			if st, _ := ledgerState(t, a, "j"); st != "released" {
				t.Fatalf("quota %s, want released", st)
			}
		})
	}
}

// hk may apply the lost write and drop the answer. The next poll must reconcile
// against the first attempt's end_reason, not a reason recomputed from a fresh
// parse of "reset in 5 minutes" (which moves with the clock): our own record
// is the intended one, so the job is announced and released.
func TestWatcherQuotaIntentFreezesTheEndReason(t *testing.T) {
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", devinStopScreen(t))
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	h.mu.Lock()
	h.dropAfterCommit = true
	h.mu.Unlock()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || !strings.HasPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free") {
		t.Fatalf("hk record %v", rec)
	}
	p := lostPayload(t, a)
	if p == nil || str(p, "reason") != "quota_stop" {
		t.Fatalf("job.lost payload=%v, want one announcement after our own record landed", p)
	}
	if got, want := str(p, "reset_at"), strings.TrimPrefix(str(rec, "end_reason"), "quota_stop pool=devin-free reset_at="); got != want {
		t.Fatalf("job.lost reset_at %q differs from the record's %q", got, want)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
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
