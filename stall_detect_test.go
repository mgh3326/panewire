package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The acceptance tests below exercise the decision document's ten checks. The
// real error strings are reproduced verbatim; nothing in these tests may ever
// write to a pane, a batch value, or a quota number.

const (
	stallFixtureLimit = `Error: [provider.auth_error] 403 {"error":{"type":"permission_error","message":"You've reached your 5-hour usage limit. It will reset at 12:00."}}`
	stallFixtureAuth  = `■ Your access token could not be refreshed because you have since logged out or signed in to another account. Please sign in again.`
	stallFixtureQueue = `Press Enter to send queued messages now`
)

type stallFixture struct {
	t          *testing.T
	store      *Store
	inbox      string
	mgr        *stallDetectManager
	now        time.Time
	agents     []paneIdentity
	reads      map[string]readEvidence
	subscribed map[string]bool
	sent       []hubScannedRelayEvent
	resubs     int
	procs      []stallProc
	procCWD    map[int64]string
	trees      map[string]string
	persisted  map[string]bool
}

func newStallFixture(t *testing.T, notify bool) *stallFixture {
	t.Helper()
	root := t.TempDir()
	inbox := filepath.Join(root, "inbox")
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(filepath.Join(root, "panewire.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	fx := &stallFixture{
		t: t, store: store, inbox: inbox,
		now:        time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC),
		reads:      map[string]readEvidence{},
		subscribed: map[string]bool{},
		procCWD:    map[int64]string{},
		trees:      map[string]string{},
		persisted:  map[string]bool{},
	}
	mgr, err := newStallDetectManager(store, inbox, StallDetectConfig{
		Enabled: true, Notify: notify, PollInterval: time.Minute,
		StartupGrace: 90 * time.Second, NotifyGrace: 15 * time.Minute, NotifyRetry: time.Minute,
	}, stallDeps{
		listAgents:  func(context.Context) ([]paneIdentity, error) { return fx.agents, nil },
		readPane:    func(_ context.Context, pane string) (readEvidence, error) { return fx.reads[pane], nil },
		subscribed:  func() (map[string]bool, time.Time) { return fx.subscribed, fx.now },
		resubscribe: func() { fx.resubs++ },
		worktree:    func(_ context.Context, dir string) string { return fx.trees[dir] },
		procs:       func(context.Context) ([]stallProc, error) { return fx.procs, nil },
		procCWD:     func(_ context.Context, pid int64) (string, error) { return fx.procCWD[pid], nil },
		enqueue:     func(event hubScannedRelayEvent) bool { fx.sent = append(fx.sent, event); return true },
		lanePersisted: func(_ context.Context, lane, eventID string) (bool, error) {
			return fx.persisted[lane+"\x00"+eventID], nil
		},
		now: func() time.Time { return fx.now },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	fx.mgr = mgr
	return fx
}

func (fx *stallFixture) advance(d time.Duration) { fx.now = fx.now.Add(d) }

func (fx *stallFixture) scan() {
	fx.t.Helper()
	fx.mgr.scan(context.Background())
}

// writeJobEvent appends one seq-named record to a job's event journal, the
// same durable shape wrk produces.
func writeJobEvent(t *testing.T, inbox, jobID string, seq int, kind string, payload map[string]any, createdAt time.Time) {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"created_at": createdAt.Format(time.RFC3339), "job_id": jobID,
		"kind": kind, "payload": payload, "seq": seq,
	})
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("%05d-%s.json", seq, kind)
	if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
		t.Fatal(err)
	}
}

// claimJob writes the standard claim/quota/spawn journal trio for a devin job.
func claimJob(t *testing.T, fx *stallFixture, jobID, pane string, payloadExtra map[string]any, at time.Time) {
	t.Helper()
	claim := map[string]any{"agent_label": "w", "owner_lane": "owner-1", "parent_lane": "director-1", "role": "worker", "t_level": "T2"}
	for k, v := range payloadExtra {
		claim[k] = v
	}
	writeJobEvent(t, fx.inbox, jobID, 1, "job.claim", claim, at)
	writeJobEvent(t, fx.inbox, jobID, 2, "quota_pool.record", map[string]any{"pool": "devin", "profile": "devin-swe2"}, at)
	writeJobEvent(t, fx.inbox, jobID, 3, "job.spawned", map[string]any{"pane_id": pane, "workspace": "w16", "profile": "devin-swe2"}, at)
	if pane != "" {
		fx.agents = append(fx.agents, paneIdentity{PaneID: pane, WorkspaceID: "w16", CWD: "/work/" + jobID, Harness: "devin", Status: "idle"})
		fx.subscribed[pane] = true
	}
}

func stallIncidentsFor(t *testing.T, fx *stallFixture, jobID string) []stallIncidentRow {
	t.Helper()
	rows, err := fx.store.stallIncidentsAll(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	var out []stallIncidentRow
	for _, row := range rows {
		if row.JobID == jobID {
			out = append(out, row)
		}
	}
	return out
}

func stallCauses(rows []stallIncidentRow) []string {
	var out []string
	for _, row := range rows {
		out = append(out, row.Cause)
	}
	return out
}

// AC1 — the three real-world strings each produce exactly one incident with
// the exact cause name and exactly one would-be notification.
func TestStallRealErrorFixtures(t *testing.T) {
	cases := []struct {
		name, text, cause string
	}{
		{"limit_refused", "working...\n" + stallFixtureLimit + "\n", stallCauseLimitRefused},
		{"auth_refused", "boot\n" + stallFixtureAuth + "\n", stallCauseAuthRefused},
		{"input_unsubmitted", "queued\n" + stallFixtureQueue + "\n", stallCauseInputUnsubmitted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newStallFixture(t, false)
			claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
			fx.reads["w1:p1"] = readEvidence{Text: "normal work output\n", Revision: 1}
			fx.scan() // baseline
			fx.reads["w1:p1"] = readEvidence{Text: tc.text, Revision: 2}
			// The queue banner needs a confirming second read; the other two
			// record on first sighting. A second identical scan adds nothing.
			for i := 0; i < 2; i++ {
				fx.advance(time.Minute)
				fx.scan()
			}
			rows := stallIncidentsFor(t, fx, "job-a")
			if len(rows) != 1 || rows[0].Cause != tc.cause {
				t.Fatalf("want one %s incident, got %+v", tc.cause, stallCauses(rows))
			}
			notifications, err := fx.store.stallNotifications(context.Background(), rows[0].incidentKey())
			if err != nil {
				t.Fatal(err)
			}
			if len(notifications) != 1 || notifications[0].SuppressedReason != stallNotifyShadow {
				t.Fatalf("want one shadow notification, got %+v", notifications)
			}
			if len(fx.sent) != 0 {
				t.Fatalf("shadow mode emitted %d lane events", len(fx.sent))
			}
		})
	}
}

// AC1 helper — repeated sightings of the same line are new occurrences, and a
// re-read of identical text is not.
func TestStallRepeatedErrorIsNewOccurrence(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureLimit + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	// Same read again: fingerprint count unchanged, no second incident.
	fx.advance(time.Minute)
	fx.scan()
	// The same line appears a second time in the buffer: a new occurrence.
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureLimit + "\nretry\n" + stallFixtureLimit + "\n", Revision: 3}
	fx.advance(time.Minute)
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 2 || rows[0].Occurrence != 1 || rows[1].Occurrence != 2 {
		t.Fatalf("want two occurrences, got %+v", stallCauses(rows))
	}
}

// A2 — the queued-input banner alone is ambiguous: a submitted prompt leaves
// the same banner behind. The detector does not consult the status string; a
// banner must hold across a second read to be recorded as unsubmitted input,
// and two advancing clean reads recover it.
func TestStallUnsubmittedVersusSubmitted(t *testing.T) {
	if matches := classifyScreen(stallFixtureQueue + "\n"); len(matches) != 1 || matches[0].cause != stallCauseInputUnsubmitted {
		t.Fatalf("queue banner did not classify: %+v", matches)
	}
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "typing a prompt\n", Revision: 1}
	fx.scan()
	// First sighting of the banner: could be queued-and-sent. Not yet an
	// incident.
	fx.reads["w1:p1"] = readEvidence{Text: "typing a prompt\n" + stallFixtureQueue + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("single banner sighting recorded: %+v", stallCauses(rows))
	}
	// The banner persists into the next read: unsubmitted input.
	fx.reads["w1:p1"] = readEvidence{Text: "typing a prompt\n" + stallFixtureQueue + "\n", Revision: 3}
	fx.advance(time.Minute)
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseInputUnsubmitted {
		t.Fatalf("persistent banner not recorded: %+v", stallCauses(rows))
	}
	// The operator submits it; output advances cleanly twice. The row
	// recovers and stays in history.
	for i := 0; i < 2; i++ {
		fx.reads["w1:p1"] = readEvidence{Text: fmt.Sprintf("answer part %d\n", i), Revision: int64(4 + i)}
		fx.advance(time.Minute)
		fx.scan()
	}
	rows = stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].RecoveredAt.IsZero() {
		t.Fatalf("submitted input did not recover the row: %+v", rows)
	}
}

// AC2 — a long, ordinary, file-quiet run produces zero incidents.
func TestStallCleanRunNoIncidents(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", map[string]any{"deadline_at": fx.now.Add(4 * time.Hour).Format(time.RFC3339)}, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "running validation suite\n", Revision: 1}
	for i := 0; i < 31; i++ {
		fx.agents[0].Status = "working"
		fx.scan()
		fx.advance(time.Minute)
		fx.reads["w1:p1"] = readEvidence{Text: "running validation suite\n", Revision: int64(1 + i)}
	}
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("clean run produced incidents: %+v", stallCauses(rows))
	}
}

// AC3 — a crossed deadline records [overdue] 확인 필요 with the evidence
// bundle and no "stopped"-style assertion anywhere in the record.
func TestStallOverdueEvidenceBundle(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", map[string]any{"deadline_at": fx.now.Add(-time.Minute).Format(time.RFC3339)}, fx.now.Add(-2*time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "halfway through the run\n", Revision: 4}
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseOverdue {
		t.Fatalf("want one overdue incident, got %+v", stallCauses(rows))
	}
	var evidence map[string]any
	if err := json.Unmarshal(rows[0].Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"deadline_at", "deadline_source", "action_owner", "cause_candidates", "last_file_activity", "process_cpu", "screen_excerpt", "report_local", "report_remote", "observed_freshness_ms"} {
		if _, exists := evidence[field]; !exists {
			t.Fatalf("evidence missing %s: %v", field, evidence)
		}
	}
	if evidence["summary"] != "[overdue] 확인 필요" {
		t.Fatalf("summary=%v", evidence["summary"])
	}
	// The bundle is a closed set of measured fields — a verdict-shaped key
	// like worker_state:"hung" must not be smuggled in (C1b).
	overdueAllowed := map[string]bool{
		"deadline_at": true, "deadline_source": true, "action_owner": true, "summary": true,
		"cause_candidates": true, "last_file_activity": true, "observed_freshness_ms": true,
		"process_cpu": true, "report_local": true, "report_remote": true,
		"screen_excerpt": true, "observed_at": true,
	}
	for key := range evidence {
		if !overdueAllowed[key] {
			t.Fatalf("overdue evidence carries non-allowlisted key %q: %v", key, evidence)
		}
	}
	blob := string(rows[0].Evidence)
	for _, banned := range []string{"stalled", "stopped", "멈춤", "정지"} {
		if strings.Contains(blob, banned) {
			t.Fatalf("evidence asserts a stop via %q: %s", banned, blob)
		}
	}
}

// SHOULD-4 — once the old banner scrolled out of the buffer completely its
// watermark must not suppress a genuinely new identical error: the second
// sighting is occurrence two, not a silent miss.
func TestStallScrolloutNewIdenticalError(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "working\n", Revision: 1}
	fx.scan()
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureAuth + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 1 || rows[0].Cause != stallCauseAuthRefused {
		t.Fatalf("first sighting missing: %+v", stallCauses(rows))
	}
	// The banner scrolls out of the buffer entirely — the fingerprint is
	// absent from this read's matches, so the old watermark must drop.
	fx.reads["w1:p1"] = readEvidence{Text: "fresh output\n", Revision: 3}
	fx.advance(time.Minute)
	fx.scan()
	// A genuinely new identical failure is a new occurrence, not a miss.
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureAuth + "\n", Revision: 4}
	fx.advance(time.Minute)
	fx.scan()
	auth := 0
	for _, row := range stallIncidentsFor(t, fx, "job-a") {
		if row.Cause == stallCauseAuthRefused {
			auth++
		}
	}
	if auth != 2 {
		t.Fatalf("post-scrollout identical error swallowed: want 2 auth rows, got %+v", stallCauses(stallIncidentsFor(t, fx, "job-a")))
	}
}

// AC4 — a pane outside the proven subscription set records [unobserved] and
// requests a resubscribe; coverage returning marks it recovered.
func TestStallUnobservedCoverage(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	delete(fx.subscribed, "w1:p1") // injected subscription failure
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseUnobserved {
		t.Fatalf("want one unobserved incident, got %+v", stallCauses(rows))
	}
	if fx.resubs == 0 {
		t.Fatal("unobserved pane did not request a resubscribe")
	}
	// A still-uncovered pane does not stack rows.
	fx.advance(31 * time.Second)
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 1 {
		t.Fatalf("unobserved gap duplicated incidents: %+v", stallCauses(rows))
	}
	fx.subscribed["w1:p1"] = true
	fx.advance(time.Minute)
	fx.scan()
	rows = stallIncidentsFor(t, fx, "job-a")
	if rows[0].RecoveredAt.IsZero() {
		t.Fatal("restored coverage did not recover the unobserved row")
	}
}

// AC6 — a daemon restart restores open incidents and does not re-notify.
func TestStallRestartRestoresWithoutDuplicates(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureLimit + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	before := stallIncidentsFor(t, fx, "job-a")
	if len(before) != 1 {
		t.Fatalf("setup incident missing: %+v", stallCauses(before))
	}
	// Restart: same store path, new manager over the same inbox.
	reopened, err := OpenStore(fx.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	fx.store = reopened
	mgr, err := newStallDetectManager(reopened, fx.inbox, fx.mgr.cfg, fx.mgr.deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	fx.mgr = mgr
	fx.advance(time.Minute)
	fx.scan()
	after := stallIncidentsFor(t, fx, "job-a")
	if len(after) != len(before) {
		t.Fatalf("restart duplicated incidents: %d -> %d", len(before), len(after))
	}
	notifications, err := fx.store.stallNotifications(context.Background(), before[0].incidentKey())
	if err != nil || len(notifications) != 1 {
		t.Fatalf("restart duplicated notifications: %+v err=%v", notifications, err)
	}
}

// AC7 — old scrollback at first observation is baseline, never a confirmed
// incident. Two suspicion shapes are still recorded rather than dropped: a
// job inside its startup grace, and — outside grace — a match anchored at
// the buffer tail where current output lives. A banner buried under newer
// lines records nothing.
func TestStallScrollbackAndStartupSuspect(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-old", "w1:p1", nil, fx.now.Add(-3*time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureAuth + "\nwork continued\nmore output\nlatest state\n", Revision: 9}
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-old"); len(rows) != 0 {
		t.Fatalf("buried scrollback produced an incident: %+v", stallCauses(rows))
	}

	// Same old job, but the banner is the last thing on screen: tail-anchored,
	// so the baseline read names it a startup suspect rather than dropping it.
	fx1 := newStallFixture(t, false)
	claimJob(t, fx1, "job-tail", "w1:p9", nil, fx1.now.Add(-3*time.Hour))
	fx1.reads["w1:p9"] = readEvidence{Text: "some output\n" + stallFixtureAuth + "\n", Revision: 9}
	fx1.scan()
	rows := stallIncidentsFor(t, fx1, "job-tail")
	if len(rows) != 1 || rows[0].Cause != stallCauseStartupSuspect {
		t.Fatalf("tail-anchored baseline match was not recorded as suspect: %+v", stallCauses(rows))
	}

	fx2 := newStallFixture(t, false)
	claimJob(t, fx2, "job-new", "w2:p2", nil, fx2.now.Add(-30*time.Second))
	fx2.reads["w2:p2"] = readEvidence{Text: stallFixtureAuth + "\n", Revision: 1}
	fx2.scan()
	rows = stallIncidentsFor(t, fx2, "job-new")
	if len(rows) != 1 || rows[0].Cause != stallCauseStartupSuspect {
		t.Fatalf("boot-time auth failure was not recorded as startup suspect: %+v", stallCauses(rows))
	}

	// Startup grace is its own signal, independent of the tail anchor: a new
	// job whose boot error is already pushed off the tail by later output is
	// still a suspect. Removing the grace branch (the C4b mutant) drops this
	// case while the tail-anchored fixtures above keep passing — this is the
	// assertion that pins the grace down.
	fx3 := newStallFixture(t, false)
	claimJob(t, fx3, "job-new-mid", "w3:p3", nil, fx3.now.Add(-30*time.Second))
	fx3.reads["w3:p3"] = readEvidence{Text: stallFixtureAuth + "\nretrying boot\nload average ok\nwaiting on socket\nlatest line\n", Revision: 1}
	fx3.scan()
	rows = stallIncidentsFor(t, fx3, "job-new-mid")
	if len(rows) != 1 || rows[0].Cause != stallCauseStartupSuspect {
		t.Fatalf("in-grace mid-buffer boot failure was not recorded as startup suspect: %+v", stallCauses(rows))
	}
}

// writeRawJobEvent writes one journal record verbatim — used for fixtures
// captured byte-for-byte from the real herdr-inbox journal, so the reader is
// tested against shapes that actually exist rather than invented ones.
func writeRawJobEvent(t *testing.T, inbox, jobID, name, raw string) {
	t.Helper()
	dir := filepath.Join(inbox, "jobs", jobID, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
}

// B2 — the real journal's 543 terminal records carry the report reference in
// four shapes: top-level report_path (535, flat panewire emit, captured from
// 179-observability-impl-20260909-1900), payload.note's `report=` token as a
// local path (installer-126l-20260908), the same token as a handoffkeep
// document key (builder44-idlewake-20260909), and nothing at all with the
// report at the convention path jobs/<job>/report.md (tester-2073 and the
// 09-15 completions) or genuinely absent (rob1353). Each shape gets its own
// handling below — nothing is dropped silently.
func TestStallReportPathRealJournalShapes(t *testing.T) {
	body := "done\nkey: sk-ABCdef1234567890ghiJEL\n"

	// Declared references are recorded as inert strings and never opened —
	// the only filesystem observation is the convention file's Lstat.
	t.Run("top_level", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"kind":"job.completed","job_id":"job-a","owner_lane":"builder-49","label":"task179-observability-impl","pane_id":"w1:p1","host":"node-a","report_path":"/outside/report.md","report_last_line":"completed","epoch":1}`)
		if err := os.WriteFile(filepath.Join(fx.inbox, "jobs", "job-a", "report.md"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "observed" || row.MTime.IsZero() {
			t.Fatalf("convention file was not observed: %+v found=%v err=%v", row, found, err)
		}
		if row.RefKind != "path" || row.RefValue != "/outside/report.md" {
			t.Fatalf("declared reference not recorded as string: %+v", row)
		}
	})

	t.Run("payload_note", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		writeRawJobEvent(t, fx.inbox, "job-a", "00007-job.completed.json",
			`{"created_at": "2026-09-07T19:27:00+00:00", "job_id": "job-a", "kind": "job.completed", "payload": {"note": "JOIN installer-126l: RESULT=INSTALLED; report=/outside/report.md marker=report/role126-local-install/executed hk:doc brief/role126/local-install-preflight-fix"}, "seq": 7}`)
		if err := os.WriteFile(filepath.Join(fx.inbox, "jobs", "job-a", "report.md"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "observed" {
			t.Fatalf("convention file was not observed: %+v found=%v err=%v", row, found, err)
		}
		if row.RefKind != "path" || row.RefValue != "/outside/report.md" {
			t.Fatalf("note report= token not recorded as string: %+v", row)
		}
	})

	// The 6 real note-only completions split three ways: jobs/<job>/report.md
	// exists for 5 (observed with mtime), is genuinely absent for 1 (recorded
	// no_report, not dropped silently), and one `report=` token is a
	// handoffkeep document key, never a local path.
	t.Run("convention_report_md", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		// tester-2073's real shape: a completion whose note names no report,
		// with the report sitting at the fleet-convention path.
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"created_at": "2026-09-14T20:56:20+00:00", "job_id": "job-a", "kind": "job.completed", "payload": {"note": "VERDICT=FAIL · BLOCKER 1(F3 port-less DSN) + SHOULD 5 · 머지 보류 판정 소비 완료"}, "seq": 4}`)
		written := fx.now.Add(-time.Hour)
		reportPath := filepath.Join(fx.inbox, "jobs", "job-a", "report.md")
		if err := os.WriteFile(reportPath, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(reportPath, written, written); err != nil {
			t.Fatal(err)
		}
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "observed" || !row.MTime.Equal(written) {
			t.Fatalf("convention report existence/mtime not observed: %+v found=%v err=%v", row, found, err)
		}
	})

	t.Run("note_doc_key", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		// builder44-idlewake-20260909's real record, byte for byte: the
		// report= token is a handoffkeep document key, not a path.
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"created_at": "2026-09-09T04:00:46+00:00", "job_id": "job-a", "kind": "job.completed", "payload": {"note": "disposition=UNVERIFIED/HOLD; report=report/task172/hold-ack; head=ca94ac5a67a9986ab1115ed5f839148c9d7ed2ea; coordinator assignment complete; no join/merge/deploy"}, "seq": 4}`)
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found {
			t.Fatalf("doc-key report left no record: found=%v err=%v", found, err)
		}
		if row.RefKind != "doc_key" || row.RefValue != "report/task172/hold-ack" || row.State != "no_report" {
			t.Fatalf("doc key was mishandled: %+v", row)
		}
	})

	t.Run("no_report_in_note", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"created_at": "2026-09-14T20:56:20+00:00", "job_id": "job-a", "kind": "job.completed", "payload": {"note": "VERDICT=FAIL · BLOCKER 1(F3 port-less DSN) + SHOULD 5 · 머지 보류 판정 소비 완료"}, "seq": 4}`)
		fx.scan()
		// rob1353's real shape: nothing declared, convention file absent.
		// The drop is a recorded row, not silence.
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found {
			t.Fatalf("report-less completion left no record: found=%v err=%v", found, err)
		}
		if row.State != "no_report" || row.Note == "" {
			t.Fatalf("no-report row malformed: %+v", row)
		}
	})

	// A report.md landing after the terminal record is observed on the next
	// scan — the no_report row upgrades instead of fossilizing.
	t.Run("late_convention_report", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done"},"seq":4,"created_at":"2026-09-17T09:00:00Z"}`)
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "no_report" {
			t.Fatalf("absent report was not recorded: %+v", row)
		}
		if err := os.WriteFile(filepath.Join(fx.inbox, "jobs", "job-a", "report.md"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		fx.advance(time.Minute)
		fx.scan()
		row, found, err = fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "observed" || row.MTime.IsZero() {
			t.Fatalf("late convention report was not observed: %+v found=%v err=%v", row, found, err)
		}
	})

	// A symlink at the convention path is refused without being followed —
	// the row records exactly why nothing was observed.
	t.Run("convention_symlink_refused", func(t *testing.T) {
		fx := newStallFixture(t, false)
		claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
		writeRawJobEvent(t, fx.inbox, "job-a", "00004-job.completed.json",
			`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done"},"seq":4,"created_at":"2026-09-17T09:00:00Z"}`)
		target := filepath.Join(t.TempDir(), "outside.md")
		if err := os.WriteFile(target, []byte("canary\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(fx.inbox, "jobs", "job-a", "report.md")); err != nil {
			t.Fatal(err)
		}
		fx.scan()
		row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
		if err != nil || !found || row.State != "symlink_refused" || !row.MTime.IsZero() {
			t.Fatalf("symlinked report was not refused: %+v found=%v err=%v", row, found, err)
		}
	})
}

// BLOCKER-2 — job.escalate and job.joined are coordination records inside a
// live job, not endings: in the real journal 242 of 274 escalates and 65 of
// 88 joins are followed by further job events. A job that escalated and then
// overran its deadline must still record [overdue].
func TestStallEscalateDoesNotEndTheJob(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", map[string]any{"deadline_at": fx.now.Add(-time.Minute).Format(time.RFC3339)}, fx.now.Add(-2*time.Hour))
	writeJobEvent(t, fx.inbox, "job-a", 4, "job.escalate", map[string]any{"note": "asked flag for judgement"}, fx.now.Add(-30*time.Minute))
	writeJobEvent(t, fx.inbox, "job-a", 5, "job.joined", map[string]any{"reason": "builder joined PR"}, fx.now.Add(-20*time.Minute))
	fx.reads["w1:p1"] = readEvidence{Text: "still working\n", Revision: 3}
	fx.scan()
	jobs, err := fx.store.stallJobs(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	open := false
	for _, job := range jobs {
		if job.JobID == "job-a" && job.Attempt == 1 && !job.Terminal {
			open = true
		}
	}
	if !open {
		t.Fatal("an escalated/joined job was closed as terminal")
	}
	var overdue bool
	for _, row := range stallIncidentsFor(t, fx, "job-a") {
		if row.Cause == stallCauseOverdue {
			overdue = true
		}
	}
	if !overdue {
		t.Fatal("deadline lapse after escalation recorded nothing")
	}
	// And an on-screen confirmed error still records — the pane keeps its
	// coverage through the escalation.
	fx.reads["w1:p1"] = readEvidence{Text: "still working\n" + stallFixtureLimit + "\n", Revision: 4}
	fx.advance(time.Minute)
	fx.scan()
	var refused bool
	for _, row := range stallIncidentsFor(t, fx, "job-a") {
		if row.Cause == stallCauseLimitRefused {
			refused = true
		}
	}
	if !refused {
		t.Fatal("post-escalation screen error was not recorded")
	}
}

// The terminal set is a closed enumeration backed by the real journal: end
// records close the job, coordination and pool records never do.
func TestStallTerminalKindEnumeration(t *testing.T) {
	terminal := []string{"job.completed", "job.completion", "job.revoked", "job.lost", "job.reaped", "job.aborted", "worker.complete"}
	live := []string{"job.escalate", "job.joined", "job.reclaim", "quota_pool.release", "lane.event"}
	closedBy := func(kind string) bool {
		inbox := t.TempDir()
		writeJobEvent(t, inbox, "job-x", 1, "job.claim", map[string]any{"owner_lane": "owner-1"}, time.Now())
		writeJobEvent(t, inbox, "job-x", 2, kind, map[string]any{}, time.Now())
		jobs := scanStallJobs(inbox)
		return len(jobs) == 1 && jobs[0].Terminal
	}
	for _, kind := range terminal {
		if !closedBy(kind) {
			t.Fatalf("%s did not close the job", kind)
		}
	}
	for _, kind := range live {
		if closedBy(kind) {
			t.Fatalf("%s closed a live job", kind)
		}
	}
}

// A spawn recorded after a terminal record is a new attempt of a recycled
// job id — it must not inherit the earlier ending.
func TestStallSpawnAfterTerminalIsNewAttempt(t *testing.T) {
	fx := newStallFixture(t, false)
	at := fx.now.Add(-time.Hour)
	writeJobEvent(t, fx.inbox, "job-a", 1, "job.claim", map[string]any{"owner_lane": "owner-1"}, at)
	writeJobEvent(t, fx.inbox, "job-a", 2, "quota_pool.record", map[string]any{"pool": "devin"}, at)
	writeJobEvent(t, fx.inbox, "job-a", 3, "job.spawned", map[string]any{"pane_id": "w1:p1"}, at)
	writeJobEvent(t, fx.inbox, "job-a", 4, "job.lost", map[string]any{}, at.Add(30*time.Minute))
	writeJobEvent(t, fx.inbox, "job-a", 5, "job.spawned", map[string]any{"pane_id": "w1:p2"}, at.Add(40*time.Minute))
	fx.agents = append(fx.agents, paneIdentity{PaneID: "w1:p2", CWD: "/work/job-a", Harness: "devin"})
	fx.subscribed["w1:p2"] = true
	fx.reads["w1:p2"] = readEvidence{Text: "fresh run\n", Revision: 1}
	fx.scan()
	jobs, err := fx.store.stallJobs(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Attempt != 2 || jobs[0].PaneID != "w1:p2" {
		t.Fatalf("post-terminal spawn did not stay open: %+v", jobs)
	}
}

// A row closed by the retracted escalate/joined rule must reopen on the next
// scan — the durable flag must not fossilize the bug.
func TestStallFalseTerminalRowReopens(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	if err := fx.store.upsertStallJob(context.Background(), stallJobRow{JobID: "job-a", Attempt: 1, Terminal: true, TerminalKind: "job.escalate"}); err != nil {
		t.Fatal(err)
	}
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	jobs, err := fx.store.stallJobs(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range jobs {
		if job.JobID == "job-a" && job.Attempt == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("falsely-terminal row did not reopen")
	}
}

// W2 — the detector may stat the convention file and nothing else. Every
// reference shape points outside jobs/<job>/ at a canary, and the check is
// deterministic rather than judgement: the lstat seam records each path it
// is asked about, a fifo canary hangs any code path that re-learns to open
// it (the watchdog fails the test), and afterwards neither the inbox tree
// nor the store may carry the canary's body. Resurrecting a ReadFile+final/
// copy lands canary bytes under jobs/ — an assertion failure, not a timeout.
func TestStallReportNeverOpensOutsidePaths(t *testing.T) {
	outside := t.TempDir()
	marker := "R4-CANARY-deadbeef42"
	canary := filepath.Join(outside, "canary.md")
	if err := os.WriteFile(canary, []byte(marker+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(outside, "fifo.md")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(outside, "home")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)

	var mu sync.Mutex
	var statted []string

	newFixture := func(t *testing.T) *stallFixture {
		fx := newStallFixture(t, false)
		fx.mgr.deps.lstat = func(path string) (os.FileInfo, error) {
			mu.Lock()
			statted = append(statted, path)
			mu.Unlock()
			return os.Lstat(path)
		}
		return fx
	}
	scan := func(fx *stallFixture) {
		done := make(chan struct{})
		go func() { defer close(done); fx.scan() }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("scan blocked — a path was opened for reading (fifo canary)")
		}
	}
	terminal := func(fx *stallFixture, seq int, raw string) {
		writeRawJobEvent(t, fx.inbox, "job-a", fmt.Sprintf("%05d-job.completed.json", seq), raw)
	}

	cases := []struct {
		name  string
		setup func(t *testing.T, fx *stallFixture)
		state string
		// preExisting names a canary-bearing path the fixture itself placed
		// in the inbox (a hardlink is the same inode) — the walk skips it;
		// the detector still must never open it.
		preExisting string
	}{
		{"note_absolute", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=%s"},"seq":4}`, canary))
		}, "no_report", ""},
		{"toplevel_report_path", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","report_path":%q,"seq":4}`, canary))
		}, "no_report", ""},
		{"payload_report_path", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","payload":{"report_path":%q},"seq":4}`, canary))
		}, "no_report", ""},
		{"toplevel_report_field", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, fmt.Sprintf(`{"kind":"worker.complete","job_id":"job-a","report":%q,"seq":4}`, canary))
		}, "no_report", ""},
		{"uppercase_REPORT_PATH", func(t *testing.T, fx *stallFixture) {
			// encoding/json matches field names case-insensitively — the
			// variant still lands in ReportPath, and must stay a string.
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","REPORT_PATH":%q,"seq":4}`, canary))
		}, "no_report", ""},
		{"mixedcase_Report", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","Report":%q,"seq":4}`, canary))
		}, "no_report", ""},
		{"note_home_relative", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, `{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=~/canary.md"},"seq":4}`)
		}, "no_report", ""},
		{"note_parent_relative", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, `{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=../../../outside.md"},"seq":4}`)
		}, "no_report", ""},
		{"note_fifo_absolute", func(t *testing.T, fx *stallFixture) {
			// A read of this token would block forever — the watchdog above
			// turns that into an assertion failure.
			terminal(fx, 4, fmt.Sprintf(`{"kind":"job.completed","job_id":"job-a","payload":{"note":"done report=%s"},"seq":4}`, fifo))
		}, "no_report", ""},
		{"convention_symlink", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
			if err := os.Symlink(canary, filepath.Join(fx.inbox, "jobs", "job-a", "report.md")); err != nil {
				t.Fatal(err)
			}
		}, "symlink_refused", ""},
		{"convention_hardlink", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
			if err := os.Link(canary, filepath.Join(fx.inbox, "jobs", "job-a", "report.md")); err != nil {
				t.Fatal(err)
			}
		}, "observed", "report.md"},
		{"convention_fifo", func(t *testing.T, fx *stallFixture) {
			terminal(fx, 4, `{"kind":"job.completed","job_id":"job-a","seq":4}`)
			if err := syscall.Mkfifo(filepath.Join(fx.inbox, "jobs", "job-a", "report.md"), 0600); err != nil {
				t.Fatal(err)
			}
		}, "non_regular", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
			tc.setup(t, fx)
			scan(fx)
			row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
			if err != nil || !found || row.State != tc.state {
				t.Fatalf("state=%q want %q: %+v found=%v err=%v", row.State, tc.state, row, found, err)
			}
			// No file the detector leaves behind may contain the canary body.
			err = filepath.Walk(fx.inbox, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return err
				}
				if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
					return nil
				}
				if tc.preExisting != "" && filepath.Base(path) == tc.preExisting {
					return nil
				}
				contents, err := os.ReadFile(path)
				if err == nil && strings.Contains(string(contents), marker) {
					t.Errorf("canary body landed in %s", path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(fx.inbox, "jobs", "job-a", "final")); err == nil {
				t.Fatal("a final/ copy directory was created")
			}
			if strings.Contains(row.RefValue, marker) || strings.Contains(row.Note, marker) {
				t.Fatalf("canary body recorded in the report row: %+v", row)
			}
		})
	}

	// Across every shape, no stat ever touched a path outside the fixture
	// inbox's jobs tree.
	mu.Lock()
	defer mu.Unlock()
	for _, path := range statted {
		if !strings.HasPrefix(path, outside) {
			continue
		}
		t.Fatalf("detector statted an outside path %s", path)
	}
}

// AC9 — three rounds of one job keep separate incident and notification keys.
func TestStallRoundsStayDistinct(t *testing.T) {
	fx := newStallFixture(t, false)
	at := fx.now.Add(-time.Hour)
	writeJobEvent(t, fx.inbox, "job-a", 1, "job.claim", map[string]any{"agent_label": "w", "owner_lane": "owner-1", "parent_lane": "director-1"}, at)
	writeJobEvent(t, fx.inbox, "job-a", 2, "quota_pool.record", map[string]any{"pool": "devin"}, at)
	pane := "w1:p1"
	fx.agents = append(fx.agents, paneIdentity{PaneID: pane, CWD: "/work/job-a", Harness: "devin", Status: "idle"})
	fx.subscribed[pane] = true
	fx.reads[pane] = readEvidence{Text: "ok\n", Revision: 1}
	for round := int64(1); round <= 3; round++ {
		writeJobEvent(t, fx.inbox, "job-a", int(2+round), "job.spawned", map[string]any{"pane_id": pane, "round": round}, at)
		fx.scan()
		// A new attempt re-baselines on its first read; let that happen on
		// clean text, then surface this round's error on the following read.
		fx.advance(time.Minute)
		fx.scan()
		fx.reads[pane] = readEvidence{Text: fmt.Sprintf("%s\n", stallFixtureLimit), Revision: 10 + round}
		fx.advance(time.Minute)
		fx.scan()
		fx.reads[pane] = readEvidence{Text: "ok\n", Revision: 20 + round}
	}
	rows := stallIncidentsFor(t, fx, "job-a")
	byRound := map[int64]int{}
	for _, row := range rows {
		if row.Cause == stallCauseLimitRefused {
			byRound[row.Round]++
		}
	}
	if len(byRound) != 3 {
		t.Fatalf("rounds collapsed into shared keys: %+v", rows)
	}
	for round := int64(1); round <= 3; round++ {
		if byRound[round] != 1 {
			t.Fatalf("round %d has %d incidents", round, byRound[round])
		}
	}
}

// F2 — only the issuer lane extends a deadline, and past overdue history is
// preserved rather than rewritten.
func TestStallDeadlineExtensionIssuerOnly(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", map[string]any{"deadline_at": fx.now.Add(-time.Minute).Format(time.RFC3339)}, fx.now.Add(-2*time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "quiet\n", Revision: 1}
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseOverdue {
		t.Fatalf("setup overdue missing: %+v", stallCauses(rows))
	}
	// A non-issuer extension is inert.
	writeJobEvent(t, fx.inbox, "job-a", 4, "job.deadline", map[string]any{"issuer_lane": "someone-else", "reason": "self-extension attempt", "deadline_at": fx.now.Add(2 * time.Hour).Format(time.RFC3339)}, fx.now)
	fx.scan()
	jobs, err := fx.store.stallJobs(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	var job stallJobRow
	for _, j := range jobs {
		if j.JobID == "job-a" {
			job = j
		}
	}
	if job.DeadlineAt.After(fx.now) {
		t.Fatalf("non-issuer extension moved the deadline: %v", job.DeadlineAt)
	}
	// The issuer's extension applies and the overdue row survives.
	writeJobEvent(t, fx.inbox, "job-a", 5, "job.deadline", map[string]any{"issuer_lane": "owner-1", "reason": "long review", "deadline_at": fx.now.Add(2 * time.Hour).Format(time.RFC3339)}, fx.now)
	fx.scan()
	jobs, _ = fx.store.stallJobs(context.Background(), true)
	for _, j := range jobs {
		if j.JobID == "job-a" && j.Attempt == 1 {
			job = j
		}
	}
	if !job.DeadlineAt.After(fx.now) {
		t.Fatalf("issuer extension was not applied: %v", job.DeadlineAt)
	}
	rows = stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseOverdue {
		t.Fatalf("extension rewrote overdue history: %+v", rows)
	}
	if rows[0].RecoveredAt.IsZero() {
		t.Fatal("the forgiven lapse stayed open after extension")
	}
}

// F11 — the default deadline proposal is measured from reps that include
// interrupted and unfinished jobs, and reports unmeasured below the sample floor.
func TestStallRepsIncludesIncomplete(t *testing.T) {
	fx := newStallFixture(t, false)
	base := fx.now.Add(-24 * time.Hour)
	for i := 0; i < 2; i++ {
		jobID := fmt.Sprintf("done-%d", i)
		writeJobEvent(t, fx.inbox, jobID, 1, "job.claim", map[string]any{"owner_lane": "owner-1"}, base)
		writeJobEvent(t, fx.inbox, jobID, 2, "quota_pool.record", map[string]any{"pool": "devin"}, base)
		writeJobEvent(t, fx.inbox, jobID, 3, "job.completed", map[string]any{}, base.Add(time.Hour))
	}
	// One job still running far past them — an unfinished rep must count.
	writeJobEvent(t, fx.inbox, "stuck-1", 1, "job.claim", map[string]any{"owner_lane": "owner-1"}, base)
	writeJobEvent(t, fx.inbox, "stuck-1", 2, "quota_pool.record", map[string]any{"pool": "devin"}, base)
	writeJobEvent(t, fx.inbox, "stuck-1", 3, "job.spawned", map[string]any{"pane_id": "w1:p9"}, base.Add(6*time.Hour))
	suggestion, samples := repsDeadlineSuggestion(scanStallJobs(fx.inbox), "devin")
	if samples != 3 {
		t.Fatalf("incomplete rep excluded: samples=%d", samples)
	}
	if suggestion < time.Hour {
		t.Fatalf("suggestion ignored the interrupted rep: %v", suggestion)
	}
	// Below the sample floor the detector says unmeasured, not a guess.
	fx2 := newStallFixture(t, false)
	claimJob(t, fx2, "job-lone", "w1:p1", nil, fx2.now.Add(-time.Hour))
	fx2.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx2.scan()
	jobs, err := fx2.store.stallJobs(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if job.JobID == "job-lone" && job.DeadlineSource != stallDeadlineUnmeasured {
			t.Fatalf("unmeasured deadline was silently guessed: %s", job.DeadlineSource)
		}
	}
}

// AC9's negative half — an acked notification is never re-sent across scans,
// while an unacknowledged one resends inside its grace and then escalates to
// the parent lane.
func TestStallNotifyLadderAndRedelivery(t *testing.T) {
	fx := newStallFixture(t, true)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureLimit + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	if len(fx.sent) != 1 || fx.sent[0].OwnerLane != "owner-1" {
		t.Fatalf("owner notification missing: %+v", fx.sent)
	}
	var payload map[string]any
	if json.Unmarshal([]byte(fx.sent[0].Text), &payload) != nil || payload["summary"] != "[limit_refused] 확인 필요" {
		t.Fatalf("notification text malformed: %s", fx.sent[0].Text)
	}
	// Unacknowledged inside grace: redelivery with a fresh event id.
	fx.advance(2 * time.Minute)
	fx.scan()
	if len(fx.sent) != 2 || fx.sent[1].EventID == fx.sent[0].EventID {
		t.Fatalf("redelivery reused or skipped: %+v", fx.sent)
	}
	// Past grace with no ack: the lapsed owner row hands off to the parent
	// lane, which is delivered on the following tick.
	fx.advance(20 * time.Minute)
	fx.scan()
	fx.advance(time.Minute)
	fx.scan()
	found := false
	for _, event := range fx.sent {
		if event.OwnerLane == "director-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unacknowledged notification never reached the parent lane: %+v", fx.sent)
	}
	// Persisted ack stops redelivery entirely.
	before := len(fx.sent)
	for _, event := range fx.sent {
		fx.persisted[event.OwnerLane+"\x00"+event.EventID] = true
	}
	fx.advance(2 * time.Minute)
	fx.scan()
	if len(fx.sent) != before {
		t.Fatalf("acked notification was re-sent: %d -> %d", before, len(fx.sent))
	}
}

// §2-10 — a harness process inside a known worktree that no live agent owns is
// recorded [unowned] on its second sighting; cwd alone never suffices. A live
// pane in the same tree accounts for the process, so the orphan shape is a
// pane that vanished while its harness process stayed.
func TestStallUnownedObservationOnly(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.trees["/work/job-a"] = "/work/job-a"
	fx.trees["/repo/job-a"] = "/work/job-a"
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.procs = []stallProc{{PID: 4242, PPID: 1, StartedAt: fx.now.Add(-30 * time.Minute), Name: "devin"}}
	fx.procCWD[4242] = "/repo/job-a"
	fx.scan()
	// One live pane owns one process: nothing is unowned.
	if rows, err := fx.store.stallIncidentsAll(context.Background(), false); err != nil || len(rows) != 0 {
		t.Fatalf("owned process recorded as unowned: %+v", rows)
	}
	// The pane closes; the harness process keeps running in the same tree.
	fx.agents = nil
	fx.advance(time.Minute)
	fx.scan()
	if rows, err := fx.store.stallIncidentsAll(context.Background(), false); err != nil || len(rows) != 0 {
		t.Fatalf("single sighting recorded unowned: %+v", rows)
	}
	fx.advance(time.Minute)
	fx.scan()
	var found bool
	for _, row := range stallIncidentsFor(t, fx, "") {
		if row.Cause == stallCauseUnowned {
			found = true
		}
	}
	if !found {
		t.Fatal("unowned process was not recorded on second sighting")
	}
}

// 정오표 — two live sessions sharing one worktree record [conflict] once and
// take no action against either.
func TestStallConflictObservation(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-2*time.Hour))
	claimJob(t, fx, "job-b", "w1:p2", nil, fx.now.Add(-time.Hour))
	fx.trees["/work/job-a"] = "/work/job-a"
	fx.trees["/work/job-b"] = "/work/job-b"
	// Point both panes at the same worktree.
	fx.agents[1].CWD = "/work/job-a"
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.reads["w1:p2"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	var conflicts []stallIncidentRow
	for _, row := range stallIncidentsFor(t, fx, "job-a") {
		if row.Cause == stallCauseConflict {
			conflicts = append(conflicts, row)
		}
	}
	if len(conflicts) != 1 {
		t.Fatalf("shared worktree not recorded once: %+v", stallCauses(stallIncidentsFor(t, fx, "job-a")))
	}
	fx.advance(time.Minute)
	fx.scan()
	if again := stallIncidentsFor(t, fx, "job-a"); len(again) != len(stallIncidentsFor(t, fx, "job-a")) || len(conflicts) != 1 {
		t.Fatal("conflict observation was not stable across scans")
	}
}

func stallInt(n int) *int { return &n }

// Node-side beat: after a scan the provider reports the persisted beat, and
// the heartbeat encoder accepts the field on the wire.
func TestStallBeatHeartbeatContract(t *testing.T) {
	fx := newStallFixture(t, false)
	fx.scan()
	beat := fx.mgr.StallBeat()
	if beat == nil || beat.BeatMS != fx.now.UnixMilli() {
		t.Fatalf("beat not recorded: %+v", beat)
	}
	heartbeat := hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, StallDetect: beat}
	payload, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := decodeHubHeartbeatPayload(payload)
	if !ok || decoded.StallDetect == nil || decoded.StallDetect.BeatMS != beat.BeatMS {
		t.Fatalf("hub rejected the stall beat: %s", payload)
	}
}

// B1 — a failed agent.list must not read as "zero panes". The beat records
// observation=nil + degraded, pane judgement is withheld, and a later healthy
// scan with an honestly empty pane set reports panes=0 — three states the
// wire must keep apart.
func TestStallBeatDegradedOnListFailure(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fail := errors.New("herdr agent.list unavailable")
	fx.mgr.deps.listAgents = func(context.Context) ([]paneIdentity, error) { return nil, fail }
	fx.scan()
	beat := fx.mgr.StallBeat()
	if beat == nil || !beat.Degraded || beat.Panes != nil {
		t.Fatalf("list failure must record observation=nil, degraded: %+v", beat)
	}
	// Judgement was withheld: no unobserved/other pane incidents exist.
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("degraded cycle still judged panes: %+v", stallCauses(rows))
	}
	// And the wire keeps the distinction: panes:null, degraded:true.
	payload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, StallDetect: beat})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if json.Unmarshal(payload, &wire) != nil {
		t.Fatal("unmarshal wire")
	}
	stall, _ := wire["stall_detect"].(map[string]any)
	if panes, exists := stall["panes"]; !exists || panes != nil {
		t.Fatalf("degraded beat must carry panes:null on the wire: %s", payload)
	}
	if stall["degraded"] != true {
		t.Fatalf("degraded flag missing on the wire: %s", payload)
	}
	// A valid observation of zero panes is a different state entirely.
	fx.mgr.deps.listAgents = func(context.Context) ([]paneIdentity, error) { return nil, nil }
	fx.advance(time.Minute)
	fx.scan()
	beat = fx.mgr.StallBeat()
	if beat == nil || beat.Degraded || beat.Panes == nil || *beat.Panes != 0 {
		t.Fatalf("empty-but-observed pane set must be panes=0, not degraded: %+v", beat)
	}
}

// B1's sibling — a successful listing whose one tracked pane can never be
// read is partial blindness, not healthy coverage. A single failure stays
// quiet; the second consecutive failure opens [unreadable] and the beat
// goes degraded, and a working read closes the row and clears the flag.
func TestStallUnreadablePaneSurfaces(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	readErr := errors.New("herdr agent.read unavailable")
	fx.mgr.deps.readPane = func(context.Context, string) (readEvidence, error) { return readEvidence{}, readErr }
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("a single read failure recorded an incident: %+v", stallCauses(rows))
	}
	if beat := fx.mgr.StallBeat(); beat == nil || beat.Degraded {
		t.Fatalf("one transient failure must not degrade the beat: %+v", beat)
	}
	fx.advance(time.Minute)
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseUnreadable {
		t.Fatalf("persistent read failure not surfaced: %+v", stallCauses(rows))
	}
	beat := fx.mgr.StallBeat()
	if beat == nil || !beat.Degraded {
		t.Fatalf("partial blindness must mark the beat degraded: %+v", beat)
	}
	// A third failed scan adds no second row.
	fx.advance(time.Minute)
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 1 {
		t.Fatalf("unreadable gap duplicated incidents: %+v", stallCauses(rows))
	}
	// Reads recover: the row closes and the beat is healthy again.
	fx.mgr.deps.readPane = func(_ context.Context, pane string) (readEvidence, error) { return fx.reads[pane], nil }
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.advance(time.Minute)
	fx.scan()
	rows = stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].RecoveredAt.IsZero() {
		t.Fatalf("recovered read did not close the unreadable row: %+v", rows)
	}
	if beat := fx.mgr.StallBeat(); beat.Degraded {
		t.Fatal("beat stayed degraded after reads recovered")
	}
}

// AC5 — a node whose stall beat stops advancing, then vanishes, raises the
// hub no-data alert.
func TestStallHubNoDataAlert(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var alerts []HubAlert
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{hubOperatorMachineID: "op", "node-a": "node"},
		Now:    func() time.Time { return now },
		Notifier: hubNotifierFunc(func(_ context.Context, alert HubAlert) error {
			alerts = append(alerts, alert)
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-a", "test", "fixture", agent, true)
	send := func(beat *hubStallBeatPayload) {
		task268SendHeartbeat(t, hub, "node-a", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, StallDetect: beat})
	}
	send(&hubStallBeatPayload{BeatMS: now.Add(-time.Minute).UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	now = now.Add(30 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.Add(-10 * time.Second).UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	if len(alerts) != 0 {
		t.Fatalf("advancing beat alerted: %+v", alerts)
	}
	// Beat frozen past the threshold.
	now = now.Add(4 * time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.Add(-5 * time.Minute).UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	now = now.Add(10 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.Add(-5 * time.Minute).UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	if len(alerts) != 1 || alerts[0].Reason != hubAlertReasonNoData || alerts[0].MachineID != "node-a" {
		t.Fatalf("no-data alert missing: %+v", alerts)
	}
	// Recovery once the beat advances again.
	now = now.Add(time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	now = now.Add(10 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(2)})
	var recovered bool
	for _, alert := range alerts {
		if alert.Recovery && alert.Reason == hubAlertReasonNoData {
			recovered = true
		}
	}
	if !recovered {
		t.Fatalf("no-data recovery missing: %+v", alerts)
	}
	// A node that never reported never alerts.
	hub.connect("node-b", "test", "fixture", &hubAgent{}, true)
	task268SendHeartbeat(t, hub, "node-b", &hubAgent{}, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}})
	_ = alerts
}

// AC10 — the detector source contains no write path to panes, batch values,
// or quota numbers, and no route to them through a helper either. The scan
// covers every stall*.go source file — a new helper file is the same write
// path with a different name — and includes the generic herdr escape hatches
// (Call, Prompt, the concrete client type), which are the only ways around
// the stallReader interface.
func TestStallNoMutationPaths(t *testing.T) {
	files, err := filepath.Glob("stall*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("stall sources not found: %v", err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{
			"send_keys", "sendKeys", "SendKeys", "agent.prompt", "pane.prompt",
			"Process.Kill", ".Kill(", "scopefuel", "quota_pool.update", "batch_",
			"SendMessage", "Prompt(", ".Call(", "HerdrClient",
		} {
			if strings.Contains(string(body), banned) {
				t.Fatalf("%s contains forbidden write path %q", file, banned)
			}
		}
	}
}

// B1 hub half — a degraded/nil-observation beat fires the stall_degraded
// alert while a genuinely-observed panes=0 stays quiet.
func TestStallHubDegradedAlert(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	var alerts []HubAlert
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{hubOperatorMachineID: "op", "node-a": "node"},
		Now:    func() time.Time { return now },
		Notifier: hubNotifierFunc(func(_ context.Context, alert HubAlert) error {
			alerts = append(alerts, alert)
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-a", "test", "fixture", agent, true)
	send := func(beat *hubStallBeatPayload) {
		task268SendHeartbeat(t, hub, "node-a", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, StallDetect: beat})
	}
	// Establish a healthy reporter, then a real empty observation.
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(0)})
	now = now.Add(30 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(0)})
	for _, alert := range alerts {
		if !alert.Recovery {
			t.Fatalf("panes=0 alerted — valid observation was read as degraded: %+v", alerts)
		}
	}
	// The listing fails: the beat still advances but the observation is nil.
	now = now.Add(time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: nil, Degraded: true})
	now = now.Add(30 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: nil, Degraded: true})
	var degraded bool
	for _, alert := range alerts {
		if !alert.Recovery && alert.Reason == hubAlertReasonStallDegraded {
			degraded = true
		}
	}
	if !degraded {
		t.Fatalf("degraded observation did not alert: %+v", alerts)
	}
	// Nil panes without the flag is still an unobserved cycle.
	now = now.Add(time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: nil})
	if reason := hub.stallBeats["node-a"]; reason == nil {
		t.Fatal("beat state vanished")
	}
	// Healthy beat returns: degraded clears after the debounce.
	now = now.Add(2 * time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(3)})
	now = now.Add(30 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: stallInt(3)})
	var recovered bool
	for _, alert := range alerts {
		if alert.Recovery && alert.Reason == hubAlertReasonStallDegraded {
			recovered = true
		}
	}
	if !recovered {
		t.Fatalf("degraded recovery missing: %+v", alerts)
	}
}

// [superseded] — a pane that takes a new job while its previous job has no
// terminal event is recorded on the old job. Observation only: nothing is
// emitted, sent, or mutated beyond the row's own mark.
func TestStallSupersededObservation(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-2*time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("setup produced incidents: %+v", stallCauses(rows))
	}
	// The worker takes job-b on the same pane; job-a never completed.
	claimJob(t, fx, "job-b", "w1:p1", nil, fx.now.Add(-time.Minute))
	fx.advance(time.Minute)
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseSuperseded {
		t.Fatalf("superseded not recorded on the orphaned job: %+v", stallCauses(rows))
	}
	var evidence map[string]any
	if err := json.Unmarshal(rows[0].Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence["superseded_by"] != "job-b" {
		t.Fatalf("superseded evidence does not name the new job: %v", evidence)
	}
	// job-a's row is marked so the detector stops reading it; job-b stays
	// live and unencumbered.
	jobs, err := fx.store.stallJobs(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].JobID != "job-b" {
		t.Fatalf("open set wrong after supersede: %+v", jobs)
	}
	// Zero outward action: no lane events, nothing to the pane.
	if len(fx.sent) != 0 {
		t.Fatalf("superseded observation acted on the world: sent=%d", len(fx.sent))
	}
	// Stable across scans — no stacking.
	fx.advance(time.Minute)
	fx.scan()
	if again := stallIncidentsFor(t, fx, "job-a"); len(again) != 1 {
		t.Fatalf("superseded duplicated: %+v", stallCauses(again))
	}
}

// J4 — an unavailable subscription set (nil) is "not proven", not a proven
// empty set. It gets a grace window before [unobserved] is recorded.
func TestStallCoverageNilSetGrace(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.subscribed = nil // subscription evidence itself is missing
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("unproven coverage recorded unobserved immediately: %+v", stallCauses(rows))
	}
	if fx.resubs == 0 {
		t.Fatal("unproven coverage did not request resubscribe")
	}
	fx.advance(90 * time.Second)
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-a"); len(rows) != 0 {
		t.Fatalf("inside grace window an incident was recorded: %+v", stallCauses(rows))
	}
	fx.advance(time.Minute)
	fx.scan()
	rows := stallIncidentsFor(t, fx, "job-a")
	if len(rows) != 1 || rows[0].Cause != stallCauseUnobserved {
		t.Fatalf("prolonged unproven coverage not recorded: %+v", stallCauses(rows))
	}
	// The distinction: a proven-empty set records without the grace wait.
	fx2 := newStallFixture(t, false)
	claimJob(t, fx2, "job-b", "w1:p2", nil, fx2.now.Add(-time.Hour))
	fx2.reads["w1:p2"] = readEvidence{Text: "ok\n", Revision: 1}
	delete(fx2.subscribed, "w1:p2")
	fx2.scan()
	if rows := stallIncidentsFor(t, fx2, "job-b"); len(rows) != 1 || rows[0].Cause != stallCauseUnobserved {
		t.Fatalf("proven-empty coverage was not recorded: %+v", stallCauses(rows))
	}
}

// J6d — a launcher and its harness child in one worktree are one worker:
// the parent/child pair must not double-count into [unowned].
func TestStallUnownedCollapsesProcessTree(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.trees["/work/job-a"] = "/work/job-a"
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	started := fx.now.Add(-30 * time.Minute)
	// devin wrapper (4242) + child (4243) in the same tree, pane gone.
	fx.procs = []stallProc{
		{PID: 4242, PPID: 1, StartedAt: started, Name: "devin"},
		{PID: 4243, PPID: 4242, StartedAt: started, Name: "devin"},
	}
	fx.procCWD[4242] = "/work/job-a"
	fx.procCWD[4243] = "/work/job-a"
	// First scan while the pane is alive: the process tree is owned, and the
	// pane's cwd lands in the known-tree set.
	fx.scan()
	if rows := stallIncidentsFor(t, fx, ""); len(rows) != 0 {
		t.Fatalf("owned process tree recorded unowned: %+v", stallCauses(rows))
	}
	fx.agents = nil
	fx.advance(time.Minute)
	fx.scan()
	fx.advance(time.Minute)
	fx.scan()
	var unowned int
	for _, row := range stallIncidentsFor(t, fx, "") {
		if row.Cause == stallCauseUnowned {
			unowned++
		}
	}
	if unowned != 1 {
		t.Fatalf("wrapper+child counted as %d unowned, want 1", unowned)
	}
}

// J1 — the no-write guarantee is enforced at the type and wiring level, not
// by hoping nobody adds a call: stallDeps may only ever carry the whitelisted
// read/record seams, and the daemon's wiring block may only use the read-only
// herdr methods.
func TestStallDepsAreReadOnly(t *testing.T) {
	allowed := map[string]bool{
		"listAgents": true, "readPane": true, "subscribed": true, "resubscribe": true,
		"worktree": true, "procs": true, "procCWD": true, "writeRecord": true,
		"enqueue": true, "lanePersisted": true, "lstat": true, "now": true,
	}
	depsType := reflect.TypeOf(stallDeps{})
	for i := 0; i < depsType.NumField(); i++ {
		field := depsType.Field(i)
		if !allowed[field.Name] {
			t.Fatalf("stallDeps grew a non-whitelisted seam %q — every dep must be read-only or local-record", field.Name)
		}
	}
	// The interface itself is the type-level guarantee: it may only ever
	// name read methods. Adding Prompt/SendKeys/Call to it fails here even
	// if no call site uses them yet.
	reader := reflect.TypeOf((*stallReader)(nil)).Elem()
	for i := 0; i < reader.NumMethod(); i++ {
		switch reader.Method(i).Name {
		case "AgentDetails", "ReadPane", "Close":
		default:
			t.Fatalf("stallReader grew a non-read method %q", reader.Method(i).Name)
		}
	}
	// The daemon wiring may only hand the detector read methods. Adding a
	// pane-write dep — or routing Prompt/send_keys through an existing one —
	// fails this check. Every method call on any receiver in the block is
	// whitelisted, so renaming the client variable is not a bypass.
	body, err := os.ReadFile("daemon.go")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(body), "deps := stallDeps{")
	if start < 0 {
		t.Fatal("stallDeps wiring block not found in daemon.go")
	}
	end := strings.Index(string(body[start:]), "\n\t}")
	if end < 0 {
		t.Fatal("stallDeps wiring block unterminated")
	}
	block := string(body[start : start+end])
	for _, banned := range []string{"send_keys", "sendKeys", "SendKeys", "agent.prompt", "Prompt(", "pane.input", "Write(", ".Call("} {
		if strings.Contains(block, banned) {
			t.Fatalf("daemon stallDeps wiring references %q", banned)
		}
	}
	methodWhitelist := map[string]bool{
		// herdr reads through stallReader, the coverage seam, and the hub
		// client's beat setter — none of which can express pane input.
		"AgentDetails": true, "ReadPane": true, "Close": true, "subscribedSet": true,
		"SetStallDetect": true,
	}
	for _, call := range regexp.MustCompile(`\.(\w+)\(`).FindAllStringSubmatch(block, -1) {
		if !methodWhitelist[call[1]] {
			t.Fatalf("stall wiring calls non-read method %s", call[1])
		}
	}
}

// J2 — notification evidence is an allowlist. An implementation must not
// smuggle a verdict-shaped field (worker_state:"hung" and friends) into the
// lane payload.
func TestStallNotifyEvidenceAllowlist(t *testing.T) {
	fx := newStallFixture(t, true)
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: "ok\n", Revision: 1}
	fx.scan()
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureLimit + "\n", Revision: 2}
	fx.advance(time.Minute)
	fx.scan()
	if len(fx.sent) != 1 {
		t.Fatalf("notification missing: %+v", fx.sent)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(fx.sent[0].Text), &payload); err != nil {
		t.Fatal(err)
	}
	topAllowed := map[string]bool{"observation": true, "job": true, "attempt": true, "round": true, "occurrence": true, "pane": true, "summary": true, "evidence": true}
	for key := range payload {
		if !topAllowed[key] {
			t.Fatalf("notification carries non-allowlisted key %q: %s", key, fx.sent[0].Text)
		}
	}
	if _, exists := payload["kind"]; exists {
		t.Fatalf("notification still carries a kind token: %s", fx.sent[0].Text)
	}
	if evidence, ok := payload["evidence"].(map[string]any); ok {
		evidenceAllowed := map[string]bool{"pane": true, "observed_at": true, "revision": true}
		for key := range evidence {
			if !evidenceAllowed[key] {
				t.Fatalf("notification evidence carries non-allowlisted key %q: %s", key, fx.sent[0].Text)
			}
		}
	}
}

type hubNotifierFunc func(context.Context, HubAlert) error

func (f hubNotifierFunc) Send(ctx context.Context, alert HubAlert) error { return f(ctx, alert) }
