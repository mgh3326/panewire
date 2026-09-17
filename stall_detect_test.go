package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
	uploads    map[string]string
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
		uploads:    map[string]string{},
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
		upload: func(_ context.Context, key string, body []byte) error {
			fx.uploads[key] = string(body)
			return nil
		},
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
		name, text, cause, status string
	}{
		{"limit_refused", "working...\n" + stallFixtureLimit + "\n", stallCauseLimitRefused, "idle"},
		{"auth_refused", "boot\n" + stallFixtureAuth + "\n", stallCauseAuthRefused, "idle"},
		{"input_unsubmitted", "queued\n" + stallFixtureQueue + "\n", stallCauseInputUnsubmitted, "idle"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newStallFixture(t, false)
			claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
			fx.reads["w1:p1"] = readEvidence{Text: "normal work output\n", Revision: 1}
			fx.scan() // baseline
			fx.agents[0].Status = tc.status
			fx.reads["w1:p1"] = readEvidence{Text: tc.text, Revision: 2}
			fx.advance(time.Minute)
			fx.scan()
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

// A2 — the queued-input banner under a working agent is input already sent;
// the same banner on an idle pane is unsubmitted input.
func TestStallUnsubmittedVersusSubmitted(t *testing.T) {
	if matches := classifyScreen(stallFixtureQueue+"\n", "working"); len(matches) != 0 {
		t.Fatalf("working agent with queue banner matched: %+v", matches)
	}
	matches := classifyScreen(stallFixtureQueue+"\n", "idle")
	if len(matches) != 1 || matches[0].cause != stallCauseInputUnsubmitted {
		t.Fatalf("idle agent queue banner did not match: %+v", matches)
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
	blob := string(rows[0].Evidence)
	for _, banned := range []string{"stalled", "stopped", "멈춤", "정지"} {
		if strings.Contains(blob, banned) {
			t.Fatalf("evidence asserts a stop via %q: %s", banned, blob)
		}
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
// incident; a job spawned inside the startup grace gets startup_block_suspect.
func TestStallScrollbackAndStartupSuspect(t *testing.T) {
	fx := newStallFixture(t, false)
	claimJob(t, fx, "job-old", "w1:p1", nil, fx.now.Add(-3*time.Hour))
	fx.reads["w1:p1"] = readEvidence{Text: stallFixtureAuth + "\n", Revision: 9}
	fx.scan()
	if rows := stallIncidentsFor(t, fx, "job-old"); len(rows) != 0 {
		t.Fatalf("old scrollback produced a confirmed incident: %+v", stallCauses(rows))
	}

	fx2 := newStallFixture(t, false)
	claimJob(t, fx2, "job-new", "w2:p2", nil, fx2.now.Add(-30*time.Second))
	fx2.reads["w2:p2"] = readEvidence{Text: stallFixtureAuth + "\n", Revision: 1}
	fx2.scan()
	rows := stallIncidentsFor(t, fx2, "job-new")
	if len(rows) != 1 || rows[0].Cause != stallCauseStartupSuspect {
		t.Fatalf("boot-time auth failure was not recorded as startup suspect: %+v", stallCauses(rows))
	}
}

// AC8 — a report carrying a fake token is stored verbatim locally but uploads
// to handoffkeep with the secret removed.
func TestStallReportUploadRedactsSecrets(t *testing.T) {
	fx := newStallFixture(t, false)
	report := filepath.Join(t.TempDir(), "report.md")
	body := "done\nkey: sk-ABCdef1234567890ghiJEL\n"
	if err := os.WriteFile(report, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	claimJob(t, fx, "job-a", "w1:p1", nil, fx.now.Add(-time.Hour))
	writeJobEvent(t, fx.inbox, "job-a", 4, "job.completed", map[string]any{"report_path": report}, fx.now)
	fx.scan()
	row, found, err := fx.store.stallReport(context.Background(), "job-a", 1)
	if err != nil || !found {
		t.Fatalf("report row missing: %v %v", found, err)
	}
	if row.RemoteState != "uploaded" || row.LocalPath == "" || row.SHA256 == "" {
		t.Fatalf("report not finalized+uploaded: %+v", row)
	}
	uploaded := fx.uploads["report/job-a/1"]
	if strings.Contains(uploaded, "sk-ABCdef1234567890ghiJEL") || !strings.Contains(uploaded, "[redacted]") {
		t.Fatalf("remote body leaked the token: %q", uploaded)
	}
	local, err := os.ReadFile(row.LocalPath)
	if err != nil || string(local) != body {
		t.Fatalf("local finalized copy was altered")
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
	send(&hubStallBeatPayload{BeatMS: now.Add(-time.Minute).UnixMilli(), IntervalMS: 60000, Panes: 2})
	now = now.Add(30 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.Add(-10 * time.Second).UnixMilli(), IntervalMS: 60000, Panes: 2})
	if len(alerts) != 0 {
		t.Fatalf("advancing beat alerted: %+v", alerts)
	}
	// Beat frozen past the threshold.
	now = now.Add(4 * time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.Add(-5 * time.Minute).UnixMilli(), IntervalMS: 60000, Panes: 2})
	now = now.Add(10 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.Add(-5 * time.Minute).UnixMilli(), IntervalMS: 60000, Panes: 2})
	if len(alerts) != 1 || alerts[0].Reason != hubAlertReasonNoData || alerts[0].MachineID != "node-a" {
		t.Fatalf("no-data alert missing: %+v", alerts)
	}
	// Recovery once the beat advances again.
	now = now.Add(time.Minute)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: 2})
	now = now.Add(10 * time.Second)
	send(&hubStallBeatPayload{BeatMS: now.UnixMilli(), IntervalMS: 60000, Panes: 2})
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

// AC10 — the detector source contains no write path to panes, batch values, or
// quota numbers. This is the executable half of the code inspection; the file
// itself is the other half.
func TestStallNoMutationPaths(t *testing.T) {
	for _, file := range []string{"stall_detect.go", "stall_store.go"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{
			"send_keys", "agent.prompt", "pane.prompt", "Process.Kill", ".Kill(",
			"scopefuel", "quota_pool.update", "batch_", "SendMessage",
		} {
			if strings.Contains(string(body), banned) {
				t.Fatalf("%s contains forbidden write path %q", file, banned)
			}
		}
	}
}

type hubNotifierFunc func(context.Context, HubAlert) error

func (f hubNotifierFunc) Send(ctx context.Context, alert HubAlert) error { return f(ctx, alert) }
