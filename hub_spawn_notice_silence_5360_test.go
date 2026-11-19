package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 5360: a spawn failure the requester replays by itself is counted by cause
// and not sent to a design owner; the cause rules and --owner-notify decide the
// rest. The requester's own copy is never silenced.

const s5360Owner = "mergewake-design"
const s5360Requester = "mergewake-daemon"

// s5360Spawn claims and completes one spawn for job with outcome, with the
// request's args plus extra. It returns the task.
func s5360Spawn(t *testing.T, h *HubServer, job string, extra []string, outcome hubSpawnOutcome) hubSpawnTask {
	t.Helper()
	req := t4270Request(job, s5360Owner, s5360Requester, "implementer")
	req.Args = append(req.Args, extra...)
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, s5360Owner, h.now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, outcome, h.now()); err != nil {
		t.Fatal(err)
	}
	return task
}

// s5360GiveUp ends task's attempt given_up the way the expiry does: the
// operation is marked, and its given-up notice is queued in the same transaction.
func s5360GiveUp(t *testing.T, h *HubServer, task hubSpawnTask, now time.Time) {
	t.Helper()
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET status='given_up' WHERE request_id=? AND attempt=?`, task.Request.RequestID, task.Attempt); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(hubSpawnOutcome{RC: 84, Stage: "queue", Reason: "expired: expires_at passed", Retryable: false})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.inbox.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = queueSpawnNotifications(t.Context(), h.inbox.store, tx, task, "given_up", string(raw), now, false); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// s5360Flush routes every pending notice, twice, so a settled row and its
// follow-on state are both read.
func s5360Flush(t *testing.T, h *HubServer) {
	t.Helper()
	for range 2 {
		h.flushSpawnNotifications(t.Context())
	}
}

// s5360OwnerSilenced is the number of owner-lane silences the daily row counts
// for owner, across causes.
func s5360OwnerSilenced(t *testing.T, h *HubServer, now time.Time) int64 {
	t.Helper()
	doc := h.loopsRowDocument(loopDateOf(now), now)
	var total int64
	for _, row := range doc.SpawnNotices {
		if row.Lane == s5360Owner {
			total += row.Silenced
		}
	}
	return total
}

func s5360Retryable(reason string) hubSpawnOutcome {
	return hubSpawnOutcome{RC: 81, Stage: "checkout", Reason: reason, Fix: "Run panewire again.", Host: "rpi", Profile: "haiku", Retryable: true}
}

func s5360Terminal(reason string) hubSpawnOutcome {
	return hubSpawnOutcome{RC: 70, Stage: "launch", Reason: reason, Fix: "Check the checkout.", Host: "rpi", Retryable: false}
}

// s5360Hub is a hub whose clock the test steps, with the owner and requester
// lanes routed.
func s5360Hub(t *testing.T, now *time.Time) (*HubServer, *hubAgent) {
	t.Helper()
	h, _, destination := spawnNotifyHubLanes(t, "", s5360Owner, s5360Requester)
	h.now = func() time.Time { return *now }
	return h, destination
}

// A replayable failure is counted by cause and sent to no design owner inbox;
// the requester still gets its own copy, which is what it replays from.
func TestRetryableFailureIsCountedNotSent_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, destination := s5360Hub(t, &now)
	s5360Spawn(t, h, "pw-5360-default-1", nil, s5360Retryable("checkout_refused:locked"))
	s5360Flush(t, h)

	if items := t4270Items(t, h, s5360Owner); len(items) != 0 {
		t.Fatalf("the owner heard a failure its requester can replay: %+v", items)
	}
	if got := spawnFailedInjections(destination); len(got) != 1 || got[0].Lane != s5360Requester {
		t.Fatalf("the requester got %+v, want its own SPAWN_FAILED copy", got)
	}
	doc := h.loopsRowDocument(loopDateOf(now), now)
	if len(doc.SpawnNotices) != 1 || doc.SpawnNotices[0].Lane != s5360Owner || doc.SpawnNotices[0].Cause != "checkout_refused:locked" || doc.SpawnNotices[0].Silenced != 1 {
		t.Fatalf("daily row spawn_notices=%+v, want one silenced checkout_refused:locked", doc.SpawnNotices)
	}
}

// The same cause three times for one owner within 30 minutes notifies once, on
// the third occurrence; the window then starts over.
func TestRepeatedCauseNotifiesOnThirdWithinWindow_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	for i := range 2 {
		s5360Spawn(t, h, fmt.Sprintf("pw-5360-repeat-%d", i), nil, s5360Retryable("checkout_refused:locked"))
		s5360Flush(t, h)
		now = now.Add(5 * time.Minute)
	}
	if items := t4270Items(t, h, s5360Owner); len(items) != 0 {
		t.Fatalf("the owner heard the first two occurrences: %+v", items)
	}
	s5360Spawn(t, h, "pw-5360-repeat-2", nil, s5360Retryable("checkout_refused:locked"))
	s5360Flush(t, h)
	items := t4270Items(t, h, s5360Owner)
	if len(items) != 1 || !strings.Contains(items[0].Event.Text, "checkout_refused:locked") {
		t.Fatalf("owner items after the third occurrence=%+v, want exactly one notice", items)
	}
	if n := s5360OwnerSilenced(t, h, now); n != 2 {
		t.Fatalf("silenced=%d, want the two occurrences the third one did not replace", n)
	}
}

// Occurrences spread beyond the window do not add up to the repeat.
func TestRepeatedCauseOutsideWindowStaysSilent_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	for i := range 3 {
		s5360Spawn(t, h, fmt.Sprintf("pw-5360-spread-%d", i), nil, s5360Retryable("checkout_refused:locked"))
		s5360Flush(t, h)
		now = now.Add(16 * time.Minute)
	}
	if items := t4270Items(t, h, s5360Owner); len(items) != 0 {
		t.Fatalf("three occurrences 16 minutes apart notified: %+v", items)
	}
}

// A must-fix cause is told at once, however often it recurs.
func TestMustFixCauseIsAlwaysTold_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	for _, reason := range []string{"spawn_failed:workspace_unresolved: no workspace", "spawn_failed:brief_upload"} {
		s5360Spawn(t, h, fmt.Sprintf("pw-5360-must-%d", len(reason)), nil, s5360Retryable(reason))
	}
	s5360Flush(t, h)
	items := t4270Items(t, h, s5360Owner)
	if len(items) != 2 {
		t.Fatalf("owner items=%d, want one notice per must-fix failure", len(items))
	}
	if n := s5360OwnerSilenced(t, h, now); n != 0 {
		t.Fatalf("a must-fix cause was counted as silenced (%d)", n)
	}
}

// A failure that cannot be replayed, and a request that is given up, are told.
func TestTerminalAndGivenUpFailuresAreTold_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	s5360Spawn(t, h, "pw-5360-terminal", nil, s5360Terminal("spawn_failed"))
	given := s5360Spawn(t, h, "pw-5360-given-up", nil, s5360Retryable("checkout_refused:locked"))
	s5360GiveUp(t, h, given, now)
	s5360Flush(t, h)
	items := t4270Items(t, h, s5360Owner)
	if len(items) != 2 {
		t.Fatalf("owner items=%d, want the terminal failure and the given-up request", len(items))
	}
	for _, item := range items {
		if !strings.Contains(item.Event.Text, "SPAWN_FAILED") {
			t.Fatalf("item %q is not a SPAWN_FAILED notice", item.Event.Text)
		}
	}
}

// A retryable failure that is settled leaves nothing held: the hold a design
// owner used to wait out is gone (the replay and the give-up decide it).
func TestRetryableSpawnNoticeIsNotHeld_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	s5360Spawn(t, h, "pw-5360-not-held", nil, s5360Retryable("checkout_refused:locked"))
	s5360Flush(t, h)
	now = now.Add(h.quietTimings.spawnFailureHold + time.Minute)
	s5360Flush(t, h)
	if held, _ := h.readSpawnNotifications(t.Context(), spawnNoticeHeld); len(held) != 0 {
		t.Fatalf("held rows=%d, want none", len(held))
	}
	if items := t4270Items(t, h, s5360Owner); len(items) != 0 {
		t.Fatalf("the owner was told after the old hold would have ended: %+v", items)
	}
}

// --owner-notify never: the owner hears none of the spawn's notices, whatever
// their cause; the requester's copy still reaches it.
func TestOwnerNotifyNeverSendsNoOwnerNotice_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, destination := s5360Hub(t, &now)
	never := []string{fleet.OwnerNotifyArg, fleet.OwnerNotifyNever}
	s5360Spawn(t, h, "pw-5360-never-must", never, s5360Retryable("spawn_failed:workspace_unresolved"))
	s5360Spawn(t, h, "pw-5360-never-terminal", never, s5360Terminal("spawn_failed"))
	success := hubSpawnOutcome{RC: 0, Host: "rpi", Pane: "w1:p9", Profile: "haiku", Model: "m"}
	s5360Spawn(t, h, "pw-5360-never-spawned", never, success)
	s5360Flush(t, h)

	if items := t4270Items(t, h, s5360Owner); len(items) != 0 {
		t.Fatalf("never: the owner heard %+v", items)
	}
	if got := spawnFailedInjections(destination); len(got) != 2 {
		t.Fatalf("never: the requester got %d failure copies, want both of its own", len(got))
	}
	if n := s5360OwnerSilenced(t, h, now); n != 3 {
		t.Fatalf("never: silenced=%d, want 3 (must-fix, terminal, spawned)", n)
	}
}

// --owner-notify final: the owner hears only a terminal or given-up failure.
func TestOwnerNotifyFinalSendsOnlyTerminalNotice_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _ := s5360Hub(t, &now)
	final := []string{fleet.OwnerNotifyArg, fleet.OwnerNotifyFinal}
	s5360Spawn(t, h, "pw-5360-final-must", final, s5360Retryable("spawn_failed:workspace_unresolved"))
	success := hubSpawnOutcome{RC: 0, Host: "rpi", Pane: "w1:p9", Profile: "haiku", Model: "m"}
	s5360Spawn(t, h, "pw-5360-final-spawned", final, success)
	lane := success
	lane.LaneUnregistered = true
	s5360Spawn(t, h, "pw-5360-final-unregistered", final, lane)
	s5360Spawn(t, h, "pw-5360-final-terminal", final, s5360Terminal("spawn_failed"))
	given := s5360Spawn(t, h, "pw-5360-final-given-up", final, s5360Retryable("checkout_refused:locked"))
	s5360GiveUp(t, h, given, now)
	s5360Flush(t, h)

	items := t4270Items(t, h, s5360Owner)
	if len(items) != 2 {
		t.Fatalf("final: owner items=%d, want the terminal failure and the given-up request only", len(items))
	}
	for _, item := range items {
		if strings.Contains(item.Event.Text, "SPAWNED") || strings.Contains(item.Event.Text, "LANE_UNREGISTERED") {
			t.Fatalf("final: a non-terminal notice reached the owner: %q", item.Event.Text)
		}
	}
	if n := s5360OwnerSilenced(t, h, now); n != 3 {
		t.Fatalf("final: silenced=%d, want 3 (must-fix, spawned, unregistered)", n)
	}
}

// The job's completion is the owner's too: never and final both withhold it
// (recorded in hk under owner-notify), and an absent flag delivers it.
func TestOwnerNotifyJobCompletedWithheld_5360(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want int
	}{{"", 1}, {fleet.OwnerNotifyNever, 0}, {fleet.OwnerNotifyFinal, 0}} {
		t.Run("mode="+tc.mode, func(t *testing.T) {
			h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			job := "pw-5360-completed-" + tc.mode
			if tc.mode == "" {
				job += "default"
			}
			req := t4270Request(job, t3426Design, "", "implementer")
			if tc.mode != "" {
				req.Args = append(req.Args, fleet.OwnerNotifyArg, tc.mode)
			}
			if _, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, t3426Design, time.Now()); err != nil {
				t.Fatal(err)
			}
			h.relayJobCompletionFrom("host-a", t3426Completion(job, t3426Design, ""))

			if items := t4270Items(t, h, t3426Design); len(items) != tc.want {
				t.Fatalf("mode %q: owner items=%d, want %d", tc.mode, len(items), tc.want)
			}
			if tc.want != 0 {
				return
			}
			rows, deliveredTo := t4270Rows(fake, job)
			if len(rows) == 0 || !strings.HasPrefix(deliveredTo[0], "hub/filtered:owner-notify") {
				t.Fatalf("mode %q: hk rows=%d delivered_to=%v, want the completion recorded as filtered:owner-notify", tc.mode, len(rows), deliveredTo)
			}
		})
	}
}

// The mode is a flag on the spawn, and the saved request keeps it: a request
// without it reads as absent, and a bad value reads as absent too.
func TestOwnerNotifyModeReadsSavedArgs_5360(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{nil, ""},
		{[]string{fleet.OwnerNotifyArg, fleet.OwnerNotifyNever}, fleet.OwnerNotifyNever},
		{[]string{fleet.OwnerNotifyArg, fleet.OwnerNotifyFinal}, fleet.OwnerNotifyFinal},
		{[]string{fleet.OwnerNotifyArg, "sometimes"}, ""},
	}
	for _, tc := range cases {
		if got := ownerNotifyMode(tc.args); got != tc.want {
			t.Fatalf("ownerNotifyMode(%v)=%q, want %q", tc.args, got, tc.want)
		}
	}
}

// A node that predates owner_notify_v1 refuses the unknown flag and drops the
// whole spawn, so the hub strips the flag and its value before it sends. The
// flag's neighbours stay.
func TestOwnerNotifyFlagStrippedForOlderNode_5360(t *testing.T) {
	args := []string{"-c", "/work", fleet.OwnerNotifyArg, fleet.OwnerNotifyNever, "--role", "reviewer"}
	got := withoutSpawnFlag(args, fleet.OwnerNotifyArg)
	want := []string{"-c", "/work", "--role", "reviewer"}
	if !slices.Equal(got, want) {
		t.Fatalf("stripped args=%v, want %v", got, want)
	}
}

// The node and hub advertise the feature name a requester gates on.
func TestOwnerNotifyCapabilityAdvertised_5360(t *testing.T) {
	if !slices.Contains(hubSpawnCapabilities, "owner_notify_v1") || hubCapabilityOwnerNotify != fleet.OwnerNotifyCapability {
		t.Fatalf("node capabilities %v do not advertise owner_notify_v1", hubSpawnCapabilities)
	}
	raw, err := versionJSON("1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Version      string   `json:"version"`
		Capabilities []string `json:"capabilities"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Version != "1.2.3" || !slices.Contains(doc.Capabilities, "owner_notify_v1") {
		t.Fatalf("panewire version --json=%s, want version and owner_notify_v1", raw)
	}
	h := &HubServer{}
	recorder := httptest.NewRecorder()
	h.handleCapabilities(recorder, httptest.NewRequest(http.MethodGet, "/v1/capabilities", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "owner_notify_v1") {
		t.Fatalf("hub capabilities %d %s, want owner_notify_v1", recorder.Code, recorder.Body.String())
	}
}

// The must-fix list is config: absent takes the built-in list, an explicit
// empty list has none, and a malformed cause refuses the policy at load.
func TestSpawnNoticeMustFixConfig_5360(t *testing.T) {
	base := "default: interrupt\nrelay:\n"
	policy, err := parseDeliveryPolicy([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(policy.Relay.SpawnNoticeMustFix, defaultSpawnNoticeMustFix) {
		t.Fatalf("absent list=%v, want the default", policy.Relay.SpawnNoticeMustFix)
	}
	policy, err = parseDeliveryPolicy([]byte(base + "  spawn_notice_must_fix: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if policy.spawnNoticeMustFix("workspace_unresolved") {
		t.Fatal("an explicit empty list still names a must-fix cause")
	}
	if _, err := parseDeliveryPolicy([]byte(base + "  spawn_notice_must_fix: [Bad Cause]\n")); err == nil {
		t.Fatal("a malformed must-fix cause was accepted")
	}
}

// A daemon-started reviewer's completion is a reviewer lifecycle record for a
// design lane, but a spawn that names --owner-notify withholds it from the
// owner; the withheld completion is still filed as owner-notify and counted
// (mergewake review of PR 732).
func TestOwnerNotifyDaemonReviewerCompletionIsRecordedAndCounted_5360(t *testing.T) {
	h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return now }
	const job = "mwr-5360-review-never"
	req := t4270Request(job, t3426Design, "", "reviewer")
	req.Args = append(req.Args, fleet.OwnerNotifyArg, fleet.OwnerNotifyNever)
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, t3426Design, now); err != nil {
		t.Fatal(err)
	}
	h.relayJobCompletionFrom("host-a", t3426Completion(job, t3426Design, ""))

	if items := t4270Items(t, h, t3426Design); len(items) != 0 {
		t.Fatalf("never: the owner heard a reviewer completion: %+v", items)
	}
	rows, deliveredTo := t4270Rows(fake, job)
	if len(rows) == 0 || !strings.HasPrefix(deliveredTo[0], "hub/filtered:owner-notify") {
		t.Fatalf("hk rows=%d delivered_to=%v, want the completion filed as filtered:owner-notify", len(rows), deliveredTo)
	}
	var counted int64
	for _, row := range h.loopsRowDocument(loopDateOf(now), now).SpawnNotices {
		if row.Lane == t3426Design && row.Cause == "job_completed" {
			counted += row.Silenced
		}
	}
	if counted != 1 {
		t.Fatalf("job_completed silenced for %s=%d, want the withheld completion counted once", t3426Design, counted)
	}
}

// The must-fix list accepts the documented cause format: a refusal code,
// optionally with its detail after a colon (checkout_refused:branch-elsewhere).
func TestSpawnNoticeMustFixAcceptsDocumentedCauseFormat_5360(t *testing.T) {
	policy, err := parseDeliveryPolicy([]byte("default: interrupt\nrelay:\n  spawn_notice_must_fix: [checkout_refused:branch-elsewhere, workspace_unresolved]\n"))
	if err != nil {
		t.Fatalf("documented cause refused at load: %v", err)
	}
	if !policy.spawnNoticeMustFix("checkout_refused:branch-elsewhere") || !policy.spawnNoticeMustFix("workspace_unresolved") {
		t.Fatalf("configured causes not honoured: %v", policy.Relay.SpawnNoticeMustFix)
	}
	for _, bad := range []string{"Bad Cause", "a:b:c", "checkout_refused:"} {
		if _, err := parseDeliveryPolicy([]byte("default: interrupt\nrelay:\n  spawn_notice_must_fix: [" + bad + "]\n")); err == nil {
			t.Fatalf("malformed cause %q accepted", bad)
		}
	}
}

// A design lane that is the requester of a failure gets its own copy, which is
// what it replays from, even when the default cause rules would count it. Only
// the owner's copy is subject to those rules (mergewake review of PR 732).
func TestDesignRequesterCopyOfRetryableFailureIsDelivered_5360(t *testing.T) {
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h, _, _ := spawnNotifyHubLanes(t, "", "mergewake-design", "pw-5360-worker")
	h.now = func() time.Time { return now }
	const job = "pw-5360-design-requester"
	req := t4270Request(job, "pw-5360-worker", "mergewake-design", "implementer")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, "pw-5360-worker", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.inbox.store.completeSpawn(t.Context(), task, s5360Retryable("checkout_refused:locked"), now); err != nil {
		t.Fatal(err)
	}
	s5360Flush(t, h)
	items := t4270Items(t, h, "mergewake-design")
	if len(items) != 1 || !strings.Contains(items[0].Event.Text, "checkout_refused:locked") {
		t.Fatalf("design requester items=%+v, want its own copy of the failure it replays", items)
	}
}

// A retryable owner row that a later outcome overtook is settled without a
// notice or a count, even though the request now reads given_up: the owner hears
// the given-up notice, once, and not the stale retryable failure beside it
// (mergewake review of PR 732). Runs under the default rules and under final.
func TestStaleRetryableOwnerRowIsSettledAfterGiveUp_5360(t *testing.T) {
	for _, extra := range [][]string{nil, {fleet.OwnerNotifyArg, fleet.OwnerNotifyFinal}} {
		t.Run(fmt.Sprintf("args=%v", extra), func(t *testing.T) {
			now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
			h, destination := s5360Hub(t, &now)
			const job = "pw-5360-stale-retryable"
			req := t4270Request(job, s5360Owner, s5360Requester, "implementer")
			req.Args = append(req.Args, extra...)
			task, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, s5360Owner, now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = h.inbox.store.completeSpawn(t.Context(), task, s5360Retryable("checkout_refused:locked"), now); err != nil {
				t.Fatal(err)
			}
			// The request's limit passes before the outbox routes the retryable
			// rows: the attempt ends given_up, and its given-up notice is queued.
			s5360GiveUp(t, h, task, now)

			s5360Flush(t, h)
			items := t4270Items(t, h, s5360Owner)
			if len(items) != 1 || !strings.Contains(items[0].Event.Text, "replay=no") || !strings.Contains(items[0].Event.Text, "expired") {
				t.Fatalf("owner items=%+v, want exactly the given-up notice", items)
			}
			if got := spawnFailedInjections(destination); len(got) == 0 {
				t.Fatal("the requester got no copy of its failure")
			}
			if n := s5360OwnerSilenced(t, h, now); n != 0 {
				t.Fatalf("a superseded row was counted (%d)", n)
			}
			var open int
			if err := h.inbox.store.db.QueryRow(`SELECT COUNT(*) FROM spawn_notifications WHERE delivered<>1`).Scan(&open); err != nil || open != 0 {
				t.Fatalf("outbox rows still pending=%d err=%v", open, err)
			}
		})
	}
}

// Two completions of one job under --owner-notify never are two events: each
// has its own hk row, and each is counted once. A second completion must not
// reuse the first one's row, which hk's idempotency key would otherwise allow
// (mergewake review of PR 732).
func TestOwnerNotifyEachCompletionHasItsOwnRecord_5360(t *testing.T) {
	h, _, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 19, 3, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return now }
	const job = "pw-5360-two-completions"
	req := t4270Request(job, t3426Design, "", "implementer")
	req.Args = append(req.Args, fleet.OwnerNotifyArg, fleet.OwnerNotifyNever)
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), req, job, t3426Design, now); err != nil {
		t.Fatal(err)
	}
	first := t3426Completion(job, t3426Design, "")
	second := t3426Completion(job, t3426Design, "")
	second.EventID = "00002-job.completed.json"
	h.relayJobCompletionFrom("host-a", first)
	h.relayJobCompletionFrom("host-a", second)

	rows, deliveredTo := t4270Rows(fake, job)
	if len(rows) != 2 {
		t.Fatalf("hk rows=%d delivered_to=%v, want one record per completion", len(rows), deliveredTo)
	}
	for _, to := range deliveredTo {
		if !strings.HasPrefix(to, "hub/filtered:owner-notify") {
			t.Fatalf("delivered_to=%q, want owner-notify", to)
		}
	}
	var counted int64
	for _, row := range h.loopsRowDocument(loopDateOf(now), now).SpawnNotices {
		if row.Lane == t3426Design && row.Cause == "job_completed" {
			counted += row.Silenced
		}
	}
	if counted != 2 {
		t.Fatalf("job_completed silenced=%d, want 2", counted)
	}
}
