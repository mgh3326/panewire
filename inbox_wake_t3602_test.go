package panewire

// hk 3602: design and desk lanes are inbox-only. A notification to one is
// filed unread and never typed into the pane; the hub has the lane's node type
// one coalesced line instead. Worker lanes keep body injection, except a lane
// in the time-boxed relay.worker_inbox_trial.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const t3602Lanes = `{"lanes":{
 "handoffkeep-design":{"machine":"host-a","pane":"w1:design"},
 "operator-desk":{"machine":"host-a","pane":"w1:desk"},
 "work-operator-desk":{"machine":"host-a","pane":"w1:workdesk"},
 "worker-3404":{"machine":"host-a","pane":"w1:worker","parent":"handoffkeep-design"},
 "review-3404":{"machine":"host-a","pane":"w1:review","parent":"handoffkeep-design"},
 "trial-3404":{"machine":"host-a","pane":"w1:trial","parent":"handoffkeep-design"}
}}`

const (
	t3602Design = "handoffkeep-design"
	t3602Start  = "2026-11-08T12:00:00Z"
	t3602Until  = "2026-11-15T12:00:00Z"
)

func TestInboxWakeOrderKeepsDisplayKindAndDecisionUrgency(t *testing.T) {
	e := hubJobEventPayload{NotifyType: "order"}
	if got := inboxWakeKind("", e); got != "order" {
		t.Fatalf("inboxWakeKind(order) = %q, want order", got)
	}
	if got, want := inboxWakeRank("order"), inboxWakeRank("decision"); got != want {
		t.Fatalf("order rank = %d, decision rank = %d", got, want)
	}
	items := []laneInboxItem{{Kind: "notify", Event: e}}
	if got := inboxWakeMostUrgent(items); got != "order" {
		t.Fatalf("most urgent = %q, want order", got)
	}
	if got, want := inboxWakeText("panewire-design", 1, inboxWakeMostUrgent(items)), "[inbox] panewire-design: 1 unread (most urgent: order) | panewire inbox list --lane panewire-design"; got != want {
		t.Fatalf("wake line = %q, want %q", got, want)
	}
}

type t3602Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *t3602Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *t3602Clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type t3602Rig struct {
	h      *HubServer
	agent  *hubAgent
	fake   *fakeHandoffkeep
	clock  *t3602Clock
	mu     sync.Mutex
	events []fleetEvent
}

// t3602TrialPolicy is the shipped policy with the worker trial turned on for
// trial-* lanes until t3602Until.
func t3602TrialPolicy(t *testing.T) string {
	t.Helper()
	raw := strings.Replace(string(defaultDeliveryPolicy), "relay:\n  design_lanes: [\"*-design\"]\n",
		"relay:\n  design_lanes: [\"*-design\"]\n  worker_inbox_trial: [\"trial-*\"]\n  worker_inbox_trial_until: \""+t3602Until+"\"\n", 1)
	if raw == string(defaultDeliveryPolicy) {
		t.Fatal("shipped policy has no relay block to extend")
	}
	path := filepath.Join(t.TempDir(), "delivery.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// t3602Hub runs policy (a path, or "default") on one connected node whose
// every pane is an idle claude session, with an injectable clock and a fleet
// event recorder.
func t3602Hub(t *testing.T, path, policy string, fake *fakeHandoffkeep, hk *handoffkeepRelayClient, clock *t3602Clock) *t3602Rig {
	t.Helper()
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, t3602Lanes),
		DeliveryPolicyPath: policy,
		InboxStorePath:     path,
		handoffkeep:        hk,
		Now:                clock.Now,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	rig := &t3602Rig{h: h, fake: fake, clock: clock}
	h.fleetEvents = func(_ time.Time, ev fleetEvent) bool {
		rig.mu.Lock()
		defer rig.mu.Unlock()
		rig.events = append(rig.events, ev)
		return true
	}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: t3426Records, Out: out, Err: errout}, nil
	}
	rig.agent = &hubAgent{machineID: "host-a", relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	var sessions []HubSession
	for _, pane := range []string{"w1:design", "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial"} {
		sessions = append(sessions, HubSession{PaneID: pane, Status: "idle", Harness: "claude"})
	}
	h.mu.Lock()
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: rig.agent, sessionSnapshot: &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: clock.Now()}}
	h.mu.Unlock()
	return rig
}

func newT3602Rig(t *testing.T, policy string) *t3602Rig {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	start, _ := time.Parse(time.RFC3339, t3602Start)
	return t3602Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policy, fake, hk, &t3602Clock{now: start})
}

// run starts the hub's inbox loop, which owns the window-end wake, until the
// test ends or stop is called.
func (r *t3602Rig) run(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.h.runInbox(ctx) }()
	stop = func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

func (r *t3602Rig) wakeEvents() []fleetEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []fleetEvent
	for _, ev := range r.events {
		if ev.Kind == "inbox_wake" {
			out = append(out, ev)
		}
	}
	return out
}

var t3602Seq int

func t3602Event(lane, notifyType, text string) hubJobEventPayload {
	t3602Seq++
	id := "t3602-" + strconv.Itoa(t3602Seq)
	return hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, NotifyType: notifyType, Text: text}
}

func t3602Wake(lane string, unread int, most string) string {
	return "[inbox] " + lane + ": " + strconv.Itoa(unread) + " unread (most urgent: " + most + ") | panewire inbox list --lane " + lane
}

// fileQuiet relays e from sender (nil: a hub notice) and fails unless it was
// filed as an unread inbox item without a body injection; it returns the
// injections the relay produced, which may only be wake lines.
func (r *t3602Rig) fileQuiet(t *testing.T, e hubJobEventPayload, sender *hubAgent) []hubRelayInjectEvent {
	t.Helper()
	if res := r.h.relayLaneEvent(e, sender); res.ID == 0 || res.PersistFailed || res.Machine != "inbox" {
		t.Fatalf("%q relay=%+v, want an inbox item", e.Text, res)
	}
	got := t3426Injected(r.agent)
	for _, injected := range got {
		if !strings.HasPrefix(injected.Text, "[inbox] ") || injected.EventID != 0 || strings.Contains(injected.Text, e.Text) {
			t.Fatalf("%q (type %q) was typed into the pane: %+v", e.Text, e.NotifyType, injected)
		}
	}
	item := t3426Item(t, r.h, e.OwnerLane, "lane.event", e.JobID)
	if item.Class != "inbox" || item.ReadAt != 0 || item.AckedAt != 0 || item.DueAt != 0 || item.DigestAt != 0 || item.HoldUntil != 0 {
		t.Fatalf("%q stored as %+v, want an unread inbox item with no deadline, digest or hold", e.Text, item)
	}
	return got
}

func t3602OneWake(t *testing.T, got []hubRelayInjectEvent, pane, text string) {
	t.Helper()
	if len(got) != 1 || got[0].Pane != pane || got[0].Text != text || got[0].EventID != 0 {
		t.Fatalf("injections=%+v, want exactly one wake %q in %s", got, text, pane)
	}
}

// (1) An interrupt-class lane.event -- an ESC --type decision from a session
// that is not a worker -- to a design lane is not typed into the pane: it is
// filed unread, and one wake line is typed at once.
func TestInboxWakeDesignLaneDecisionIsFiledAndWakesOnce(t *testing.T) {
	r := newT3602Rig(t, "default")
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?"), r.agent)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "decision"))
	if ev := r.wakeEvents(); len(ev) != 1 || ev[0].Outcome != "sent" || ev[0].Job != t3602Design || ev[0].Reason != "design" {
		t.Fatalf("inbox_wake fleet events=%+v, want one sent for the design lane", ev)
	}
}

// (2) Three more items inside the window add no line until the window ends;
// then exactly one line counts all four and names the most urgent.
func TestInboxWakeCoalescesArrivalsUntilTheWindowEnds(t *testing.T) {
	r := newT3602Rig(t, "default")
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?"), r.agent)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "decision"))
	stop := r.run(t)
	for _, e := range []hubJobEventPayload{
		t3602Event(t3602Design, "blocked", "BLOCKED hk 3602: CI needs an operator secret"),
		t3602Event(t3602Design, "findings", "FINDINGS hk 3602: two review findings on #400"),
		t3602Event(t3602Design, "automation-failed", "[panewire] SPAWN_FAILED job-9 request=r9 host=none profile=none rc=70 stage=launch replay=no reason=spawn_failed fix=Check the checkout."),
	} {
		r.clock.advance(time.Minute)
		sender := r.agent
		if e.NotifyType == "automation-failed" {
			sender = nil
		}
		if got := r.fileQuiet(t, e, sender); len(got) != 0 {
			t.Fatalf("an arrival inside the window typed %+v", got)
		}
	}
	r.clock.advance(2*time.Minute + time.Second)
	want := t3602Wake(t3602Design, 4, "decision")
	var wakes []hubRelayInjectEvent
	eventually(t, 5*time.Second, "the window-end wake", func() bool {
		wakes = append(wakes, t3426Injected(r.agent)...)
		return len(wakes) > 0
	})
	stop()
	wakes = append(wakes, t3426Injected(r.agent)...)
	t3602OneWake(t, wakes, "w1:design", want)
}

// (3) Pure info (MERGED, SPAWNED records) is filed and counts, but never wakes
// by itself.
func TestInboxWakeInfoOnlyDoesNotWake(t *testing.T) {
	r := newT3602Rig(t, "default")
	for _, tc := range []struct {
		e      hubJobEventPayload
		sender *hubAgent
	}{
		{t3602Event(t3602Design, "merged", "[mergewake] MERGED #400 hk 3602 into main"), r.agent},
		{t3602Event(t3602Design, "report", "[panewire] SPAWNED wk-3602 host=host-a profile=codex pane=w1:p9 model=gpt"), nil},
	} {
		if got := r.fileQuiet(t, tc.e, tc.sender); len(got) != 0 {
			t.Fatalf("info %q typed %+v", tc.e.Text, got)
		}
	}
	if ev := r.wakeEvents(); len(ev) != 0 {
		t.Fatalf("info-only arrivals recorded wakes %+v", ev)
	}
	// The info items count toward N of the next real wake.
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: ship it?"), r.agent)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 3, "decision"))
}

// (4) A desk lane -- operator-desk and the per-fleet work-operator-desk -- gets
// the same rule.
func TestInboxWakeDeskLaneIsInboxOnly(t *testing.T) {
	r := newT3602Rig(t, "default")
	for _, tc := range []struct{ lane, pane string }{{"operator-desk", "w1:desk"}, {"work-operator-desk", "w1:workdesk"}} {
		got := r.fileQuiet(t, t3602Event(tc.lane, "decision", "Decision required: choose the release"), nil)
		t3602OneWake(t, got, tc.pane, t3602Wake(tc.lane, 1, "decision"))
	}
	if ev := r.wakeEvents(); len(ev) != 2 || ev[0].Reason != "desk" || ev[1].Reason != "desk" {
		t.Fatalf("inbox_wake fleet events=%+v, want two desk wakes", ev)
	}
}

// (5) Worker job lanes, reviewer lanes included, keep today's body injection
// for a READY, review findings, an ESC answer and an operator chat turn.
func TestInboxWakeWorkerLanesKeepBodyInjection(t *testing.T) {
	for _, lane := range []struct{ name, pane string }{{"worker-3404", "w1:worker"}, {"review-3404", "w1:review"}, {"trial-3404", "w1:trial"}} {
		for _, tc := range []struct{ name, notifyType, eventID, text string }{
			{"ready", "ready", "", "READY hk3404 PR #12 head abc1234"},
			{"findings", "findings", "", "FINDINGS #12: 2 blocking"},
			{"esc-answer", "decision", "", "ESC answer: ship the hub first"},
			{"chat", "", "chat-77", "operator: please rebase on main"},
		} {
			t.Run(lane.name+"/"+tc.name, func(t *testing.T) {
				r := newT3602Rig(t, "default")
				e := t3602Event(lane.name, tc.notifyType, tc.text)
				if tc.eventID != "" {
					e.EventID, e.JobID = tc.eventID, laneEventTransportID(lane.name, tc.eventID)
				}
				if res := r.h.relayLaneEvent(e, nil); res.PersistFailed || res.Machine == "inbox" {
					t.Fatalf("relay=%+v, want a pane delivery", res)
				}
				got := t3426Injected(r.agent)
				if len(got) != 1 || got[0].Pane != lane.pane || got[0].EventID == 0 || !strings.Contains(got[0].Text, tc.text) {
					t.Fatalf("injections=%+v, want the body in %s", got, lane.pane)
				}
				if ev := r.wakeEvents(); len(ev) != 0 {
					t.Fatalf("a worker lane recorded wakes %+v", ev)
				}
			})
		}
	}
}

// (6) A worker lane in relay.worker_inbox_trial is inbox-only with the wake
// until relay.worker_inbox_trial_until; after it, body injection returns. A
// worker lane outside the trial is unchanged throughout.
func TestInboxWakeWorkerTrialUntilExpiry(t *testing.T) {
	r := newT3602Rig(t, t3602TrialPolicy(t))
	got := r.fileQuiet(t, t3602Event("trial-3404", "findings", "FINDINGS #12: 2 blocking"), nil)
	t3602OneWake(t, got, "w1:trial", t3602Wake("trial-3404", 1, "findings"))
	if ev := r.wakeEvents(); len(ev) != 1 || ev[0].Reason != "trial" || ev[0].Outcome != "sent" {
		t.Fatalf("inbox_wake fleet events=%+v, want one trial wake", ev)
	}
	other := t3602Event("worker-3404", "findings", "FINDINGS #13: 1 blocking")
	if res := r.h.relayLaneEvent(other, nil); res.Machine == "inbox" {
		t.Fatalf("a worker lane outside the trial was filed: %+v", res)
	}
	if got := t3426Injected(r.agent); len(got) != 1 || got[0].Pane != "w1:worker" || !strings.Contains(got[0].Text, other.Text) {
		t.Fatalf("worker lane outside the trial injections=%+v", got)
	}

	until, _ := time.Parse(time.RFC3339, t3602Until)
	r.clock.advance(until.Sub(r.clock.Now()) + time.Second)
	after := t3602Event("trial-3404", "findings", "FINDINGS #12 round 2: 1 blocking")
	if res := r.h.relayLaneEvent(after, nil); res.PersistFailed || res.Machine == "inbox" {
		t.Fatalf("after the trial relay=%+v, want a pane delivery", res)
	}
	if got := t3426Injected(r.agent); len(got) != 1 || got[0].Pane != "w1:trial" || got[0].EventID == 0 || !strings.Contains(got[0].Text, after.Text) {
		t.Fatalf("after the trial injections=%+v, want the body in w1:trial", got)
	}
}

// The trial and window keys parse and refuse as documented: a trial needs
// both its lane patterns and its RFC3339 end, and the window is 10s..1h.
func TestInboxWakeConfig(t *testing.T) {
	base := "default: interrupt\n"
	for _, good := range []string{
		"relay:\n  inbox_wake_window: 2m\n  desk_lanes: []\n",
		"relay:\n  desk_lanes: [\"operator-desk\", \"*-operator-desk\"]\n  worker_inbox_trial: [\"trial-*\"]\n  worker_inbox_trial_until: \"" + t3602Until + "\"\n",
	} {
		if _, err := parseDeliveryPolicy([]byte(base + good)); err != nil {
			t.Fatalf("policy %q refused: %v", good, err)
		}
	}
	for _, bad := range []string{
		"relay:\n  worker_inbox_trial: [\"trial-*\"]\n",
		"relay:\n  worker_inbox_trial_until: \"" + t3602Until + "\"\n",
		"relay:\n  worker_inbox_trial: [\"trial-*\"]\n  worker_inbox_trial_until: \"next week\"\n",
		"relay:\n  inbox_wake_window: 1s\n",
		"relay:\n  desk_lanes: [\"[\"]\n",
	} {
		if _, err := parseDeliveryPolicy([]byte(base + bad)); err == nil {
			t.Fatalf("policy %q was accepted", bad)
		}
	}
}

// (7) A failed unread-count read never becomes "0 unread": the window-end wake
// is skipped and recorded, and sent with the real count once the read works.
func TestInboxWakeCountFailureNeverShowsZero(t *testing.T) {
	r := newT3602Rig(t, "default")
	got := r.fileQuiet(t, t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?"), r.agent)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "decision"))
	r.clock.advance(time.Minute)
	if got := r.fileQuiet(t, t3602Event(t3602Design, "blocked", "BLOCKED hk 3602: CI needs an operator secret"), r.agent); len(got) != 0 {
		t.Fatalf("an arrival inside the window typed %+v", got)
	}
	if _, err := r.h.inbox.store.db.Exec(`ALTER TABLE lane_inbox RENAME TO lane_inbox_unreadable`); err != nil {
		t.Fatal(err)
	}
	r.clock.advance(5 * time.Minute)
	stop := r.run(t)
	eventually(t, 5*time.Second, "a skipped count_failed wake", func() bool {
		for _, ev := range r.wakeEvents() {
			if ev.Outcome == "skipped" && ev.Reason == "count_failed" {
				return true
			}
		}
		return false
	})
	stop()
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("a failed count read typed %+v", got)
	}
	if _, err := r.h.inbox.store.db.Exec(`ALTER TABLE lane_inbox_unreadable RENAME TO lane_inbox`); err != nil {
		t.Fatal(err)
	}
	r.clock.advance(5 * time.Minute)
	r.run(t)
	var wakes []hubRelayInjectEvent
	eventually(t, 5*time.Second, "the retried wake", func() bool {
		wakes = append(wakes, t3426Injected(r.agent)...)
		return len(wakes) > 0
	})
	t3602OneWake(t, wakes, "w1:design", t3602Wake(t3602Design, 2, "decision"))
}

// (8) A hub restart replays no injection into a design lane: an item filed
// before it is not filed or announced again, and an undelivered row an older
// hub persisted is filed rather than typed.
func TestInboxWakeHubRestartReplaysNoInjection(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	start, _ := time.Parse(time.RFC3339, t3602Start)
	clock := &t3602Clock{now: start}
	r := t3602Hub(t, path, "default", fake, hk, clock)
	first := t3602Event(t3602Design, "decision", "ESC hk 3602: which order do we ship?")
	t3602OneWake(t, r.fileQuiet(t, first, r.agent), "w1:design", t3602Wake(t3602Design, 1, "decision"))
	r.h.Close()

	old := t3602Event(t3602Design, "", "ESC hk 3602: the older hub never delivered this")
	fake.seedUndelivered(handoffkeepRelayEvent{ID: 701, Kind: "lane.event", JobID: old.JobID, Epoch: 1, OwnerLane: t3602Design, EventID: old.EventID, Text: old.Text, ReceivedAt: start.Format(time.RFC3339Nano)})
	clock.advance(10 * time.Minute)
	r2 := t3602Hub(t, path, "default", fake, hk, clock)
	r2.h.replayUndeliveredRelayEvents(context.Background())
	r2.h.replayUndeliveredLaneEvents(context.Background())
	r2.h.flushInbox()
	if err := r2.h.inbox.store.makeBatches(clock.Now().Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	r2.h.flushInbox()
	if resent := r2.h.relayLaneEvent(first, r2.agent); resent.Machine != "inbox" {
		t.Fatalf("a resend after the restart relay=%+v", resent)
	}
	got := t3426Injected(r2.agent)
	for _, injected := range got {
		if injected.EventID != 0 || !strings.HasPrefix(injected.Text, "[inbox] ") {
			t.Fatalf("the restart typed a body into the design lane: %+v", injected)
		}
	}
	if len(got) > 1 {
		t.Fatalf("the restart typed %d wake lines: %+v", len(got), got)
	}
	item := t3426Item(t, r2.h, t3602Design, "lane.event", old.JobID)
	if item.Class != "inbox" || item.ReadAt != 0 {
		t.Fatalf("the replayed row was stored as %+v, want an unread inbox item", item)
	}
}

// t3602AssertWoken fails unless got is exactly one wake line for lane in pane
// whose most urgent kind is most, and the lane's inbox holds the kind/jobID
// item unread. It then acks the item and closes the lane's wake window, so
// the next event is judged on its own: its wake names its own urgency. The
// hk 3410, 3426 and 3434 tests use it where a design lane used to be
// interrupted.
func t3602AssertWoken(t *testing.T, h *HubServer, got []hubRelayInjectEvent, lane, pane, kind, jobID, most string) {
	t.Helper()
	if len(got) != 1 || got[0].Pane != pane || got[0].EventID != 0 || !strings.HasPrefix(got[0].Text, "[inbox] "+lane+": ") || !strings.HasSuffix(got[0].Text, " unread (most urgent: "+most+") | panewire inbox list --lane "+lane) {
		t.Fatalf("injections=%+v, want one wake line for %s in %s, most urgent %s", got, lane, pane, most)
	}
	item := t3426Item(t, h, lane, kind, jobID)
	if item.Class != "inbox" || item.ReadAt != 0 || item.AckedAt != 0 {
		t.Fatalf("%s %s stored as %+v, want an unread inbox item", kind, jobID, item)
	}
	if _, err := h.inbox.store.ack(item.ID, lane, false, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	h.inbox.wakes.mu.Lock()
	delete(h.inbox.wakes.lanes, lane)
	h.inbox.wakes.mu.Unlock()
}
