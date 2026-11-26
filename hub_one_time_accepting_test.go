package panewire

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func setAcceptingOverrideForTest(h *HubServer, machine, mode string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.relayControl.acceptingOverride == nil {
		h.relayControl.acceptingOverride = map[string]acceptingOverride{}
	}
	h.relayControl.acceptingOverride[machine] = acceptingOverride{Mode: mode}
}

func TestOneTimeLoginNeedWaitsForAcceptingHost(t *testing.T) {
	h, fake, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 27, 3, 0, 0, 0, time.UTC)
	key := oneTimeActionKey{machine: "oci", kind: "claude", need: oneTimeLoginMissing}
	missing := func(at time.Time) *fleet.Readiness {
		return readinessWith(at, "claude", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing})
	}

	setAcceptingOverrideForTest(h, "oci", "off")
	h.noteReadinessActions("oci", missing(start), start)
	if got := oneTimeMessages(t, h, transport); len(got) != 0 {
		t.Errorf("login notice while host is not accepting: %q", got)
	}
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Errorf("operator waits while host is not accepting: %+v", got)
	}
	h.loginNeedMu.Lock()
	gotNeeds := append([]string(nil), h.loginNeeds["oci"]...)
	h.loginNeedMu.Unlock()
	if len(gotNeeds) != 1 || gotNeeds[0] != "claude" {
		t.Errorf("placement need while host is not accepting = %v, want [claude]", gotNeeds)
	}
	if state, ok := h.oneTimeStateOf(key); !ok || !state.ToldAt.IsZero() {
		t.Errorf("off-host need state = %+v, present=%t; want recorded and untold", state, ok)
	}

	setAcceptingOverrideForTest(h, "oci", "on")
	h.noteReadinessActions("oci", missing(start.Add(time.Minute)), start.Add(time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 || got[0] != "ACTION NEEDED: oci claude — log in" {
		t.Fatalf("standing need after host becomes accepting = %q", got)
	}
	if got := fake.openOperatorWaits(); len(got) != 1 {
		t.Fatalf("operator waits after host becomes accepting = %+v, want one", got)
	}

	setAcceptingOverrideForTest(h, "oci", "off")
	h.noteReadinessActions("oci", missing(start.Add(2*time.Minute)), start.Add(2*time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("disabling an already-told host started another episode: %q", got)
	}
	if got := fake.openOperatorWaits(); len(got) != 0 {
		t.Fatalf("operator wait remained open while host is not accepting: %+v", got)
	}
	setAcceptingOverrideForTest(h, "oci", "on")
	h.noteReadinessActions("oci", missing(start.Add(3*time.Minute)), start.Add(3*time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("re-enabling an uncleared need repeated its notice: %q", got)
	}
}

// A readiness report computed before a newer login-missing sighting cannot
// start or complete the clear countdown, even if it reaches the worker later.
func TestOneTimeLoginNeedIgnoresCleanReportOlderThanLatestSighting(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 27, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	readiness := func(at time.Time, login string) *fleet.Readiness {
		return readinessWith(at, "claude", fleet.HarnessReadiness{Installed: true, Login: login})
	}

	h.noteReadinessActions("oci", readiness(start, fleet.ReadinessLoginMissing), start)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("initial notice count = %d, want one", len(got))
	}
	newerMissing := start.Add(3 * cycle)
	h.noteReadinessActions("oci", readiness(newerMissing, fleet.ReadinessLoginMissing), newerMissing)
	// This clean report predates the latest missing report, but arrives after it.
	staleClean := start.Add(2 * cycle)
	h.noteReadinessActions("oci", readiness(staleClean, fleet.ReadinessLoginPresent), newerMissing.Add(time.Second))
	h.noteReadinessActions("oci", readiness(staleClean.Add(cycle), fleet.ReadinessLoginPresent), staleClean.Add(cycle))
	h.noteReadinessActions("oci", readiness(newerMissing.Add(time.Minute), fleet.ReadinessLoginMissing), newerMissing.Add(time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("stale clean reports restarted the standing episode: %q", got)
	}
}

func TestOneTimeLoginSightedAtSurvivesHubRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first := newOneTimeHub(t, path)
	firstTransport := oneTimeSentry(t, first)
	start := time.Date(2026, 11, 27, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	key := oneTimeActionKey{machine: "oci", kind: "claude", need: oneTimeLoginMissing}
	readiness := func(at time.Time, login string) *fleet.Readiness {
		return readinessWith(at, "claude", fleet.HarnessReadiness{Installed: true, Login: login})
	}
	first.noteReadinessActions("oci", readiness(start, fleet.ReadinessLoginMissing), start)
	if got := oneTimeMessages(t, first, firstTransport); len(got) != 1 {
		t.Fatalf("initial notice count = %d, want one", len(got))
	}
	newerMissing := start.Add(3 * cycle)
	first.noteReadinessActions("oci", readiness(newerMissing, fleet.ReadinessLoginMissing), newerMissing)
	settleOneTimeNotices(t, first)
	first.Close()

	second := newOneTimeHub(t, path)
	secondTransport := oneTimeSentry(t, second)
	staleClean := start.Add(2 * cycle)
	second.noteReadinessActions("oci", readiness(staleClean, fleet.ReadinessLoginPresent), newerMissing.Add(time.Second))
	second.noteReadinessActions("oci", readiness(staleClean.Add(cycle), fleet.ReadinessLoginPresent), staleClean.Add(cycle))
	second.noteReadinessActions("oci", readiness(newerMissing.Add(time.Minute), fleet.ReadinessLoginMissing), newerMissing.Add(time.Minute))
	if got := oneTimeMessages(t, second, secondTransport); len(got) != 0 {
		t.Fatalf("a stale clean report after restart retold the standing login need: %q", got)
	}
	if state, ok := second.oneTimeStateOf(key); !ok || !state.ConfirmedAt.IsZero() {
		t.Fatalf("state after stale reports = %+v, present=%t; want unconfirmed standing need", state, ok)
	}
}
