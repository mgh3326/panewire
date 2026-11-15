package panewire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// suppressedWorkerRig is an idle-wake rig whose pane belongs to a local worker
// job, with a quota manager that reads a devin limit screen and counts reads.
func suppressedWorkerRig(t *testing.T) (rig *idleWakeTestRig, reads *int) {
	t.Helper()
	rig = newIdleWakeTestRig(t, time.Second)
	root := rig.manager.inboxRoot
	events := filepath.Join(root, "jobs", "job-suppressed", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	for name, body := range map[string]string{
		"00001-job.claimed.json": `{"type":"job.claimed","created_at":"` + stamp + `","agent_label":"worker-synthetic","owner_lane":"parkingarcade-design","epoch":1}`,
		"00002-job.spawned.json": `{"type":"job.spawned","created_at":"` + stamp + `","pane_id":"workspace:pane-1"}`} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	q, err := newQuotaStopManager(rig.store, root, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	rig.manager.quota = q
	reads = new(int)
	q.enqueue = func(hubScannedRelayEvent) bool { return true }
	screen, err := os.ReadFile("testdata/quota-stop/devin.txt")
	if err != nil {
		t.Fatal(err)
	}
	q.read = func(context.Context, string) (string, error) { *reads++; return string(screen), nil }
	policy := `## Values
- default_expected: 1d
- urgent_priorities: [1, 2]
- max_waits: 1
- resume_text: "continue"
`
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), `{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"job-suppressed"}}]}`, new(int))
	q.send = func(_ context.Context, pane, pool, text string) relayInjectResult {
		if pane != "workspace:pane-1" || !strings.HasPrefix(text, "continue") {
			t.Fatalf("send %s %s", pane, text)
		}
		return relayInjectResult{Outcome: relayInjectDelivered, Evidence: "visible:nonce_echo", Proven: relayNoncesIn(text)}
	}

	return rig, reads
}

// Suppression is about waking, not reading (hk 4507). A worker whose idle-wake
// candidate the route suppressed, for any reason, still has its quota stop
// read once per idle episode: hk 4233 found design-owned workers unobserved,
// and hk 4507 the same for ambiguous_owner / parentless_lane ones.
func TestQuotaStopObservedForSuppressedCandidate(t *testing.T) {
	for _, reason := range []string{idleWakeReasonDesignLane, idleWakeReasonAmbiguous, idleWakeReasonParentless, idleWakeReasonUnknown, idleWakeReasonInvalid} {
		t.Run(reason, func(t *testing.T) {
			rig, reads := suppressedWorkerRig(t)
			rig.observe("working", 1, rig.now)
			rig.observe("idle", 2, rig.now.Add(time.Second))
			rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
			if len(rig.requests) != 1 {
				t.Fatalf("settled candidates = %d, want 1", len(rig.requests))
			}
			route := hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: rig.requests[0].EventID, Pane: rig.requests[0].Pane, Eligible: false, Reason: reason}
			rig.manager.ApplyRoute(context.Background(), route)
			if *reads != 1 {
				t.Fatalf("quota reads after %s suppression = %d, want 1", reason, *reads)
			}
			rig.manager.ApplyRoute(context.Background(), route)
			rig.manager.Tick(context.Background(), rig.now.Add(10*time.Second))
			if *reads != 1 {
				t.Fatalf("quota reads after repeat route and tick = %d, want 1 (one read per idle episode)", *reads)
			}
		})
	}
}

// A suppressed candidate whose pane went back to work before its route was
// decided is not read: the scan is of the panes idle now.
func TestQuotaStopSuppressedScanSkipsPaneThatMovedOn(t *testing.T) {
	rig, reads := suppressedWorkerRig(t)
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	if len(rig.requests) != 1 {
		t.Fatalf("settled candidates = %d, want 1", len(rig.requests))
	}
	rig.observe("working", 3, rig.now.Add(6*time.Second))
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: rig.requests[0].EventID, Pane: rig.requests[0].Pane, Eligible: false, Reason: idleWakeReasonAmbiguous})
	if *reads != 0 {
		t.Fatalf("a pane that went back to work was read %d times", *reads)
	}
}

// hk 4507: the pane showed the limit above an injected directive and an empty
// prompt, which the classifier read as no stop. The stop is now found there and
// records the host's free limit, with the reset the limit line gives (13 hours
// 9 minutes), so the next devin measurement publishes it.
func TestQuotaStopRecordsFreeLimitShownAboveInjectedDirective(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	t.Setenv("PANEWIRE_CACHE", "")
	rig, reads := suppressedWorkerRig(t)
	rig.manager.quota.read = func(context.Context, string) (string, error) {
		*reads++
		return "❭ [pw-x] Read the brief and review the PR\n\n" +
			"▲ Something went wrong — Reached free model rate limit … Your limit will reset in 13 hours 9 minutes (at 22:01 UTC). Send a message to retry\n\n" +
			"❭ [relay] owner: please report status\n\n" +
			"──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n", nil
	}
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	if len(rig.requests) != 1 {
		t.Fatalf("settled candidates = %d, want 1", len(rig.requests))
	}
	at := rig.now
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: rig.requests[0].EventID, Pane: rig.requests[0].Pane, Eligible: false, Reason: idleWakeReasonAmbiguous})
	if *reads != 1 {
		t.Fatalf("screen reads = %d, want 1", *reads)
	}
	got := devin.LocalFree(devin.Config{Now: func() time.Time { return at }})(at)
	want := at.Add(13*time.Hour + 9*time.Minute)
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(want) {
		t.Fatalf("free limit after the observation = %+v, want exhausted until %v", got, want)
	}
}
