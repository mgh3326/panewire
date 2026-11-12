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
)

// A worker whose owner lane is a design lane gets its idle-wake candidate
// suppressed as design_lane, so it never reaches the assigned path. Its quota
// stop must still be read once (hk 4233: the stop went unobserved for hours).
func TestQuotaStopObservedForDesignLaneCandidate(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	root := rig.manager.inboxRoot
	events := filepath.Join(root, "jobs", "job-design", "events")
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
	reads := 0
	q.enqueue = func(hubScannedRelayEvent) bool { return true }
	screen, err := os.ReadFile("testdata/quota-stop/devin.txt")
	if err != nil {
		t.Fatal(err)
	}
	q.read = func(context.Context, string) (string, error) { reads++; return string(screen), nil }
	policy := `## Values
- default_expected: 1d
- urgent_priorities: [1, 2]
- max_waits: 1
- resume_text: "continue"
`
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), `{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"job-design"}}]}`, new(int))
	q.send = func(_ context.Context, pane, pool, text string) relayInjectResult {
		if pane != "workspace:pane-1" || !strings.HasPrefix(text, "continue") {
			t.Fatalf("send %s %s", pane, text)
		}
		return relayInjectResult{Outcome: relayInjectDelivered, Evidence: "visible:nonce_echo", Proven: relayNoncesIn(text)}
	}

	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	if len(rig.requests) != 1 {
		t.Fatalf("settled candidates = %d, want 1", len(rig.requests))
	}
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: rig.requests[0].EventID, Pane: rig.requests[0].Pane, Eligible: false, Reason: idleWakeReasonDesignLane})
	if reads != 1 {
		t.Fatalf("quota reads after design-lane suppression = %d, want 1", reads)
	}
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: rig.requests[0].EventID, Pane: rig.requests[0].Pane, Eligible: false, Reason: idleWakeReasonDesignLane})
	if reads != 1 {
		t.Fatalf("quota reads after repeat route = %d, want 1 (one-time read per candidate)", reads)
	}
}
