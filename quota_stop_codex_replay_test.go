package panewire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// An urgent pinned worker that stops at the codex usage limit is handed over
// (never resumed) with one owner notice and one hk quota-stop note (hk 2847).
func TestQuotaStopCodexReplayHandsOverUrgentWorker(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	root := rig.manager.inboxRoot
	events := filepath.Join(root, "jobs", "job-urgent", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	for name, body := range map[string]string{
		"00001-job.claimed.json": `{"type":"job.claimed","created_at":"` + stamp + `","agent_label":"worker-synthetic","owner_lane":"owner","role":"worker","epoch":1}`,
		"00002-job.spawned.json": `{"type":"job.spawned","created_at":"` + stamp + `","pane_id":"workspace:pane-1"}`,
	} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	q, err := newQuotaStopManager(rig.store, root, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	rig.manager.quota = q
	reads, sends, notes, owners := 0, 0, 0, 0
	announce := q.announce
	q.announce = func(ctx context.Context, job, pane string, stop quotastop.Stop) error {
		owners++
		return announce(ctx, job, pane, stop)
	}
	screen, err := os.ReadFile("testdata/quota-stop/codex.txt")
	if err != nil {
		t.Fatal(err)
	}
	q.read = func(context.Context, string) (string, error) { reads++; return string(screen), nil }
	policy := `## Values
- default_expected: 1d
- urgent_priorities: [1, 2]
- max_waits: 1
- resume_text: "continue (the quota has reset; resume exactly where you stopped)"
`
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), `{"tasks":[{"priority":1,"expected":"6h","refs":{"job_id":"job-urgent"}}]}`, &notes)
	q.send = func(context.Context, string, string, string) relayInjectResult {
		sends++
		return relayInjectResult{Outcome: relayInjectDelivered}
	}

	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
	if len(rig.requests) != 1 {
		t.Fatalf("settled candidates = %d, want 1", len(rig.requests))
	}
	rig.route(0, "owner")
	rig.manager.Tick(context.Background(), rig.now.Add(6*time.Second))
	if reads != 1 {
		t.Fatalf("quota reads = %d, want 1", reads)
	}
	if sends != 0 {
		t.Fatalf("urgent worker was resumed %d times, want 0", sends)
	}
	if notes != 1 {
		t.Fatalf("hk quota-stop notes = %d, want 1", notes)
	}
	if owners != 1 {
		t.Fatalf("owner notices = %d, want exactly one", owners)
	}
}
