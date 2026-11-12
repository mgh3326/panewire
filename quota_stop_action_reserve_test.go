package panewire

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// A direct caller (the stall net) with no observe-reserved row still gets
// exactly one decision row, and a repeat call for the same stop acts zero
// more times (hk 4302 review: actOnQuotaStop owns the reservation).
func TestQuotaStopActionReservesDecisionForDirectCaller(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	q, err := newQuotaStopManager(rig.store, rig.manager.inboxRoot, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	notes := 0
	policy := `## Values
- default_expected: 1d
- urgent_priorities: [1, 2]
- max_waits: 1
- resume_text: "continue (the quota has reset; resume exactly where you stopped)"
`
	q.hk = quotaStopHKFixture(quotaPolicyAnswer(policy), `{"tasks":[{"priority":1,"expected":"6h","refs":{"job_id":"job-direct"}}]}`, &notes)

	now := rig.now
	c := idleWakeCandidate{EventID: "wake-direct", PaneID: "workspace:pane-9", Label: "worker-direct"}
	job := HubActiveJob{
		JobID:      "job-direct",
		AgentLabel: "worker-direct",
		OwnerLane:  "owner",
		Role:       "worker",
		StartedAt:  now.Add(-time.Hour).UTC().Format(time.RFC3339),
	}
	first := quotastop.Stop{Pool: "codex", Reset: now.Add(2 * time.Hour)}
	if err := q.actOnQuotaStop(context.Background(), c, job, "", "", first, quotastop.BasisHerdr, now); err != nil {
		t.Fatalf("first action: %v", err)
	}

	var rows int
	var pool, decision string
	if err := rig.store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE event_id='wake-direct'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("decision rows = %d, want 1 (err=%v)", rows, err)
	}
	if err := rig.store.db.QueryRow(`SELECT pool,decision FROM quota_stop_decisions WHERE event_id='wake-direct'`).Scan(&pool, &decision); err != nil || pool != "codex" || decision == "" {
		t.Fatalf("first decision pool=%q decision=%q err=%v, want codex and a decision", pool, decision, err)
	}

	// A repeat for the same stop must not act again: a different pool would
	// overwrite the row if it did.
	second := quotastop.Stop{Pool: "claude", Reset: now.Add(3 * time.Hour)}
	if err := q.actOnQuotaStop(context.Background(), c, job, "", "", second, quotastop.BasisClassifier, now.Add(time.Minute)); err != nil {
		t.Fatalf("repeat action: %v", err)
	}
	if err := rig.store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE event_id='wake-direct'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("decision rows after repeat = %d, want 1 (err=%v)", rows, err)
	}
	if err := rig.store.db.QueryRow(`SELECT pool FROM quota_stop_decisions WHERE event_id='wake-direct'`).Scan(&pool); err != nil || pool != "codex" {
		t.Fatalf("pool after repeat = %q, want codex (err=%v)", pool, err)
	}
}
