package panewire

import (
	"testing"
	"time"
)

// TestNodeLoopQuotaPoolReconcileStaleAlarmFollowsOptIn pins the ledger's
// opt-in rule for quota_pool_reconcile (hk 5320): a node without
// --quota-pool-reconcile never runs the loop and must raise no stale alarm,
// while a node with the flag still alarms once it goes stale.
func TestNodeLoopQuotaPoolReconcileStaleAlarmFollowsOptIn(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	// Past the stale window: three 5-minute intervals.
	later := start.Add(20 * time.Minute)

	staleHits := func(hub *HubServer, at time.Time) []loopNoticeHit {
		var out []loopNoticeHit
		for _, hit := range hub.nodeLoopNoticeHits(at) {
			if string(hit.Mechanism) == string(nodeQuotaPoolReconcile) {
				out = append(out, hit)
			}
		}
		return out
	}

	t.Run("flag off raises no alarm", func(t *testing.T) {
		now := start
		client := &HubClient{now: func() time.Time { return now }, quotaPoolReconcile: false}
		hub := loopLedgerTestHub(t, &now, nil)
		hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), start)
		if hits := staleHits(hub, later); len(hits) != 0 {
			t.Fatalf("flag off quota_pool_reconcile hits=%+v, want none", hits)
		}
	})

	t.Run("flag on and never ticking alarms", func(t *testing.T) {
		now := start
		client := &HubClient{now: func() time.Time { return now }, quotaPoolReconcile: true}
		hub := loopLedgerTestHub(t, &now, nil)
		hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), start)
		if hits := staleHits(hub, later); len(hits) != 1 {
			t.Fatalf("flag on, not ticking hits=%+v, want one stale hit", hits)
		}
	})

	t.Run("flag on and stale after a tick alarms", func(t *testing.T) {
		now := start
		client := &HubClient{now: func() time.Time { return now }, quotaPoolReconcile: true}
		client.nodeLoopTick(nodeQuotaPoolReconcile)
		hub := loopLedgerTestHub(t, &now, nil)
		hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), start)
		if hits := staleHits(hub, later); len(hits) != 1 {
			t.Fatalf("flag on, stale after tick hits=%+v, want one stale hit", hits)
		}
		// A fresh tick inside the window clears the alarm.
		fresh := start.Add(4 * time.Minute)
		now = fresh
		client.nodeLoopTick(nodeQuotaPoolReconcile)
		hub.observeNodeLoops("machine-a", client.nodeLoopsForHeartbeat(), fresh)
		if hits := staleHits(hub, fresh.Add(time.Minute)); len(hits) != 0 {
			t.Fatalf("flag on, fresh tick hits=%+v, want none", hits)
		}
	})
}
