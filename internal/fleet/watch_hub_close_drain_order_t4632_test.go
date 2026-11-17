package fleet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A close that finishes after settlement's first read, followed by a drain,
// must not be tombstoned as cancelled: the drain marker is read before the
// journal, so a marker of true always pairs with a current journal entry.
func TestSettlementReadsDrainMarkerBeforeJournal_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpRegistered})
	admission, err := LockCloseOpsAdmission(context.Background(), a.Jobs)
	if err != nil {
		t.Fatal(err)
	}
	read, proceed := make(chan struct{}), make(chan struct{}, 1)
	settlementBetweenDrainAndJournalReads = func() { close(read); <-proceed }
	t.Cleanup(func() {
		settlementBetweenDrainAndJournalReads = nil
		select {
		case proceed <- struct{}{}:
		default:
		}
	})
	states := make(chan HubCloseState, 1)
	go func() {
		state, _ := (HubCloseSettlement{App: a}).Settle(context.Background(), "j")
		states <- state
	}()
	select {
	case <-read:
	case <-time.After(5 * time.Second):
		t.Fatal("settlement did not reach the seam between its reads")
	}
	// A close that had already passed admission completes after the first read,
	// then the drain publishes its marker.
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	admission()
	if err := BeginCloseOpsDrain(context.Background(), a.Jobs); err != nil {
		t.Fatal(err)
	}
	proceed <- struct{}{}
	select {
	case <-states:
	case <-time.After(5 * time.Second):
		t.Fatal("settlement did not finish")
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	op, _, err := ReadCloseOp(root, in.RequestID)
	if err != nil || op.State != CloseOpClosed {
		t.Fatalf("completed close was overwritten: state=%q err=%v hk=%v", op.State, err, h.record(t, "j"))
	}
}
