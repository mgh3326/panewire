package fleet

import (
	"context"
	"testing"
)

func TestHubCloseSettlesAnEpochStampedIntent(t *testing.T) {
	a, hk := hubCloseFixture(t)
	in := liveIntent(t, a)
	in.Epoch = 3
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, Epoch: in.Epoch, State: CloseOpClosed})
	state, err := (HubCloseSettlement{App: a}).Settle(context.Background(), "j")
	if err != nil || state != HubCloseSettled {
		t.Fatalf("settlement=%v err=%v", state, err)
	}
	wantHubCloseFinished(t, a, hk)
}
