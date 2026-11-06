package panewire

import (
	"os"
	"testing"
)

// hk 3276: a lane that opts into hold_while_working still gets a worker
// directive at once. The lane here has a parent, so it is a worker lane; the
// same findings notice to the same lane without a parent stays held while the
// pane is working (TestHoldWhileWorkingReleasedOnIdle).
func TestHoldWhileWorkingNeverHoldsWorkerDirective_t3276(t *testing.T) {
	h, a, _, _, _ := holdHub(t, "", true)
	lanes := `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-b"},"lane-b":{"machine":"host-b","pane":"w2:p1"}}}`
	if err := os.WriteFile(h.reportRelayPath, []byte(lanes), 0o600); err != nil {
		t.Fatal(err)
	}
	holdNotice(t, h, a, "directive", "findings", "FINDINGS review complete")
	d := holdReceive(t, a)
	if d.Deliver != "now" {
		t.Fatalf("worker directive injected with deliver=%q, want now", d.Deliver)
	}
}
