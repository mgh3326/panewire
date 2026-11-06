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
	if d.Deliver != relayDeliverDirective {
		t.Fatalf("worker directive injected with deliver=%q, want directive", d.Deliver)
	}
}

// A lane policy may file findings or orders as inbox (kept for a digest or an
// idle promotion). For a worker lane those are directives and still reach the
// pane at once; the same policy on a lane with no parent keeps its deferral.
func TestWorkerDirectiveBypassesInboxClass_t3276(t *testing.T) {
	policy := "    types:\n      findings: inbox\n      order: inbox\n"
	for _, tc := range []struct {
		name, lanes string
		wantNow     bool
	}{
		{"worker lane", `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"lane-b"},"lane-b":{"machine":"host-b","pane":"w2:p1"}}}`, true},
		{"lane with no parent", `{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`, false},
	} {
		for _, kind := range []string{"findings", "order"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				h, a, _, _, _ := holdHub(t, policy, true)
				if err := os.WriteFile(h.reportRelayPath, []byte(tc.lanes), 0o600); err != nil {
					t.Fatal(err)
				}
				holdNotice(t, h, a, "bypass-"+kind, kind, "review complete")
				select {
				case d := <-a.relays:
					if !tc.wantNow {
						t.Fatalf("a lane with no parent was injected %+v, want it kept as inbox", d)
					}
					if d.Deliver != relayDeliverDirective {
						t.Fatalf("deliver=%q, want directive", d.Deliver)
					}
				default:
					if tc.wantNow {
						t.Fatalf("a %s for a worker lane was kept as inbox instead of injected", kind)
					}
				}
			})
		}
	}
}
