package panewire

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

type rungOutCall struct {
	job  string
	stop quotastop.Stop
}

// The quota-stop manager keeps the rung a stop spent out of new placements when
// it sees the stop, whatever it then decides for the pane (hk 5562): a reviewer
// the policy waits for, an implementer it waits for, and a handover all report
// the stop. Before, the limit reached the account's shared reading only when
// the job ended, so a pane that waited left the rung open to every other
// request for the whole wait.
func TestQuotaStopKeepsTheLimitedRungOutWhateverItDecides_T5562(t *testing.T) {
	for _, tc := range []struct {
		name, role, screen string
		startedAgo         time.Duration
	}{
		{"reviewer waits", "reviewer", "testdata/quota-stop/devin-reset-2min.txt", time.Hour},
		{"implementer waits", "implementer", "testdata/quota-stop/devin-reset-2min.txt", time.Hour},
		{"handover on a long reset", "implementer", "testdata/quota-stop/devin.txt", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRoleStopRigScreen(t, tc.role, tc.startedAgo, roleStopTask, tc.screen, "")
			calls := make(chan rungOutCall, 4)
			rig.q.keepRungOut = func(_ context.Context, job string, stop quotastop.Stop, _ func() time.Time) {
				calls <- rungOutCall{job, stop}
			}
			rig.run()
			select {
			case got := <-calls:
				if got.job != "job-active" || got.stop.Pool != "devin-free" || got.stop.Reset.IsZero() {
					t.Fatalf("kept out %+v, want job-active on devin-free with its reset", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the stop did not keep its rung out of new placements")
			}
		})
	}
}

// A screen that is no stop keeps nothing out.
func TestQuotaStopKeepsNothingOutForAScreenWithoutAStop_T5562(t *testing.T) {
	rig := newRoleStopRigScreen(t, "reviewer", time.Hour, roleStopTask, "testdata/quota-stop/devin-reset-2min.txt", "")
	called := make(chan struct{}, 1)
	rig.q.keepRungOut = func(context.Context, string, quotastop.Stop, func() time.Time) { called <- struct{}{} }
	rig.q.read = func(context.Context, string) (string, error) { return "working on the review\n", nil }
	rig.run()
	select {
	case <-called:
		t.Fatal("a screen without a limit kept a rung out")
	case <-time.After(200 * time.Millisecond): // proves nothing more happens
	}
}
