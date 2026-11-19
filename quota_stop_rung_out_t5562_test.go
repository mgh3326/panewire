package panewire

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// rungOutRig is a role rig whose record and publication hooks are observed.
type rungOutRig struct {
	*roleStopRig
	mu        sync.Mutex
	recorded  []quotastop.Stop
	published chan string
	failures  int
	attempts  int
	block     chan struct{}
}

func newRungOutRig(t *testing.T, role, screen string) *rungOutRig {
	t.Helper()
	r := &rungOutRig{roleStopRig: newRoleStopRigScreen(t, role, time.Hour, roleStopTask, screen, ""), published: make(chan string, 8)}
	r.q.publishRetry = time.Millisecond
	r.q.recordRung = func(job string, stop quotastop.Stop, _ func() time.Time) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.recorded = append(r.recorded, stop)
		return nil
	}
	r.q.publishRung = func(_ context.Context, job string, _ func() time.Time) error {
		r.mu.Lock()
		r.attempts++
		fail := r.attempts <= r.failures
		block := r.block
		r.mu.Unlock()
		if block != nil {
			<-block
		}
		if fail {
			return errors.New("lease held by another host")
		}
		r.published <- job
		return nil
	}
	return r
}

func (r *rungOutRig) recordedStops() []quotastop.Stop {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]quotastop.Stop(nil), r.recorded...)
}

// The quota-stop manager keeps the rung a stop spent out of new placements when
// it sees the stop, whatever it then decides for the pane (hk 5562): a reviewer
// and an implementer the policy waits for, and a handover all report the stop.
// Before, the limit reached the account's shared reading only when the job
// ended, so a pane that waited left the rung open to every other request for
// the whole wait.
func TestQuotaStopKeepsTheLimitedRungOutWhateverItDecides_T5562(t *testing.T) {
	for _, tc := range []struct{ name, role, screen string }{
		{"reviewer waits", "reviewer", "testdata/quota-stop/devin-reset-2min.txt"},
		{"implementer waits", "implementer", "testdata/quota-stop/devin-reset-2min.txt"},
		{"handover on a long reset", "implementer", "testdata/quota-stop/devin.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRungOutRig(t, tc.role, tc.screen)
			rig.run()
			select {
			case job := <-rig.published:
				if job != "job-active" {
					t.Fatalf("published for %q, want job-active", job)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the stop did not keep its rung out of new placements")
			}
			stops := rig.recordedStops()
			if len(stops) == 0 || stops[0].Pool != "devin-free" || stops[0].Reset.IsZero() {
				t.Fatalf("recorded %+v, want a devin-free stop with its reset", stops)
			}
		})
	}
}

// The local record is written before the observation returns, even when the
// publication is slow: the wake is reserved and never read again, so a record
// left to a later step is lost with a daemon exit in between.
func TestQuotaStopRecordsTheLimitBeforeItPublishes_T5562(t *testing.T) {
	rig := newRungOutRig(t, "reviewer", "testdata/quota-stop/devin-reset-2min.txt")
	rig.block = make(chan struct{})
	t.Cleanup(func() { close(rig.block) })
	rig.run()
	if len(rig.recordedStops()) == 0 {
		t.Fatal("the limit was not recorded while its publication was still running")
	}
}

// A refresh that fails (the shared lease held by another host, a store error)
// is tried again until it lands: a waiting pane has no later event to repeat it.
func TestQuotaStopRetriesAFailedPublication_T5562(t *testing.T) {
	rig := newRungOutRig(t, "reviewer", "testdata/quota-stop/devin-reset-2min.txt")
	rig.failures = 2
	rig.run()
	select {
	case <-rig.published:
	case <-time.After(5 * time.Second):
		t.Fatalf("the publication was not retried (attempts=%d)", rig.attempts)
	}
	rig.mu.Lock()
	defer rig.mu.Unlock()
	if rig.attempts != 3 {
		t.Fatalf("attempts=%d, want 3 (two failures, then the publication)", rig.attempts)
	}
}

// Once the limit is published into the shared store the node's quota loop is
// asked to send it to the hub, which placement reads first: left to the loop's
// next interval, default placement keeps the old reading for up to a minute
// (hk 5562 review).
func TestQuotaStopAsksTheNodeToRepublishAPublishedLimit_T5562(t *testing.T) {
	rig := newRungOutRig(t, "reviewer", "testdata/quota-stop/devin-reset-2min.txt")
	kicked := make(chan struct{}, 4)
	rig.q.republish = func() { kicked <- struct{}{} }
	rig.run()
	select {
	case <-kicked:
	case <-time.After(5 * time.Second):
		t.Fatal("the node's quota loop was not asked to republish")
	}
}

// A screen that is no stop keeps nothing out.
func TestQuotaStopKeepsNothingOutForAScreenWithoutAStop_T5562(t *testing.T) {
	rig := newRungOutRig(t, "reviewer", "testdata/quota-stop/devin-reset-2min.txt")
	rig.q.read = func(context.Context, string) (string, error) { return "working on the review\n", nil }
	rig.run()
	select {
	case <-rig.published:
		t.Fatal("a screen without a limit kept a rung out")
	case <-time.After(200 * time.Millisecond): // proves nothing more happens
	}
	if len(rig.recordedStops()) != 0 {
		t.Fatal("a screen without a limit was recorded")
	}
}
