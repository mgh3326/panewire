package fleet

import (
	"context"
	"testing"
)

// hk 3704 (review on PR 752): the TTL pass keeps a dir its job's relay still
// owes, whatever its age, so the relay window cannot outlive the dir even when
// it is longer than the TTL floor.

func TestJobDirTTLKeepsWhileTheRelayOwes_t3704(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	out, err := r.passWith(&JobDirTTL{TTL: DefaultJobDirTTL, Apply: true, RelayOwed: relayOwes(true, nil)})
	if err != nil {
		t.Fatal(err)
	}
	wantKeep(t, r, out, "j1", "relay-owed")
}

// The TTL pass still deletes an aged dir once its relay has settled, and
// without a relay reader it is unchanged.
func TestJobDirTTLDeletesOnceTheRelaySettles_t3704(t *testing.T) {
	for name, reader := range map[string]func(context.Context, string) (bool, error){
		"settled":   relayOwes(false, nil),
		"no reader": nil,
	} {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			out, err := r.passWith(&JobDirTTL{TTL: DefaultJobDirTTL, Apply: true, RelayOwed: reader})
			if err != nil {
				t.Fatal(err)
			}
			wantDelete(t, r, out, "j1", "confirmed")
		})
	}
}
