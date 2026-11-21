package fleet

import (
	"context"
	"testing"
)

// hk 5829: a hub-requested spawn stamps its request id on the job.spawned
// receipt as node_request_id, so the node's restart settle can bind the
// record to the attempt; a spawn with no such context writes no key, the
// shape an older node's record has.
func TestSpawnStampsNodeRequestIDOnSpawnedReceipt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		requestID string // "" leaves the context bare
	}{
		{"a hub request's spawn", "req-5829"},
		{"a spawn with no request binding", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, _ := cmdFixture(t)
			ctx := context.Background()
			if tc.requestID != "" {
				ctx = WithSpawnNodeRequestID(ctx, tc.requestID)
			}
			if e := a.Spawn(ctx, s); e != nil {
				t.Fatal(e)
			}
			for _, ev := range mustEvents(t, a, s.Job) {
				if ev.Kind != "job.spawned" {
					continue
				}
				got, ok := ev.Payload["node_request_id"]
				if tc.requestID == "" {
					if ok {
						t.Fatalf("node_request_id = %v on an unbound spawn", got)
					}
					return
				}
				if !ok || got != tc.requestID {
					t.Fatalf("node_request_id = %v (present=%v), want %q", got, ok, tc.requestID)
				}
				return
			}
			t.Fatal("no job.spawned event")
		})
	}
}
