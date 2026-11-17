package panewire

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestCloseOpsDoesNotAdvertiseBeforeStartupRecovery(t *testing.T) {
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	list := func(context.Context) ([]byte, error) { return nil, nil }
	if err := fleet.EnableCloseOps(t.Context(), jobs, list, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := fleet.AdvertiseCloseOps(jobs, time.Now()); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: inbox}
	if slices.Contains(client.spawnHelloCapabilities(t.Context()), hubCapabilityCloseOps) {
		t.Fatal("node advertised close operations before checking legacy journals")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.runHubCloseSettle(ctx) }()
	defer func() { cancel(); <-done }()
	eventually(t, 2*time.Second, "close operations advertised after recovery", func() bool {
		return slices.Contains(client.spawnHelloCapabilities(ctx), hubCapabilityCloseOps)
	})
}

func TestCloseOpsStartupRecoversLegacyUnfinishedJournal(t *testing.T) {
	for _, tc := range []struct{ initial, want string }{
		{"missing", fleet.CloseOpCancelled},
		{fleet.CloseOpRegistered, fleet.CloseOpCancelled},
		{fleet.CloseOpClosing, fleet.CloseOpUnknown},
	} {
		t.Run(tc.initial, func(t *testing.T) {
			inbox := t.TempDir()
			fake := idlePaneHerdr()
			client := closeOpNode(t, inbox, fake)
			addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"legacy-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a","end_reason":"quota_stop pool=devin-free"}}`)
			if tc.initial != "missing" {
				seedNodeCloseOp(t, inbox, "legacy-op", tc.initial)
				root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
				if err != nil {
					t.Fatal(err)
				}
				op, _, err := fleet.ReadCloseOp(root, "legacy-op")
				if err != nil {
					t.Fatal(err)
				}
				op.Epoch = 0
				if err := fleet.WriteCloseOp(root, op); err != nil {
					t.Fatal(err)
				}
				root.Close()
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); client.runHubCloseSettle(ctx) }()
			var got fleet.CloseOp
			recovered := waitUntil(2*time.Second, func() bool {
				root, err := os.OpenRoot(seedCloseOpJob(t, inbox))
				if err != nil {
					return false
				}
				defer root.Close()
				var found bool
				got, found, err = fleet.ReadCloseOp(root, "legacy-op")
				return err == nil && found && got.State == tc.want
			})
			cancel()
			<-done
			if !recovered || tc.want == fleet.CloseOpCancelled && got.Released == nil {
				t.Fatalf("legacy %s recovered=%v op=%+v; want %s", tc.initial, recovered, got, tc.want)
			}
			if fake.called("pane", "close") != 0 {
				t.Fatal("legacy recovery ran a pane close")
			}
		})
	}
}
