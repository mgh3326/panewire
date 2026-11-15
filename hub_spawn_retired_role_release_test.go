package panewire

import (
	"slices"
	"testing"
	"time"
)

// A finished request stored under a retired role name (`worker`, from before
// the rename) cannot be parsed any more. The orphan sweep still frees its
// reservation, by request id, instead of failing release on every pass.
func TestReleaseSpawnRoleFreesAFinishedRequestStoredUnderARetiredRole(t *testing.T) {
	hub, _, _, held := roleHolderHub(t, hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane}, "spawned", "succeeded")
	ctx := t.Context()
	if _, err := hub.inbox.store.db.ExecContext(ctx, hub.inbox.store.q(`UPDATE spawn_reservations SET task_role='42:worker' WHERE request_id=?`), held.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	held.Request.Args = slices.Replace(slices.Clone(held.Request.Args), slices.Index(held.Request.Args, "implementer"), slices.Index(held.Request.Args, "implementer")+1, "worker")
	if err := hub.releaseSpawnRole(ctx, held); err != nil {
		t.Fatalf("release of an unparsable finished request failed: %v", err)
	}
	if owner, _, _, err := hub.inbox.store.spawnRoleOwner(ctx, "42:worker"); err == nil {
		t.Fatalf("the retired-role slot is still held by %s", owner)
	}
	// A new request reserves the slot under the new name at once.
	if _, _, err := hub.inbox.store.claimSpawn(ctx, roleSecondRequest(), "test-job-r2", "parent", time.Now()); err != nil {
		t.Fatalf("new request refused: %v", err)
	}
}
