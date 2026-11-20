package fleet

import (
	"context"
	"testing"
)

// hk 5776: the node reports its start receipt from the landing observer. Spawn
// calls it once, after job.spawned is committed and not before, and a context
// without an observer is unaffected.
func TestSpawnReportsLandingAfterTheJobSpawnedRecord(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	calls, spawnedWhenCalled := 0, false
	ctx := WithSpawnLanded(context.Background(), func() {
		calls++
		for _, ev := range mustEvents(t, a, s.Job) {
			if ev.Kind == "job.spawned" {
				spawnedWhenCalled = true
			}
		}
	})
	if e := a.Spawn(ctx, s); e != nil {
		t.Fatal(e)
	}
	if calls != 1 || !spawnedWhenCalled {
		t.Fatalf("landing observer calls=%d, job.spawned committed when called=%v; want 1 and true", calls, spawnedWhenCalled)
	}
}
