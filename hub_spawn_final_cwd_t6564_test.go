package panewire

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestNodeSpawnFinalCWDWriteFailureStillLaunchesAndReplays_t6564(t *testing.T) {
	fixture, client, _ := rootChangeSpawnFixture(t, 2)
	message := rootChangeMessage(fixture, 656401, "job-a")
	journal, err := openNodeSpawnJournal(client.spawnJournalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.db.Close()
	if claimed, _, err := journal.claim(t.Context(), message, spawnJournalHold{}); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	if _, err := journal.db.Exec(`CREATE TRIGGER fail_final_cwd BEFORE UPDATE OF final_cwd ON spawn_inbox BEGIN SELECT RAISE(ABORT, 'injected final_cwd failure'); END`); err != nil {
		t.Fatal(err)
	}

	engineCalls := 0
	client.spawnEngine = func(_ context.Context, cwd fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		engineCalls++
		if want := filepath.Join(fixture.workRoot, "job-a"); cwd.Path != want {
			t.Errorf("launch cwd=%q, want %q", cwd.Path, want)
		}
		return t14EngineOK()
	}
	result := client.runSpawnTarget(t.Context(), message, client.spawnTargetFor(message), journal)
	if engineCalls != 1 || result.RC != 0 {
		t.Fatalf("spawn result=%+v, engine calls=%d; want successful launch", result, engineCalls)
	}
	if err := journal.complete(t.Context(), result); err != nil {
		t.Fatal(err)
	}
	known, replay, err := journal.lookup(t.Context(), message)
	if err != nil || !known || replay == nil || replay.RC != 0 || replay.Error != "" {
		t.Fatalf("replay: known=%v result=%+v err=%v, want cached launch success", known, replay, err)
	}
}

func TestNodeSpawnFinalCWDWriteAfterTimeoutReturnsTimeout_t6564(t *testing.T) {
	fixture, client, _ := rootChangeSpawnFixture(t, 2)
	message := rootChangeMessage(fixture, 656402, "job-a")
	journal, err := openNodeSpawnJournal(client.spawnJournalDir)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.db.Close()
	if claimed, _, err := journal.claim(t.Context(), message, spawnJournalHold{}); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client.spawnPrepare = func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		cancel()
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: filepath.Join(fixture.workRoot, "job-a")}}, ""
	}
	engineCalls := 0
	client.spawnEngine = func(ctx context.Context, _ fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		engineCalls++
		if ctx.Err() == nil {
			t.Error("engine context is not expired")
		}
		return &hubSpawnOutput{}, fleet.Execution{RC: 1, Stage: "launch"}
	}
	result := client.runSpawnTarget(ctx, message, client.spawnTargetFor(message), journal)
	if engineCalls != 1 || result.RC != 124 || result.Error != "timeout" {
		t.Fatalf("spawn result=%+v, engine calls=%d; want timeout after prepare", result, engineCalls)
	}
	if err := journal.complete(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	known, replay, err := journal.lookup(context.Background(), message)
	if err != nil || !known || replay == nil || replay.RC != 124 || replay.Error != "timeout" {
		t.Fatalf("replay: known=%v result=%+v err=%v, want cached timeout", known, replay, err)
	}
}
