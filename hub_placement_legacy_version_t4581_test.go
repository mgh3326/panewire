package panewire

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestLegacyPlacementVersionReconciliation_t4581(t *testing.T) {
	for _, tc := range []struct {
		name          string
		finished      bool
		localVersion  string
		remoteVersion string
		remoteReason  string
		localTime     string
		remoteTime    string
		historical    bool
		negativeZero  bool
		timeout       bool
		wantHold      bool
	}{
		{name: "flat trace accepts existing version", remoteVersion: "v-before-upgrade"},
		{name: "flat trace still rejects other differences", remoteVersion: "v-before-upgrade", remoteReason: "different_result", wantHold: true},
		{name: "flat trace accepts explicitly empty hk version"},
		{name: "flat trace matches hk microsecond precision", remoteVersion: "v-before-upgrade", localTime: "2026-11-14T00:00:00.123456789Z", remoteTime: "2026-11-14T00:00:00.123456Z"},
		{name: "finished record matches hk microsecond precision", finished: true, localVersion: "v-before-upgrade", remoteVersion: "v-before-upgrade", localTime: "2026-11-14T00:00:00.123456789Z", remoteTime: "2026-11-14T00:00:00.123456Z"},
		{name: "different microseconds remain a conflict", remoteVersion: "v-before-upgrade", localTime: "2026-11-14T00:00:00.123456789Z", remoteTime: "2026-11-14T00:00:00.123457Z", wantHold: true},
		{name: "flat trace matches hk normalized zero", remoteVersion: "v-before-upgrade", negativeZero: true},
		{name: "finished record matches hk normalized zero", finished: true, localVersion: "v-before-upgrade", remoteVersion: "v-before-upgrade", negativeZero: true},
		{name: "flat trace finds an older retained hk record", remoteVersion: "v-before-upgrade", localTime: "2020-01-01T00:00:00Z", historical: true},
		{name: "finished record accepts its own version", finished: true, localVersion: "v-before-upgrade", remoteVersion: "v-before-upgrade"},
		{name: "finished record rejects a different version", finished: true, localVersion: "v-before-upgrade", remoteVersion: "v-other", wantHold: true},
		{name: "finished record cannot mask the terminal timeout", finished: true, localVersion: "v-before-upgrade", remoteVersion: "v-before-upgrade", timeout: true, wantHold: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := placementOutboxServer(t)
			hub := placementOutboxHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			request := durableSpawnRequest()
			task := boundSpawnTask(t, hub, request)
			old := placementTraceSnapshot()
			if tc.negativeZero {
				score := math.Copysign(0, -1)
				old.Candidates[0].Score = &score
			}
			if tc.localTime != "" {
				old.DecidedAt = tc.localTime
			}
			payload, _ := json.Marshal(old)
			if err := hub.inbox.store.saveSpawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt, payload, ""); err != nil {
				t.Fatal(err)
			}
			legacy := old
			legacy.Candidates = append(legacy.Candidates, fleet.PlacementCandidateRecord{Host: "legacy-refusal", Outcome: "skipped", Reason: "hub_cwd_unmapped"})
			remote := fleet.PlacementRecord{
				JobID: task.Job, Attempt: task.Attempt, TaskID: 42, DecidedAt: old.DecidedAt, ToolVersion: tc.remoteVersion,
				Source: "auto", Ranking: "score", StateSource: "hub", Outcome: "failed", Reason: "spawn_failed",
				Candidates:         append([]fleet.PlacementCandidateRecord(nil), legacy.Candidates...),
				PlacementExecution: fleet.PlacementExecution{Host: "machine-a", Family: "claude", Profile: "claude-sonnet", Pool: "claude-sub", Role: "worker"},
			}
			if tc.finished {
				local := remote
				local.ToolVersion = tc.localVersion
				local.Candidates = append([]fleet.PlacementCandidateRecord(nil), legacy.Candidates...)
				legacy.Record, legacy.Outcome, legacy.Reason = &local, local.Outcome, local.Reason
			}
			if tc.negativeZero {
				score := 0.0
				remote.Candidates[0].Score = &score
			}
			if tc.remoteReason != "" {
				remote.Reason = tc.remoteReason
			}
			if tc.remoteTime != "" {
				remote.DecidedAt = tc.remoteTime
			}
			legacyPayload, _ := json.Marshal(legacy)
			deferred, _ := json.Marshal([]placementDeferredSave{{RequestID: request.RequestID, Attempt: task.Attempt, Payload: legacyPayload}})
			if err := os.WriteFile(hub.inbox.deferredPath, deferred, 0o600); err != nil {
				t.Fatal(err)
			}
			outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", NodeResult: true}
			if tc.timeout {
				outcome = hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a", Retryable: true, NodeResult: true}
			}
			if changed, err := hub.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
				t.Fatalf("commit result: changed=%v err=%v", changed, err)
			}
			hk.mu.Lock()
			hk.lookup = []fleet.PlacementRecord{remote}
			hk.lookupRequireSince = tc.historical
			hk.mu.Unlock()
			hub.flushPlacementRecords(t.Context())
			_, _, recorded, err := hub.inbox.store.spawnPlacementSnapshot(t.Context(), request.RequestID, task.Attempt)
			if err != nil || recorded == tc.wantHold {
				t.Fatalf("recorded=%v wantHold=%v err=%v", recorded, tc.wantHold, err)
			}
			if records := hk.records(); len(records) != 0 {
				t.Fatalf("existing hk record was posted again: %v", records)
			}
			if hub.hasDeferredTraceSaves() != tc.wantHold {
				t.Fatalf("deferred trace retained=%v, want %v", hub.hasDeferredTraceSaves(), tc.wantHold)
			}
			var notices int
			err = hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_notifications WHERE request_id=? AND owner=? AND result LIKE ?`, request.RequestID, task.Owner, "%legacy_placement_record_conflict%").Scan(&notices)
			if err != nil || (notices == 1) != tc.wantHold {
				t.Fatalf("owner notices=%d wantHold=%v err=%v", notices, tc.wantHold, err)
			}
		})
	}
}
