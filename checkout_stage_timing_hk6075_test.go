package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6075: a checkout reports lock_wait, fetch and worktree_add beside its
// total, so a slow checkout names the step that took the time. The fixture is
// a real git repository; the delays are simulated latency on the fetch and the
// worktree add, so each step's elapsed time has a known lower bound.
func TestCheckoutStageTimingSplitsLockWaitFetchAndWorktreeAdd(t *testing.T) {
	fixture := newNodeWorktreeFixture(t)
	config := filepath.Join(t.TempDir(), "spawn.json")
	contents, err := json.Marshal(map[string]any{"cwd_map": map[string]string{}, "workspace": "worker", "herdr_session": "worker", "work_root": fixture.workRoot, "enabled": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, contents, 0600); err != nil {
		t.Fatal(err)
	}

	const fetchDelay, addDelay = 40 * time.Millisecond, 60 * time.Millisecond
	realGit := defaultNodeCommandRunner()
	realAdd := defaultNodeAddRunner()
	client := &HubClient{
		spawnConfigPath: config,
		// Simulated latency: a slow fetch and a slow worktree add.
		nodeGit: func(ctx context.Context, args ...string) ([]byte, error) {
			if nodeGitSubcommand(args) == "fetch" {
				time.Sleep(fetchDelay)
			}
			return realGit(ctx, args...)
		},
		nodeAdd: func(ctx context.Context, record func(nodeAddProcess) error, args ...string) error {
			time.Sleep(addDelay)
			return realAdd(ctx, record, args...)
		},
		spawnEngine: func(_ context.Context, _ fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
			return t14EngineOK()
		},
	}
	checkout := &hubSpawnCheckout{Origin: "file://" + fixture.origin, Mode: "detached", Sha: fixture.feat, Fetch: "pull/1/head", Dir: "repo.job"}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(6075), CWDKey: "unmapped-repo", BriefInline: "brief", Checkout: checkout, WaitSeconds: 1})
	if result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}

	for _, key := range []string{"checkout", checkoutStageLockWait, checkoutStageFetch, checkoutStageWorktreeAdd} {
		if _, ok := result.StageMS[key]; !ok {
			t.Fatalf("StageMS=%v lacks %q", result.StageMS, key)
		}
	}
	sub := result.StageMS[checkoutStageLockWait] + result.StageMS[checkoutStageFetch] + result.StageMS[checkoutStageWorktreeAdd]
	if sub > result.StageMS["checkout"] {
		t.Fatalf("sub-stages sum to %dms, over the checkout total %dms: %v", sub, result.StageMS["checkout"], result.StageMS)
	}
	if got := result.StageMS[checkoutStageFetch]; got < fetchDelay.Milliseconds() {
		t.Fatalf("fetch=%dms, want at least the %dms delay: %v", got, fetchDelay.Milliseconds(), result.StageMS)
	}
	if got := result.StageMS[checkoutStageWorktreeAdd]; got < addDelay.Milliseconds() {
		t.Fatalf("worktree_add=%dms, want at least the %dms delay: %v", got, addDelay.Milliseconds(), result.StageMS)
	}
}

// Only a hub that lists hubFeatureSpawnStageCheckout takes the sub-stage keys.
// Any other hub, which rejects an unknown key, gets the checkout total alone,
// and the journaled result keeps its own map.
func TestCheckoutStageTimingFoldsSubStagesOutForAnOlderHub(t *testing.T) {
	result := hubSpawnResult{StageMS: map[string]int64{"claim": 5, "checkout": 30, checkoutStageLockWait: 1, checkoutStageFetch: 10, checkoutStageWorktreeAdd: 12}}

	kept := spawnCheckoutStagesForHub(result, true)
	if len(kept.StageMS) != 5 || kept.StageMS[checkoutStageFetch] != 10 {
		t.Fatalf("new hub got %v, want the sub-stages kept", kept.StageMS)
	}

	folded := spawnCheckoutStagesForHub(result, false)
	if !slices.Equal(sortedStageKeys(folded.StageMS), []string{"checkout", "claim"}) {
		t.Fatalf("older hub got %v, want only the checkout total", folded.StageMS)
	}
	if !validSpawnStages(folded.StageMS) {
		t.Fatalf("folded map is not one the hub accepts: %v", folded.StageMS)
	}
	if len(result.StageMS) != 5 || result.StageMS[checkoutStageFetch] != 10 {
		t.Fatalf("fold changed the journaled map: %v", result.StageMS)
	}
}

// The hub accepts the sub-stage keys and advertises that in hub.features, so a
// node that sees the flag sends them.
func TestCheckoutStageTimingHubAcceptsAndAdvertisesTheSubStages(t *testing.T) {
	for _, key := range []string{checkoutStageLockWait, checkoutStageFetch, checkoutStageWorktreeAdd} {
		if !validSpawnTimingKey(key) {
			t.Fatalf("hub rejects stage_ms key %q", key)
		}
	}
	if !slices.Contains(hubFeaturesFrame().Features, hubFeatureSpawnStageCheckout) {
		t.Fatalf("hub.features lacks %q", hubFeatureSpawnStageCheckout)
	}
}

func sortedStageKeys(stages map[string]int64) []string {
	keys := make([]string, 0, len(stages))
	for key := range stages {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
