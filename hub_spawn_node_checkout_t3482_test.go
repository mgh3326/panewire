package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func t3482SpawnConfig(t *testing.T, workRoot string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spawn.json")
	contents := `{"cwd_map":{},"workspace":"worker","herdr_session":"worker"`
	if workRoot != "" {
		contents += `,"work_root":"` + workRoot + `"`
	}
	contents += `,"enabled":true}`
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func t3482Checkout() *hubSpawnCheckout {
	return &hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: "branch", Branch: "feature/t3482", Dir: "repo-job"}
}

func TestT3482NodeClonesAnUnmappedRepoWhenTheSpawnCarriesACheckout(t *testing.T) {
	root := t.TempDir()
	preparedPath := filepath.Join(root, "prepared")
	config := t3482SpawnConfig(t, filepath.Join(root, "work"))

	prepared := 0
	ranIn := ""
	client := &HubClient{
		spawnConfigPath: config,
		spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
			prepared++
			return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: preparedPath}}, ""
		},
		spawnEngine: func(_ context.Context, cwd, _, _, _ string, _ []string) (*hubSpawnOutput, fleet.Execution) {
			ranIn = cwd
			return t14EngineOK()
		},
	}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(482), CWDKey: "unmapped-repo", BriefInline: "brief", Checkout: t3482Checkout(), WaitSeconds: 1})
	if result.Error == "cwd_unmapped" || result.RC != 0 {
		t.Fatalf("result=%+v", result)
	}
	if prepared != 1 || ranIn != preparedPath {
		t.Fatalf("prepared=%d ranIn=%q", prepared, ranIn)
	}

	// The same node without a checkout still refuses the unmapped repo.
	noCheckout := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(483), CWDKey: "unmapped-repo", BriefInline: "brief", WaitSeconds: 1})
	if noCheckout.RC != 2 || noCheckout.Error != "cwd_unmapped" || prepared != 1 {
		t.Fatalf("no checkout result=%+v prepared=%d", noCheckout, prepared)
	}
}

func TestT3482NodeStillRefusesAnUnmappedCheckoutWithoutAWorkRoot(t *testing.T) {
	root := t.TempDir()
	config := t3482SpawnConfig(t, "")

	prepared := 0
	client := &HubClient{
		spawnConfigPath: config,
		spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
			prepared++
			return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: filepath.Join(root, "prepared")}}, ""
		},
	}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(484), CWDKey: "unmapped-repo", BriefInline: "brief", Checkout: t3482Checkout(), WaitSeconds: 1})
	if result.RC != 2 || result.Error != "cwd_unmapped" || prepared != 0 {
		t.Fatalf("result=%+v prepared=%d", result, prepared)
	}
}
