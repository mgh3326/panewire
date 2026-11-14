package panewire

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestLaunchProofCleanupGateT2803(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		retry, want  bool
	}{
		{"proof cleaned", "launch_proof_failed", true, true},
		{"proof uncertain", "launch_proof_failed", false, false},
		{"configuration", "spawn_failed", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			req := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), req, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			node := hubSpawnRequest{RequestID: t14RequestID(2803), Machine: "machine-a", Args: []string{"-m", "sonnet"}}
			task, err = h.inbox.store.bindSpawn(t.Context(), req.RequestID, task.Attempt, node, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, h.inbox.store, task)
			transport := &directSpawnTransport{h: h, task: task, bound: true}
			got, err := transport.nextHostAfterRefusal(t.Context(), node, hubSpawnResult{RC: 70, Error: tc.reason, Retryable: tc.retry})
			if err != nil || got != tc.want {
				t.Fatalf("retry=%v err=%v, want %v", got, err, tc.want)
			}
		})
	}
}

func TestLaunchProofNodeWireReasonT2803(t *testing.T) {
	for _, tc := range []struct{ name, class, want string }{
		{"proof", "harness_version", "launch_proof_failed"},
		{"configuration", "", "spawn_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := quotaClient(t)
			client.spawnConfigPath = t14SpawnConfig(t, t.TempDir())
			client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
				return &hubSpawnOutput{}, fleet.Execution{RC: 70, Stage: "launch", Retryable: true, FailureClass: tc.class}
			}
			result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(2803), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1, Args: []string{"--job", "job-2803", "-m", "codex-sol"}})
			if result.Error != tc.want {
				t.Fatalf("node reason=%q, want %q", result.Error, tc.want)
			}
		})
	}
}

func TestLaunchProofOldHubCompatibilityT2803(t *testing.T) {
	result := hubSpawnResult{RC: 70, Error: "launch_proof_failed", Retryable: true}
	if got := spawnResultForHub(result, false); got.Error != "spawn_failed" || !got.Retryable {
		t.Fatalf("old hub result = %+v", got)
	}
	if got := spawnResultForHub(result, true); got.Error != "launch_proof_failed" {
		t.Fatalf("new hub result = %+v", got)
	}
}
