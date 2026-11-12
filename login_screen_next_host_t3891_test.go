package panewire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

const t3891AgyLoginScreen = `Antigravity CLI
Open the URL below in your browser:
https://accounts.google.com/o/oauth2/auth?client_id=x
After authenticating, copy the code displayed in the browser and paste it below:
>`

const (
	t3891AuthUnavailableRC   = 88
	t3891AuthUnavailableHint = "Sign in to the selected quota account on the executing host and refresh that pool with panewire quota refresh POOL, then retry."
)

func t3891RunHubSpawn(t *testing.T, profile string, retryable bool, capture string) (hubSpawnResult, *HubClient, string) {
	t.Helper()
	client := quotaClient(t)
	client.spawnConfigPath = t14SpawnConfig(t, t.TempDir())
	client.jobsInboxRoot = t.TempDir()
	capturePath := ""
	if capture != "" {
		dir := filepath.Join(client.jobsInboxRoot, "jobs", "job-3891")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		capturePath = filepath.Join(dir, "spawn-failure-123.txt")
		if err := os.WriteFile(capturePath, []byte(capture), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		output := &hubSpawnOutput{}
		_, _ = output.Write([]byte("BRIEF_SUBMISSION_UNCONFIRMED: no working transition\n"))
		output.jobID = "job-3891"
		return output, fleet.Execution{RC: fleet.ExitSubmitUnconfirmed, Stage: "launch", Retryable: retryable}
	}
	result := client.runHubSpawn(t.Context(), hubOutboundMessage{
		RequestID: t14RequestID(3891), CWDKey: "repo-a", BriefInline: "brief", WaitSeconds: 1,
		Args: []string{"--job", "job-3891", "-m", profile},
	})
	return result, client, capturePath
}

func TestLoginScreenNextHostT3891(t *testing.T) {
	result, client, capturePath := t3891RunHubSpawn(t, "agy-flash38-medium", true, t3891AgyLoginScreen)
	if result.RC != t3891AuthUnavailableRC || spawnResultReason(result) != "spawn_failed:auth_unavailable" || !spawnRefusalTriesNextHost(spawnResultReason(result)) {
		t.Fatalf("sent result=%+v reason=%q", result, spawnResultReason(result))
	}
	if code := fleet.SpawnRefusalCode(spawnResultRefusal(result.StdoutTail), result.RC); code != "auth_unavailable" {
		t.Fatalf("hub-classified code=%q tail=%q", code, result.StdoutTail)
	}
	if strings.Contains(result.StdoutTail, "BRIEF_SUBMISSION_UNCONFIRMED") || !strings.Contains(result.StdoutTail, "Fix: "+t3891AuthUnavailableHint) {
		t.Fatalf("translated tail retained the old refusal: %q", result.StdoutTail)
	}
	if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginMissing {
		t.Fatalf("node did not mark the original login-screen result: %q", got)
	}
	if capture, err := os.ReadFile(capturePath); err != nil || string(capture) != t3891AgyLoginScreen {
		t.Fatalf("saved capture changed: err=%v", err)
	}
}

func TestLoginScreenNextHostT3891Boundaries(t *testing.T) {
	for _, tc := range []struct {
		name, profile, capture string
		retryable              bool
	}{
		{"cleanup unverified", "agy-flash38-medium", t3891AgyLoginScreen, false},
		{"composer screen", "agy-flash38-medium", agyComposerScreen, true},
		{"half screen", "agy-flash38-medium", "Open the URL below in your browser:\n", true},
		{"no capture", "agy-flash38-medium", "", true},
		{"other harness", "opus", t3891AgyLoginScreen, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _, _ := t3891RunHubSpawn(t, tc.profile, tc.retryable, tc.capture)
			if got.RC != fleet.ExitSubmitUnconfirmed || fleet.SpawnRefusalCode(spawnResultRefusal(got.StdoutTail), got.RC) != "submit_unconfirmed" {
				t.Fatalf("boundary result=%+v code=%q", got, fleet.SpawnRefusalCode(spawnResultRefusal(got.StdoutTail), got.RC))
			}
		})
	}
}
