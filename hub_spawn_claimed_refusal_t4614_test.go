package panewire

import (
	"testing"
	"time"
)

// A reviewer spawn was refused rc 74 "job id already claimed" on a host that
// never saw the job (hk 4614). The first host launched a pane, hit the agy
// login screen and answered rc 88 after the node's hk claim; hk holds the job
// id for that host, so the next candidate's claim is a 409 job_id_taken. A
// refusal past the claim must end the operation on the host that made it.
const t4614AuthTail = "stderr: AUTH_UNAVAILABLE: selected quota account login missing\nFix: sign in"

func t4614Refusal(rc int, stage, tail string) hubSpawnResult {
	return hubSpawnResult{RC: rc, Error: "spawn_failed", Stage: stage, StdoutTail: tail, Retryable: true}
}

func TestRefusalAfterJobClaimDoesNotMoveToNextHost_t4614(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result hubSpawnResult
		want   bool
	}{
		{"login screen after launch", t4614Refusal(88, "landing", t4614AuthTail), false},
		{"login screen after claim", t4614Refusal(88, "claim", t4614AuthTail), false},
		{"trust prompt after launch", t4614Refusal(90, "launch", "stderr: WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt"), false},
		{"launch proof after launch", hubSpawnResult{RC: 70, Error: "launch_proof_failed", Stage: "launch", Retryable: true}, false},
		{"unknown stage counts as claimed", t4614Refusal(88, "future-stage", t4614AuthTail), false},
		{"account refused at placement", t4614Refusal(88, "placement", t4614AuthTail), true},
		{"unmapped repo answers no stage", hubSpawnResult{RC: 2, Error: "cwd_unmapped", Retryable: true}, true},
		{"checkout refused before claim", hubSpawnResult{RC: 81, Error: "checkout_refused:dirty", Stage: "checkout", Retryable: true}, true},
		{"never started", hubSpawnResult{RC: 124, Error: "deadline_not_started", Stage: "claim", Retryable: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			node := hubSpawnRequest{RequestID: t14RequestID(4614), Machine: "machine-a", Args: []string{"-m", "agy-flash38-medium"}}
			task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			task = sentSpawnFixture(t, h.inbox.store, task)
			transport := &directSpawnTransport{h: h, task: task, bound: true}
			if got, err := transport.nextHostAfterRefusal(t.Context(), node, tc.result); err != nil || got != tc.want {
				t.Fatalf("moves on = %v, err=%v, want %v (reason %q)", got, err, tc.want, spawnResultReason(tc.result))
			}
		})
	}
}

// A saved refusal replays where it was refused when it left a claim there; one
// that left none places again.
func TestReplayPinsHostOnlyAfterJobClaim_t4614(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome hubSpawnOutcome
		want    bool
	}{
		{"login screen after launch", hubSpawnOutcome{Retryable: true, Host: "mac-personal", Stage: "landing", Reason: "spawn_failed:auth_unavailable"}, true},
		{"account refused at placement", hubSpawnOutcome{Retryable: true, Host: "mac-personal", Stage: "placement", Reason: "spawn_failed:auth_unavailable"}, false},
		{"unmapped repo", hubSpawnOutcome{Retryable: true, Host: "mac-personal", Stage: "placement", Reason: "cwd_unmapped"}, false},
		{"cleanup retry of another refusal", hubSpawnOutcome{Retryable: true, Host: "mac-personal", Stage: "landing", Reason: "spawn_failed"}, true},
		{"not retryable", hubSpawnOutcome{Host: "mac-personal", Stage: "landing", Reason: "spawn_failed:auth_unavailable"}, false},
		{"no host", hubSpawnOutcome{Retryable: true, Stage: "landing", Reason: "spawn_failed:auth_unavailable"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := spawnReplayPinsHost(tc.outcome); got != tc.want {
				t.Fatalf("pins = %v, want %v", got, tc.want)
			}
		})
	}
}
