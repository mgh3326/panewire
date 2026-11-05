package fleet

import (
	"context"
	"testing"
)

// The hub hands a first-run refusal back to placement with NextHost set; the
// loop then tries the next candidate instead of ending on rc 90.
func TestHubFirstRunRefusalMovesOnWhenHubHandsItBack(t *testing.T) {
	for _, code := range []string{"spawn_failed:hooks_review", "spawn_failed:trust_prompt", "spawn_failed:omp_setup_wizard", "spawn_failed:auth_unavailable"} {
		t.Run(code, func(t *testing.T) {
			a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
				if r.Machine == "h1" {
					return HubSpawnReply{RC: exitTrustPrompt, Error: code, NextHost: true}, nil
				}
				return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
			}
			_, rec, e := a.placeAuto(context.Background(), s, "brief")
			if e != nil || rec["chose"] != "h2" || len(hub.spawns) != 2 {
				t.Fatalf("rec=%v spawns=%d err=%v", rec, len(hub.spawns), e)
			}
		})
	}
}

// Without NextHost the same rc 90 is the request's answer, as before.
func TestHubTrustPromptRefusalStopsWithoutNextHost(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RC: exitTrustPrompt, Error: "spawn_failed", StdoutTail: "stderr: HOOKS_REVIEW_REQUIRED"}, nil
	}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil || exitCode(e) != exitTrustPrompt || len(hub.spawns) != 1 {
		t.Fatalf("spawns=%d err=%v", len(hub.spawns), e)
	}
}
