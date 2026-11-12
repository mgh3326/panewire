package fleet

import (
	"context"
	"strings"
	"testing"
)

func TestHubCheckoutRefusalNextHostContinuesPlacement(t *testing.T) {
	for _, code := range []string{"dirty", "sha-missing", "branch-elsewhere"} {
		t.Run(code, func(t *testing.T) {
			a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
				if r.Machine == "h1" {
					return HubSpawnReply{RC: exitPlacement, Error: "checkout_refused:" + code, NextHost: true}, nil
				}
				return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
			}
			_, rec, err := a.placeAuto(context.Background(), s, "brief")
			if err != nil || rec["chose"] != "h2" || len(hub.spawns) != 2 {
				t.Fatalf("rec=%v spawns=%d err=%v", rec, len(hub.spawns), err)
			}
		})
	}

	// Without NextHost, a checkout refusal remains the request's exit-81 answer.
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RC: exitPlacement, Error: "checkout_refused:dirty"}, nil
	}
	if _, _, err := a.placeAuto(context.Background(), s, "brief"); err == nil || exitCode(err) != exitPlacement || !strings.Contains(err.Error(), "dirty") || len(hub.spawns) != 1 {
		t.Fatalf("without NextHost: spawns=%d err=%v", len(hub.spawns), err)
	}
}

func TestAllCheckoutRefusalsReportTriedHosts(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local = map[string]any{"spawn": false}
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RequestID: r.RequestID, RC: exitPlacement, Error: "checkout_refused:dirty", NextHost: true}, nil
	}
	handled, _, err := a.placeAuto(context.Background(), s, "brief")
	if !handled || err == nil || exitCode(err) != exitLocalDisabled {
		t.Fatalf("handled=%v spawns=%d err=%v", handled, len(hub.spawns), err)
	}
	for _, want := range []string{"local spawns are disabled on this host", "h1 hub_checkout_refused:dirty", "h2 hub_checkout_refused:dirty"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("exhaustion error %q lacks %q", err, want)
		}
	}
	if len(hub.spawns) != 2 {
		t.Fatalf("tried %d candidates, want 2", len(hub.spawns))
	}
}
