package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCodexLaunchProofHasTypedReasonT2803(t *testing.T) {
	a, s, _, _ := newCodexHooksFixture(t, "Codex banner without a footer")
	err := a.Spawn(context.Background(), s)
	var proof interface{ LaunchFailureClass() string }
	if code(err) != exitConfig || !errors.As(err, &proof) || proof.LaunchFailureClass() != "harness_version" {
		t.Fatalf("footer failure should carry harness_version with rc 70: %v", err)
	}
}

func TestLaunchProofTriesAnotherHarnessOnSameHostT2803(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.autoSelected = true
	s.Profile = "codex-sol"
	s.Effort = "high"
	s.T = "T1"
	s.SelHost = "h1"
	src := a.Source.(*fakePlacementSource)
	now := a.Now()
	src.accounts["claude:0123456789abcdef"] = SharedAccount{Key: "claude:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"}}
	q.PoolViews = map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "claude"})
	useFakeHK(t).SetTask(s.Task, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
	var comments []string
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		if status, body, ok := hkCommentAnswer(r); ok {
			comments = append(comments, string(r.Body))
			return status, body, ok
		}
		return 0, "", false
	})
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if len(hub.spawns) == 1 {
			return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
		}
		return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
	}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || len(hub.spawns) != 2 || hub.spawns[0].Machine != "h1" || hub.spawns[1].Machine != "h1" || indexArg(hub.spawns[1].Args, "-m") < 0 || hub.spawns[1].Args[indexArg(hub.spawns[1].Args, "-m")+1] != "sonnet" {
		t.Fatalf("expected alternate harness on h1: spawns=%+v rec=%v err=%v", hub.spawns, rec, err)
	}
	selection, _ := rec["selection"].(map[string]any)
	if len(comments) != 1 || !strings.Contains(comments[0], "from-profile=codex-sol") || !strings.Contains(comments[0], "to-profile=sonnet") || selection["launch_fallbacks"] == nil {
		t.Fatalf("fallback evidence missing: comments=%v selection=%v", comments, selection)
	}
}

func TestLaunchProofTriesNextHostWhenNoAlternateT2803(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.autoSelected = true
	s.T = "T1"
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if r.Machine == "h1" {
			return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
		}
		return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
	}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || len(hub.spawns) != 2 || hub.spawns[1].Machine != "h2" {
		t.Fatalf("expected next host: spawns=%+v rec=%v err=%v", hub.spawns, rec, err)
	}
	selection, _ := rec["selection"].(map[string]any)
	if selection["launch_fallbacks"] == nil {
		t.Fatalf("fallback missing from record: %v", rec)
	}
}

func TestLaunchProofPreservesPinnedAndDelegatedProfileT2803(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delegated bool
	}{{"pinned", false}, {"delegated", true}} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
			s.Profile, s.Effort, s.T, s.SelHost, s.Delegated = "codex-sol", "high", "T1", "h1", tc.delegated
			s.autoSelected = tc.delegated
			src := a.Source.(*fakePlacementSource)
			src.accounts["claude:0123456789abcdef"] = SharedAccount{Key: "claude:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}
			q := a.Quota.(*fakeQuota)
			q.CatalogRows = []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"}}
			q.PoolViews = map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}
			q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "claude"})
			useFakeHK(t).SetTask(s.Task, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
			hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
				if r.Machine == "h1" {
					return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
				}
				return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
			}
			_, _, err := a.placeAuto(t.Context(), s, "brief")
			if err != nil || len(hub.spawns) != 2 || hub.spawns[1].Machine != "h2" {
				t.Fatalf("pinned profile changed or placement failed: spawns=%+v err=%v", hub.spawns, err)
			}
			i := indexArg(hub.spawns[1].Args, "-m")
			if i < 0 || hub.spawns[1].Args[i+1] != "codex-sol" {
				t.Fatalf("profile changed: %+v", hub.spawns[1])
			}
		})
	}
}
