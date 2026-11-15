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
	var comments []string
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		if status, body, ok := hkCommentAnswer(r); ok {
			comments = append(comments, string(r.Body))
			return status, body, ok
		}
		return 0, "", false
	})
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
	if i := indexArg(hub.spawns[1].Args, "-m"); i < 0 || hub.spawns[1].Args[i+1] != "sonnet" {
		t.Fatalf("the next host must retry the same profile: %+v", hub.spawns[1])
	}
	selection, _ := rec["selection"].(map[string]any)
	if len(comments) != 0 || selection["launch_fallbacks"] != nil {
		t.Fatalf("a fallback that never happened must not be recorded: comments=%v selection=%v", comments, selection)
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

// splitHarnessHosts gives h1 the codex family's account only and h2 both, so a
// claude-family seat is only ever placeable on h2.
func splitHarnessHosts(a *App) *fakeQuota {
	src := a.Source.(*fakePlacementSource)
	now := a.Now()
	ok := &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}
	src.accounts["openai:0123456789abcdef"] = SharedAccount{Key: "openai:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.accounts["claude:0123456789abcdef"] = SharedAccount{Key: "claude:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.presenceDo = func() []PresenceRecord {
		return []PresenceRecord{
			{HostID: "h1", AccountKey: "openai:0123456789abcdef", LastSeen: now},
			{HostID: "h2", AccountKey: "openai:0123456789abcdef", LastSeen: now},
			{HostID: "h2", AccountKey: "claude:0123456789abcdef", LastSeen: now},
		}
	}
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Pool: "openai"}
	q.CatalogRows = []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "openai", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	q.PoolViews = map[string]PoolView{"openai": *ok, "claude": *ok}
	q.GateDo = gatePools(map[string]string{"codex-sol": "openai", "sonnet": "claude"})
	return q
}

// codexSpawn reports whether a hub spawn asks for the codex seat.
func codexSpawn(r HubSpawnRequest) bool {
	i := indexArg(r.Args, "-m")
	return i >= 0 && r.Args[i+1] == "codex-sol"
}

// A harness kind that failed on h1 must not stop h2 from offering a different
// kind: the alternate is chosen on each host (hk 4335).
func TestLaunchProofTriesAlternateOnALaterHostT4335(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	s.autoSelected = true
	s.Profile, s.Effort, s.T = "codex-sol", "high", "T1"
	splitHarnessHosts(a)
	useFakeHK(t).SetTask(s.Task, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if codexSpawn(r) {
			return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
		}
		return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
	}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil {
		t.Fatalf("placement died although h2 holds an alternate seat: %v", err)
	}
	var got []string
	for _, sp := range hub.spawns {
		i := indexArg(sp.Args, "-m")
		got = append(got, sp.Machine+":"+sp.Args[i+1])
	}
	if want := "h1:codex-sol h2:codex-sol h2:sonnet"; strings.Join(got, " ") != want {
		t.Fatalf("expected %s, got %v (record %v)", want, got, rec)
	}
}

// A remote fallback never carries to the local seat: Spawn resolves the local
// one from the pre-placement selection, so admission and the record must judge
// and name that seat (hk 4335).
func TestLaunchProofLocalCandidateKeepsPlacementSeatT4335(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", map[string]map[string]any{"h1": {"via": "hub"}})
	a.Config.HostOrder = []string{"h1"}
	a.Config.Local = map[string]any{"spawn": true}
	s.autoSelected = true
	s.Profile, s.Effort, s.T = "codex-sol", "high", "T1"
	src := a.Source.(*fakePlacementSource)
	now := a.Now()
	ok := &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}
	src.accounts["openai:0123456789abcdef"] = SharedAccount{Key: "openai:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.accounts["claude:0123456789abcdef"] = SharedAccount{Key: "claude:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.presenceDo = func() []PresenceRecord {
		return []PresenceRecord{
			{HostID: "h1", AccountKey: "openai:0123456789abcdef", LastSeen: now},
			{HostID: "h1", AccountKey: "claude:0123456789abcdef", LastSeen: now},
		}
	}
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Pool: "openai"}
	q.CatalogRows = []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "openai", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	q.PoolViews = map[string]PoolView{"openai": *ok, "claude": *ok}
	q.GateDo = gatePools(map[string]string{"codex-sol": "openai", "sonnet": "claude"})
	useFakeHK(t).SetTask(s.Task, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if codexSpawn(r) {
			return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
		}
		return HubSpawnReply{RC: 77, Error: "quota role-denied"}, nil
	}
	handled, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || handled {
		t.Fatalf("expected a local placement: handled=%v rec=%v err=%v", handled, rec, err)
	}
	if rec["profile"] != "codex-sol" || rec["pool"] != "openai" {
		t.Fatalf("record names a seat the local spawn will not run: %v", rec)
	}
	selection, _ := rec["selection"].(map[string]any)
	if selection["launch_fallbacks"] != nil {
		t.Fatalf("local record claims a remote fallback: %v", selection)
	}
	var launched []string
	for _, r := range q.Calls {
		launched = append(launched, r.Profile)
	}
	if len(q.Calls) == 0 || q.Calls[len(q.Calls)-1].Profile != "codex-sol" {
		t.Fatalf("local admission judged the wrong seat: %v", launched)
	}
}

// A second seat of the failed kind is not an alternate: the selection excludes
// the kind it just recorded, so one host cannot cycle between same-kind seats
// (hk 4335).
func TestLaunchProofAlternateIsADifferentKindT4335(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", map[string]map[string]any{"h1": {"via": "hub"}})
	a.Config.HostOrder = []string{"h1"}
	s.autoSelected = true
	s.Profile, s.Effort, s.T = "codex-sol", "high", "T1"
	src := a.Source.(*fakePlacementSource)
	now := a.Now()
	ok := &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}
	src.accounts["openai:0123456789abcdef"] = SharedAccount{Key: "openai:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.accounts["claude:0123456789abcdef"] = SharedAccount{Key: "claude:0123456789abcdef", Observed: true, Fresh: true, MeasuredAt: now, Quota: ok}
	src.presenceDo = func() []PresenceRecord {
		return []PresenceRecord{
			{HostID: "h1", AccountKey: "openai:0123456789abcdef", LastSeen: now},
			{HostID: "h1", AccountKey: "claude:0123456789abcdef", LastSeen: now},
		}
	}
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Pool: "openai"}
	// Two codex-kind seats and one claude-kind seat, all on h1.
	q.CatalogRows = []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "openai", Gate: "default", ModelID: "m"},
		{Profile: "codex-astra", Effort: "high", Grade: "B", Pool: "openai", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	q.PoolViews = map[string]PoolView{"openai": *ok, "claude": *ok}
	q.GateDo = gatePools(map[string]string{"codex-sol": "openai", "codex-astra": "openai", "sonnet": "claude"})
	useFakeHK(t).SetTask(s.Task, `{"id":42,"title":"work (grade B, T1)","state":"ready","refs":{}}`)
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		if codexSpawn(r) {
			return HubSpawnReply{RC: 70, Error: "launch_proof_failed", NextHost: true}, nil
		}
		return HubSpawnReply{Pane: "w:p1", JobID: "job-a"}, nil
	}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || len(hub.spawns) != 2 {
		t.Fatalf("expected one retry on a different harness kind: spawns=%d rec=%v err=%v", len(hub.spawns), rec, err)
	}
	i := indexArg(hub.spawns[1].Args, "-m")
	if i < 0 || hub.spawns[1].Args[i+1] != "sonnet" {
		t.Fatalf("the alternate must be a different harness kind, got %+v", hub.spawns[1])
	}
}
