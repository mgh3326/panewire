package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

func TestQuotaFullRefusalReadsOnlyFullQuotaReasons_T5358(t *testing.T) {
	reset := "2026-11-19T03:34:12Z"
	for _, test := range []struct {
		text  string
		ok    bool
		free  bool
		reset string
	}{
		{"quota gate: free limit exceeded until " + reset, true, true, reset},
		{"stderr: quota gate: free limit exceeded until " + reset + "\nFix: x", true, true, reset},
		{"quota gate: quota exhausted window=5h cutoff=90", true, false, ""},
		{"quota gate: credit_exhausted balance=0", true, false, ""},
		{"quota gate: free limit exceeded until not-a-time", true, true, ""},
		// Not a full quota: an unmeasured reading, a request or policy refusal.
		{"quota gate: measurement unavailable", false, false, ""},
		{"quota gate: shared free bucket state unavailable", false, false, ""},
		{"quota gate: pool excluded by policy", false, false, ""},
		{"quota gate: e6_arm_required: rung is not placed", false, false, ""},
		{"free limit exceeded until " + reset, false, false, ""},
	} {
		got, ok := QuotaFullRefusal(test.text)
		if ok != test.ok || got.Free != test.free || (test.reset == "") != got.Reset.IsZero() || test.reset != "" && got.Reset.Format(time.RFC3339) != test.reset {
			t.Errorf("%q: got %+v ok=%v, want ok=%v free=%v reset=%q", test.text, got, ok, test.ok, test.free, test.reset)
		}
	}
}

func TestQuotaFullRefusalHasItsOwnCode_T5358(t *testing.T) {
	for _, text := range []string{"quota gate: free limit exceeded until 2026-11-19T03:34:12Z", "quota gate: quota exhausted window=7d cutoff=90", "quota gate: credit_exhausted balance=0"} {
		if got := SpawnRefusalCode(text, quotaGateBlocked); got != "quota_full" {
			t.Errorf("%q: code %q, want quota_full", text, got)
		}
		if fix := spawnRefusalFix(fail(quotaGateBlocked, "%s", text)); !strings.Contains(fix, "next candidate") {
			t.Errorf("%q: fix %q does not say the hub moves on", text, fix)
		}
	}
	if got := SpawnRefusalCode("quota gate: measurement unavailable", quotaGateBlocked); got == "quota_full" {
		t.Errorf("an unmeasured reading is not a full quota")
	}
}

// Every host of an automatic placement refuses on a full quota: the request is
// refused only then, and the refusal names every candidate and why it was
// dropped (hk 5358).
func TestEveryCandidateFullRefusesNamingEachOne_T5358(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	hub.node = func(r HubSpawnRequest) (HubSpawnReply, error) {
		return HubSpawnReply{RC: quotaGateBlocked, Error: "spawn_failed:quota_full", NextHost: true}, nil
	}
	_, _, err := a.placeAuto(context.Background(), s, "brief")
	if err == nil || len(hub.spawns) != 2 {
		t.Fatalf("spawns=%d err=%v", len(hub.spawns), err)
	}
	for _, want := range []string{"h1", "h2", "hub_quota_full"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q lacks %q", err, want)
		}
	}
	// A refusal that is not handed back keeps ending the request on the first host.
}

// The refusal's reset becomes the account's shared reading at once: the node
// that was refused records the limit and publishes it, so the next selection
// of any host skips the free band until the reset (hk 5358, docs/quota-stop.md).
func TestFreeLimitRefusalIsRecordedAndPublishedUntilItsReset_T5358(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
	}
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})
	reset := a.Now().Add(2 * time.Minute)
	cfg := devin.Config{Home: a.Home, Now: func() time.Time { return a.Now() }}
	published := false
	src := a.Source.(*fakePlacementSource)
	base := src.quotaDo
	src.quotaDo = func(key string) *PoolView {
		v := base(key)
		if v == nil || !strings.HasPrefix(key, "devin:") {
			return v
		}
		v.Free = &shared.FreeBucket{State: shared.FreeAvailable}
		if published {
			v.Free = devin.LocalFree(cfg)(a.Now())
		}
		return v
	}
	fr := a.Runner.(*fakeRunner)
	fr.Do = func(c Command) Result {
		if c.Name == "panewire" && strings.Join(c.Args, " ") == "quota refresh devin" {
			published = true
		}
		return Result{}
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("before the refusal the free rung is admitted: %v %v", sel.chosen, err)
	}

	// Not a free limit, or no reset ahead: nothing is recorded.
	a.recordRefusedFreeLimit(context.Background(), "job-a", fail(quotaGateBlocked, "quota gate: quota exhausted window=5h cutoff=90"))
	a.recordRefusedFreeLimit(context.Background(), "job-a", fail(quotaGateBlocked, "quota gate: free limit exceeded until %s", a.Now().Add(-time.Minute).Format(time.RFC3339)))
	if published {
		t.Fatal("a refusal that names no limit ahead published a free limit")
	}

	a.recordRefusedFreeLimit(context.Background(), "job-a", fail(quotaGateBlocked, "quota gate: free limit exceeded until %s", reset.UTC().Format(time.RFC3339)))
	if !published {
		t.Fatal("the refusal's reset was not published into the account's shared reading")
	}
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:free_limit" || sel.chosen.seatName != "sonnet" {
		t.Fatalf("next selection must skip the free band: outcome=%v chosen=%s", got, sel.chosen.seatName)
	}
	if err := a.Sleep(context.Background(), 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	if sel, err = a.selectProfile(context.Background(), s); err != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("after the reset the free rung returns: %v %v", sel.chosen, err)
	}
}
