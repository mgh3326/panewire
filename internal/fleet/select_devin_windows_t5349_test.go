package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// hk 5349: Devin's two bands draw on different quotas. The paid rung
// (devin-ds41) spends the account's weekly window; the SWE-2 free rungs spend
// the free-model bucket (devinFreeRungs, docs/config.md "free_limit"). So a
// fresh 100 % weekly reading refuses the paid rung alone, and a free-limit stop
// (the free bucket exhausted until its reset) refuses the free rungs alone.
// devinBandGate answers as the real admission does: a SWE-2 free rung is
// admitted on the free bucket (free-bucket basis), any other rung on its pool.
func devinBandGate(r QuotaRequest) (string, error) {
	name := firstNonempty(r.GateProfile, r.Profile)
	if IsFreeRung(name) {
		return "pool=devin profile=" + name + " gate_basis=free-bucket:" + name + "\n", nil
	}
	return gatePools(map[string]string{"devin-ds41": "devin", "sonnet": "claude"})(r)
}

func TestSelectDevinWeeklyWindowRefusesPaidRungNotFreeRung_T5349(t *testing.T) {
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(100), Projected: ptrF(100)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", devinBandRows(), pools, []string{"devin", "claude"})
	q.GateDo = devinBandGate
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; !strings.HasPrefix(got.(string), "dropped:window_exhausted") {
		t.Fatalf("a fresh 100%% weekly window must refuse the paid rung: %v", got)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; strings.HasPrefix(got.(string), "dropped:") {
		t.Fatalf("a fresh 100%% weekly window is the paid rung's, not the free rung's: %v", got)
	}
}

func TestSelectDevinFreeLimitRefusesFreeRungNotPaidRung_T5349(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4), Free: &shared.FreeBucket{State: shared.FreeExhausted, ResetsAt: &reset}},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", devinBandRows(), pools, []string{"devin", "claude"})
	q.GateDo = devinBandGate
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:free_limit" {
		t.Fatalf("an exhausted free bucket must refuse the free rung: %v", got)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; strings.HasPrefix(got.(string), "dropped:") {
		t.Fatalf("an exhausted free bucket must leave the paid rung eligible: %v", got)
	}
}
