package fleet

import (
	"strings"
	"testing"
)

// hk 3532: a credit rung is decided by its catalog pool. omp-dsflash41 draws
// on the commandcode pool, so it is a credit rung without a name-list entry.
func omp3532Inputs() admissionInputs {
	return admissionInputs{Now: switchNow, AccountScoped: true, Rows: []admissionRow{
		{Profile: "omp-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true},
	}}
}

func TestCreditRungIsDecidedByPool(t *testing.T) {
	if !admissionCredit("omp-dsflash41", "commandcode") {
		t.Fatal("omp-dsflash41 on the commandcode pool must be a credit rung")
	}
	if admissionCredit("omp-dsflash41", "openai") {
		t.Fatal("a profile on a non-commandcode pool must not be a credit rung by name")
	}
	if !admissionCredit("cmd-dsflash41", "commandcode") {
		t.Fatal("cmd-dsflash41 on the commandcode pool must stay a credit rung")
	}
	if admissionCredit("cmd-dsflash41", "claude") {
		t.Fatal("cmd-dsflash41 is a credit rung only through its pool, not its name")
	}
	if !admissionCredit("pi-example", "") {
		t.Fatal("the pi- prefix rule still marks a credit rung")
	}
}

// A caller that holds only a profile finds the credit rule through the
// profile's own rows.
func TestProfileCreditLooksUpRows(t *testing.T) {
	in := omp3532Inputs()
	if !in.profileCredit("omp-dsflash41") {
		t.Fatal("profile-only lookup must see omp-dsflash41 as credit")
	}
	if in.profileCredit("no-such-profile") {
		t.Fatal("an unknown profile is not a credit rung")
	}
}

func TestOmpDsflash41CreditRequiresExplicitPlacedEffort(t *testing.T) {
	in := omp3532Inputs()
	if _, _, _, e := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Role: "implementer"}, false); e == nil || !strings.Contains(e.msg, "explicit --effort required for credit profile") {
		t.Fatalf("credit rung without effort must demand one: %+v", e)
	}
	if _, _, _, e := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "medium", Role: "implementer"}, false); e == nil || !strings.Contains(e.msg, "credit effort is not placed") {
		t.Fatalf("credit rung at an unplaced effort must be refused: %+v", e)
	}
	row, resolved, _, e := in.resolveRow(admissionRequest{Profile: "omp-dsflash41", Effort: "high", Role: "implementer"}, false)
	if e != nil || resolved != "high" || row.Pool != "commandcode" {
		t.Fatalf("omp-dsflash41@high: row=%+v resolved=%q err=%+v", row, resolved, e)
	}
}

// A credit rung ranks at priority 4 among the alternatives, as cmd-dsflash41
// does. The name-list mutant leaves omp-dsflash41 at its non-credit priority.
func TestOmpDsflash41RankedAsCreditAlternative(t *testing.T) {
	in := omp3532Inputs()
	in.Rows = append(in.Rows, admissionRow{Profile: "cmd-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true})
	var omp, cmd *admissionAlternative
	alts := in.recommend("A+", "implementer")
	for i := range alts {
		switch alts[i].Profile {
		case "omp-dsflash41":
			omp = &alts[i]
		case "cmd-dsflash41":
			cmd = &alts[i]
		}
	}
	if omp == nil || cmd == nil {
		t.Fatalf("both commandcode rungs must be alternatives: %+v", alts)
	}
	if omp.priority != 4 {
		t.Fatalf("omp-dsflash41 priority %d, want the credit rank 4", omp.priority)
	}
	if cmd.priority != omp.priority {
		t.Fatalf("cmd-dsflash41 priority %d must match omp-dsflash41's %d (same pool)", cmd.priority, omp.priority)
	}
}

// The credit role rule applies to omp-dsflash41 as to any credit rung: the
// reviewer role it gained in hk 3532 is refused at admission (docs/quota.md).
func TestOmpDsflash41CreditRoleRule(t *testing.T) {
	row := admissionRow{Profile: "omp-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true}
	for _, role := range []string{"implementer"} {
		if d := admissionRoleCheck(admissionRequest{Profile: "omp-dsflash41", Role: role}, row); d != nil {
			t.Fatalf("credit rung must seat %s: %s", role, d.msg)
		}
	}
	if d := admissionRoleCheck(admissionRequest{Profile: "omp-dsflash41", Role: "reviewer"}, row); d == nil {
		t.Fatal("credit rung must not seat a reviewer")
	}
}

// cmd-dsflash41 keeps its implementer-only rule and its credit ranking.
func TestCmdDsflash41CreditUnchanged(t *testing.T) {
	row := admissionRow{Profile: "cmd-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true}
	if d := admissionRoleCheck(admissionRequest{Profile: "cmd-dsflash41", Role: "builder"}, row); d == nil {
		t.Fatal("cmd-dsflash41 must still refuse a builder")
	}
	if d := admissionRoleCheck(admissionRequest{Profile: "cmd-dsflash41", Role: "implementer"}, row); d != nil {
		t.Fatalf("cmd-dsflash41 worker: %s", d.msg)
	}
}
