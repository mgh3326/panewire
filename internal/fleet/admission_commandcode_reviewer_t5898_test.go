package fleet

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// hk 5898 (operator decision 464): the commandcode pool's rungs seat a reviewer
// and a verifier, still under the pool's pace cutoff. A pi- rung keeps the
// implementer-only rule, and cmd-dsflash41 keeps its own implementer-only rule.

// ccReviewerInputs is one omp-dsflash41 rung at grade A on a commandcode
// reading with a 5h and a 7d window, the 7d window used at used7d percent and
// resetting in a week.
func ccReviewerInputs(now time.Time, used7d float64) admissionInputs {
	reset := now.Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	fiveH := now.Add(3 * time.Hour).UTC().Format(time.RFC3339Nano)
	used5h := 10.0
	return admissionInputs{Now: now, Rows: []admissionRow{
		{Profile: "omp-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A", Gate: defaultGate, Placed: true},
	}, Readings: map[string]admissionReading{"commandcode": {Buckets: []admissionBucket{
		{Window: "5h", Used: &used5h, Reset: fiveH, ScopeKind: "account"},
		{Window: "7d", Used: &used7d, Reset: reset, ScopeKind: "account"},
	}}}}
}

// hk 5898 acceptance 1: an omp-dsflash41 reviewer at grade A is admitted while
// the commandcode pool is under its cutoff of 90, and refused at the cutoff.
func TestCommandcodeReviewerAdmittedUnderCutoffRefusedAtIt_t5898(t *testing.T) {
	now := switchNow
	req := admissionRequest{Profile: "omp-dsflash41", Effort: "high", Role: "reviewer"}

	under := ccReviewerInputs(now, 89).judge(req)
	if !under.OK || under.Exit != gateAdmitted {
		t.Fatalf("reviewer under the cutoff must be admitted: exit=%d reason=%q", under.Exit, under.Reason)
	}

	at := ccReviewerInputs(now, 90).judge(req)
	if at.OK || at.Exit != gateBlocked {
		t.Fatalf("reviewer at the cutoff must be refused: exit=%d reason=%q", at.Exit, at.Reason)
	}
	if !strings.Contains(at.Reason, "quota exhausted window=7d cutoff=90") {
		t.Fatalf("refusal must name the 7d window at cutoff 90: %q", at.Reason)
	}
}

// hk 5898 acceptance 2: a pi- reviewer is still refused with role_restricted.
func TestPiReviewerStillRefusedRoleRestricted_t5898(t *testing.T) {
	in := admissionInputs{Now: switchNow, Rows: []admissionRow{
		{Profile: "pi-example", Effort: "low", Pool: "pi-api", Grade: "A", Gate: defaultGate, Placed: true},
	}}
	d := in.judge(admissionRequest{Profile: "pi-example", Effort: "low", Role: "reviewer"})
	if d.OK || d.Exit != gateRoleDenied {
		t.Fatalf("pi- reviewer must be role denied: exit=%d reason=%q", d.Exit, d.Reason)
	}
	if !strings.HasPrefix(d.Reason, "role_restricted: credit rung requires implementer") {
		t.Fatalf("pi- reviewer reason = %q, want the credit role_restricted refusal", d.Reason)
	}
}

// hk 5898 acceptance 3: plan for a reviewer on an anthropic parent shows the
// commandcode rung as a candidate, not role_restricted.
func TestPlanReviewerShowsCommandcodeCandidate_t5898(t *testing.T) {
	rows := []CatalogRow{{Profile: "omp-dsflash41", Effort: "high", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "m"}}
	sel, err := cutoffReviewer(t, 0, 55.5, rows)
	if err != nil || sel == nil || sel.chosen == nil {
		t.Fatalf("commandcode reviewer must be chosen under the cutoff: err=%v", err)
	}
	if got := sel.chosen.row.Profile; got != "omp-dsflash41" {
		t.Fatalf("chosen %s, want omp-dsflash41", got)
	}
	if row := fmt.Sprint(candOutcome(sel.record(), "omp-dsflash41", "high")); strings.Contains(row, "role_restricted") {
		t.Fatalf("omp-dsflash41 must not be role_restricted for a reviewer: %s", row)
	}
}

// Consult stays refused on the commandcode pool, and cmd-dsflash41 keeps its
// implementer-only rule through the commandcode seat change.
func TestCommandcodeSeatRolesHoldOutsideReviewerAndVerifier_t5898(t *testing.T) {
	omp := admissionRow{Profile: "omp-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A", Gate: defaultGate, Placed: true}
	if d := admissionRoleCheck(admissionRequest{Profile: "omp-dsflash41", Role: "consult"}, omp); d == nil || d.code != gateRoleDenied {
		t.Fatalf("commandcode rung must refuse consult: %+v", d)
	}
	cmd := admissionRow{Profile: "cmd-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true}
	for _, role := range []string{"reviewer", "verifier"} {
		d := admissionRoleCheck(admissionRequest{Profile: "cmd-dsflash41", Role: role}, cmd)
		if d == nil || d.msg != "role_restricted: cmd-dsflash41 seats only an implementer" {
			t.Fatalf("cmd-dsflash41 %s must keep its implementer-only refusal: %+v", role, d)
		}
	}
	if d := admissionRoleCheck(admissionRequest{Profile: "cmd-dsflash41", Role: "implementer"}, cmd); d != nil {
		t.Fatalf("cmd-dsflash41 implementer must seat: %s", d.msg)
	}
}

// The launch path reads the same seat rule as admission (admissionCreditRoleDenial):
// a commandcode reviewer launches, cmd-dsflash41 seats only an implementer.
func TestLaunchRoleCheckSharesCommandcodeSeatRule_t5898(t *testing.T) {
	omp := launchRow{admissionRow: admissionRow{Profile: "omp-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A", Gate: defaultGate, Placed: true}}
	if msg := launchRoleCheck(admissionRequest{Profile: "omp-dsflash41", Role: "reviewer"}, omp); msg != "" {
		t.Fatalf("commandcode reviewer must launch: %s", msg)
	}
	cmd := launchRow{admissionRow: admissionRow{Profile: "cmd-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: defaultGate, Placed: true}}
	if msg := launchRoleCheck(admissionRequest{Profile: "cmd-dsflash41", Role: "reviewer"}, cmd); msg != "role_restricted: cmd-dsflash41 seats only an implementer" {
		t.Fatalf("cmd-dsflash41 reviewer launch refusal = %q", msg)
	}
}
