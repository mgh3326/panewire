package fleet

import (
	"context"
	"testing"
)

// (i) A gate failure is scoped to the host it failed on. The waste account
// fails its gate on mac-b and the walk moves on, so the live choice is the
// ranked account on cloud-small. The same account also sits on cloud-tiny,
// which the walk never reached: there it is still admissible, and the
// pool-first order picks it.
func TestWasteFirstHostFailureHoldsOnItsHost_T3651(t *testing.T) {
	a, s, q := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB, "cloud-tiny": testClaudeA}, threeHostReadings())
	gate := q.GateDo
	failed := false
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testClaudeA && !failed {
			failed = true
			return "", fail(exitUnreachable, "host unreachable")
		}
		return gate(r)
	}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testClaudeB || sel.chosen.host != "cloud-small" {
		t.Fatalf("live must choose the ranked account on cloud-small: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Outcome != "differs" || w.Choice == nil || w.Choice.AccountKey != testClaudeA || w.Choice.Host != "cloud-tiny" {
		t.Fatalf("a host failure on mac-b must not refuse the account on cloud-tiny: %+v", w)
	}
}

// (ii) A cached account-wide exhaustion holds on every host the account is on.
// The waste account is exhausted on mac-b; the walk then chooses the ranked
// account on cloud-small without reaching cloud-tiny. The account is still
// exhausted there, so the arm must not pick it: it agrees with the live choice.
func TestWasteFirstExhaustionHoldsOnEveryHost_T3651(t *testing.T) {
	a, s, q := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB, "cloud-tiny": testClaudeA}, threeHostReadings())
	// A second rung of the same profile on the same accounts: the exhaustion
	// refusal reaches it only through the account-wide cache, not refusedOn.
	q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "sonnet", Effort: "medium", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "sonnet-m2"})
	gate := q.GateDo
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testClaudeA {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testClaudeB {
		t.Fatalf("live must choose the ranked account: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Outcome != "agree" || w.Choice == nil || w.Choice.AccountKey != testClaudeB {
		t.Fatalf("the exhausted account must not be chosen on cloud-tiny: %+v", w)
	}
}
