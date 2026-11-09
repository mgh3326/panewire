package fleet

import (
	"context"
	"testing"
)

// (j) A host-scoped gate failure on mac-b does not hold the account on the
// other usable host, even when the walk goes on to choose another account on
// mac-b. The waste account fails its gate there (a policy failure, which does
// not mark the host down), the walk admits the codex account on mac-b, and the
// same waste account on cloud-tiny is still admissible: the pool-first order
// picks it.
func TestWasteFirstFailureAfterOtherChoiceHoldsOnItsHost_T3651(t *testing.T) {
	a, s, q := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeA, "cloud-tiny": testClaudeA}, threeHostReadings())
	// A codex seat on mac-b is the other choice the walk makes there: a host
	// holds one account per pool, so the second account has to be on another pool.
	src := a.Source.(*fakePlacementSource)
	src.presence = append(src.presence, PresenceRecord{HostID: "mac-b", AccountKey: testCodexA, LastSeen: a.Now()})
	codex := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(30)}
	src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &codex}
	q.CatalogRows = append(q.CatalogRows, codexRow)
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex"})
	gate := q.GateDo
	failed := false
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testClaudeA && !failed {
			failed = true
			return "", fail(99, "policy unavailable")
		}
		return gate(r)
	}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testCodexA || sel.chosen.host != "mac-b" {
		t.Fatalf("live must choose the codex account on mac-b after the failure: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Outcome != "differs" || w.Choice == nil || w.Choice.AccountKey != testClaudeA || w.Choice.Host != "cloud-tiny" {
		t.Fatalf("the failure on mac-b must not hold the account on cloud-tiny: %+v", w)
	}
}
