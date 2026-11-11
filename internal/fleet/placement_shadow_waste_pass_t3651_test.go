package fleet

import (
	"context"
	"testing"
	"time"
)

// (h) The T2997 shape: a paced subscription on mac-b, which holds no credit,
// and expiring credit on cloud-small. The live walk admits the paced pool in
// pass 1 on mac-b, where its behindCredit is false. The selection-wide flag is
// true (credit is pending elsewhere), so the order must take each host's own
// pass-1 membership, or the candidate the live walk chose is missing from it.
func TestWasteSnapshotKeepsHostPassOneCandidate_T3651(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), LongProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
	a, s, _, src := creditBandFixture(t, []CatalogRow{creditRow, sonnetRow}, map[string]string{"mac-b": testClaudeA, "cloud-small": testCreditA}, balance(80, 100*time.Hour))
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &paced}
	var order []*selCand
	a.shadowSnapProbe = func(o []*selCand) { order = o }
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testClaudeA || sel.chosen.host != "mac-b" || sel.chosen.behindCredit {
		t.Fatalf("live must admit the paced pool on mac-b in pass 1: %v %+v", err, sel)
	}
	found := false
	for _, c := range order {
		if c.accountKey == testClaudeA && c.row.Profile == "sonnet" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the pass-1 candidate the live walk chose is missing from the waste_first order (%d candidates)", len(order))
	}
	// Pool-first across hosts: credit ranks in the free band, so it leads.
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Outcome != "differs" || w.Choice == nil || w.Choice.AccountKey != testCreditA || w.Choice.Host != "cloud-small" {
		t.Fatalf("waste_first %+v, want the expiring credit on cloud-small", w)
	}
}
