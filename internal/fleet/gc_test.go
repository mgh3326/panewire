package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestGCCommandCleansLeasesForAgentsGoneFromHerdr(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a) // herdr agent list: empty
	seedLedger(t, a, "j")
	later := a.Now().Add(time.Hour)
	a.Now = func() time.Time { return later }
	if e := a.Run(context.Background(), []string{"gc"}); e != nil {
		t.Fatal(e)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("gc left %s %v", st, pools)
	}
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(out, "job=j") || !strings.Contains(out, "orphan-released") {
		t.Fatalf("output %q", out)
	}
}

func TestGCCommandKeepsRecordOfListedAgent(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	f.Do = func(c Command) Result {
		return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"idle"}]}}`}
	}
	seedLedger(t, a, "j")
	later := a.Now().Add(time.Hour)
	a.Now = func() time.Time { return later }
	if e := a.Run(context.Background(), []string{"gc", "--json"}); e != nil {
		t.Fatal(e)
	}
	if st, pools := ledgerState(t, a, "j"); st != "leased" || len(pools) != 1 {
		t.Fatalf("a listed agent lost its lease: %s %v", st, pools)
	}
}

func TestGCCommandRefusesUnknownAgentStatus(t *testing.T) {
	a := testApp(t)
	f := ledgerRunner(t, a)
	f.Do = func(c Command) Result {
		return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"unknown"}]}}`}
	}
	seedLedger(t, a, "j")
	later := a.Now().Add(time.Hour)
	a.Now = func() time.Time { return later }
	if code(a.Run(context.Background(), []string{"gc"})) != 6 {
		t.Fatal("an agent in status unknown must fail closed (exit 6)")
	}
	if _, pools := ledgerState(t, a, "j"); len(pools) != 1 {
		t.Fatal("a refused gc changed state")
	}
}
