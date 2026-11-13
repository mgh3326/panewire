package fleet

import (
	"strings"
	"testing"
)

// reapLaneJob reaps a finished job whose lane is registered to laneMachine,
// from a host whose machine id is hostMachine, and returns the lane-removal
// command it ran (empty when none), plus reap's stderr and stdout.
func reapLaneJob(t *testing.T, hostMachine, laneMachine string) (laneRM, stderr, stdout string) {
	t.Helper()
	a := testApp(t)
	a.Config.Local = map[string]any{"machine_id": hostMachine}
	f := ledgerRunner(t, a)
	base := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" && len(c.Args) > 1 && c.Args[0] == "lanes" && c.Args[1] == "rm" {
			laneRM = strings.Join(c.Args, " ")
		}
		return base(c)
	}
	store := a.JobStore.(*fakeJobStore)
	if err := store.Claim(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": "implementer"})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "hub_lane": "lane-j", "hub_lane_machine": laneMachine})
	doneJob(t, a)
	reapJob(t, a)
	return laneRM, a.Err.(interface{ String() string }).String(), a.Out.(interface{ String() string }).String()
}

func TestReapSkipsLaneOwnedByAnotherMachineAndNamesIt(t *testing.T) {
	laneRM, stderr, stdout := reapLaneJob(t, "host-a", "host-b")
	if laneRM != "" {
		t.Fatalf("reap tried to remove another machine's lane: %q", laneRM)
	}
	for _, want := range []string{"lane lane-j retained", "belongs to machine host-b", "this host is host-a"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr lacks %q: %q", want, stderr)
		}
	}
	if strings.Contains(stderr+stdout, "lane_machine_mismatch") || strings.Contains(stderr, "lanes rejected") {
		t.Fatalf("reap reported a hub refusal: %q", stderr)
	}
	if !strings.Contains(stdout, "action=reap") {
		t.Fatalf("job was not reaped: %q", stdout)
	}
}

// The same-machine path keeps the #234 route guard: the hub removes the lane
// only while it still names this machine and pane.
func TestReapRemovesOwnLaneWithRouteGuard(t *testing.T) {
	useFakeHK(t).Handle(hkCommentAnswer)
	laneRM, stderr, _ := reapLaneJob(t, "host-a", "host-a")
	for _, want := range []string{"lanes rm lane-j", "--expect-machine host-a", "--expect-pane w:p1"} {
		if !strings.Contains(laneRM, want) {
			t.Fatalf("lane removal %q lacks %q", laneRM, want)
		}
	}
	if strings.Contains(stderr, "retained") {
		t.Fatalf("unexpected warning: %q", stderr)
	}
}
