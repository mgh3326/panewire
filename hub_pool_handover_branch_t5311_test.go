package panewire

import (
	"strings"
	"testing"
)

// The successor is kept off the old host, whose worktree still holds the
// branch (hk 5311, review 4611190394). With no other host to take it, the
// hand-over is refused before any close, naming branch_in_use.

func TestPoolHandoverRefusesSameHostSuccessorBeforeTheClose(t *testing.T) {
	const job = "branch-held-job"
	f := handoverFixture(t, job, 1, 0)
	f.hub.mu.Lock()
	f.hub.nodes["machine-b"].accepting = false
	f.hub.mu.Unlock()
	result := sweepHandover(t, f, window5h(), handoverReadReply(job))
	if result.action != poolJobNotified || result.why != "branch_in_use" {
		t.Fatalf("sweep=%+v, want the refusal branch_in_use", result)
	}
	if actions, _ := f.hub.inbox.store.activePoolActions(t.Context()); len(actions) != 0 {
		t.Fatalf("a refused hand-over stored a row, so a close could follow: %+v", actions)
	}
	lines := ownerLines(t, f.hub, "pool-exhausted:"+job)
	if len(lines) != 1 || !strings.Contains(lines[0], "branch_in_use") {
		t.Fatalf("owner lines=%q, want one naming branch_in_use", lines)
	}
}

func TestPoolHandoverKeepsTheSuccessorOffTheHeldHost(t *testing.T) {
	const job = "branch-elsewhere-job"
	f := handoverFixture(t, job, 1, 0)
	if result := sweepHandover(t, f, window5h(), handoverReadReply(job)); result.action != poolJobQueued {
		t.Fatalf("sweep=%+v", result)
	}
	args := readOnlyPoolAction(t, f.hub).Data.Handover.Successor.Args
	if got := hubSpawnArg(args, "--exclude-host"); got != "machine-a" {
		t.Fatalf("--exclude-host=%q in %v, want the held host machine-a", got, args)
	}
}

func TestExcludeHostArgKeepsTheReplacedList(t *testing.T) {
	args := excludeHostArg([]string{"-c", "/work/project", "--exclude-host", "machine-c", "--host", "auto"}, "machine-a")
	if got := hubSpawnArg(args, "--exclude-host"); got != "machine-c,machine-a" {
		t.Fatalf("merged list %q, want machine-c,machine-a", got)
	}
	again := excludeHostArg(args, "machine-a")
	if got := hubSpawnArg(again, "--exclude-host"); got != "machine-c,machine-a" {
		t.Fatalf("repeated merge %q, want no duplicate", got)
	}
	if n := strings.Count(strings.Join(again, " "), "--exclude-host"); n != 1 {
		t.Fatalf("%d --exclude-host flags in %v, want one", n, again)
	}
	fresh := excludeHostArg([]string{"-c", "/work/project"}, "machine-a")
	if got := hubSpawnArg(fresh, "--exclude-host"); got != "machine-a" {
		t.Fatalf("fresh list %q, want machine-a", got)
	}
}

func TestOtherHostCanTakeSuccessorNeedsAnAcceptingHost(t *testing.T) {
	f := handoverFixture(t, "other-host-job", 1, 0)
	set := func(connected, accepting bool) {
		t.Helper()
		f.hub.mu.Lock()
		defer f.hub.mu.Unlock()
		record := f.hub.nodes["machine-b"]
		record.accepting = accepting
		if connected {
			record.agent = f.destination
		} else {
			record.agent = nil
		}
	}
	for _, tc := range []struct {
		name                 string
		connected, accepting bool
		want                 bool
	}{
		{"connected and accepting", true, true, true},
		{"connected but not accepting", true, false, false},
		{"accepting but not connected", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(tc.connected, tc.accepting)
			if got := f.hub.otherHostCanTakeSuccessor("machine-a"); got != tc.want {
				t.Fatalf("otherHostCanTakeSuccessor=%v, want %v", got, tc.want)
			}
		})
	}
}
