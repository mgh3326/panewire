package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// A provider-limit stop keeps the rung it spent out of new placements until its
// reset, from the stop itself (hk 5562): the Devin free limit is recorded on the
// host and published into the account's shared reading at once. The stop is not
// a job end here: the quota-stop manager calls this when it sees the stop, whatever
// it then decides.
func TestKeepLimitedRungOutRecordsAndPublishesAFreeLimit_T5562(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	a := testApp(t)
	cfg := devin.Config{Home: a.Home, Now: a.Now}
	var commands []string
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		commands = append(commands, c.Name+" "+strings.Join(c.Args, " "))
		return Result{}
	}
	reset := a.Now().Add(2 * time.Minute)
	a.keepLimitedRungOut(context.Background(), "job-a", quotastop.Stop{Pool: "devin-free", Reset: reset})

	got := devin.LocalFree(cfg)(a.Now())
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(reset) {
		t.Fatalf("free limit after the stop: %+v, want exhausted until %v", got, reset)
	}
	if len(commands) != 1 || commands[0] != "panewire quota refresh devin" {
		t.Fatalf("published through %q, want one panewire quota refresh devin", commands)
	}
	// The rung is out only until the reset.
	if err := a.Sleep(context.Background(), 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := devin.LocalFree(cfg)(a.Now()); got.State != shared.FreeAvailable {
		t.Fatalf("free limit after its reset: %+v", got)
	}
}

// Another pool's stop is not a Devin free limit: nothing is recorded or published.
func TestKeepLimitedRungOutLeavesOtherPoolsAlone_T5562(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	a := testApp(t)
	cfg := devin.Config{Home: a.Home, Now: a.Now}
	ran := 0
	a.Runner.(*fakeRunner).Do = func(Command) Result { ran++; return Result{} }
	for _, pool := range []string{"claude", "codex", "devin"} {
		a.keepLimitedRungOut(context.Background(), "job-a", quotastop.Stop{Pool: pool, Reset: a.Now().Add(time.Hour)})
	}
	if got := devin.LocalFree(cfg)(a.Now()); got.State != shared.FreeAvailable || ran != 0 {
		t.Fatalf("another pool's stop touched the free limit: %+v, %d commands", got, ran)
	}
}
