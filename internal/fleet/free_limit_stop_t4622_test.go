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

// A free-tier limit seen on a pane must reach the account's shared reading
// from the stop itself, not from the next devin measurement: while the reading
// still says available, every selection admits a free rung that stops on the
// same limit as soon as it starts (hk 4622). The test publishes through the
// command the stop runs and reads the account's bucket from the record, the
// way a host's measurement leaves it in the store.
func TestFreeLimitSeenOnAPaneDropsTheFreeRungsUntilReset_T4622(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
	}
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})

	reset := a.Now().Add(90 * time.Minute)
	cfg := devin.Config{Home: a.Home, Now: func() time.Time { return a.Now() }}
	// published stands for the store's copy of this host's devin reading: it
	// carries the local record only once the stop's publish ran.
	published := false
	src := a.Source.(*fakePlacementSource)
	base := src.quotaDo
	src.quotaDo = func(key string) *PoolView {
		v := base(key)
		if v == nil || !strings.HasPrefix(key, "devin:") {
			return v
		}
		v.Free = &shared.FreeBucket{State: shared.FreeAvailable}
		if published {
			v.Free = devin.LocalFree(cfg)(a.Now())
		}
		return v
	}
	fr := a.Runner.(*fakeRunner)
	fr.Do = func(c Command) Result {
		if c.Name == "panewire" && strings.Join(c.Args, " ") == "quota refresh devin" {
			published = true
		}
		return Result{}
	}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("before the limit the free rung is admitted: chosen=%v err=%v", sel.chosen, err)
	}

	// The pane reports the limit; the stop records it and publishes it.
	if err := a.quotaStopEvent(context.Background(), Job{ID: "job-a", Pane: "w:p1"}, quotastop.Stop{Pool: "devin-free", Reset: reset}, true); err != nil {
		t.Fatal(err)
	}
	if !published {
		t.Fatal("the stop did not publish the free limit into the account's shared reading")
	}

	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:free_limit" {
		t.Fatalf("the next selection must drop the free rung as free_limit: %v", got)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("the next selection must move to another pool: chosen=%s", sel.chosen.seatName)
	}

	// Past the reset the record no longer holds and the rung returns on its own.
	if err := a.Sleep(context.Background(), 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("after the reset the free rung must be admitted again: chosen=%v err=%v", sel.chosen, err)
	}
}
