package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/devin"
	"github.com/mgh3326/panewire/internal/quota/shared"
	"github.com/mgh3326/panewire/internal/quotastop"
)

// A devin-free quota stop writes this host's free-limit record, so the next
// devin measurement publishes it and selection skips the free rung until the
// reset. The record survives the process: it is the file `quota observe
// devin-free-limit` writes.
func TestQuotaStopAnnounceRecordsFreeLimit_T4006(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	a := testApp(t)
	cfg := devin.Config{Home: a.Home, Now: a.Now}
	reset := a.Now().Add(2 * time.Hour)
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "job-a", Pane: "w:p1"}, quotastop.Stop{Pool: "devin-free", Reset: reset}); err != nil {
		t.Fatal(err)
	}
	got := devin.LocalFree(cfg)(a.Now())
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(reset) {
		t.Fatalf("free limit after a quota stop: %+v", got)
	}
	// Another pool's stop is not a devin free limit.
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "job-a", Pane: "w:p1"}, quotastop.Stop{Pool: "claude", Reset: reset}); err != nil {
		t.Fatal(err)
	}
	// Past the reset the record no longer holds.
	if err := a.Sleep(context.Background(), 3*time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := devin.LocalFree(cfg)(a.Now()); got.State != shared.FreeAvailable {
		t.Fatalf("free limit after its reset: %+v", got)
	}
}

// A stop with no readable reset uses the record's one-hour hold, the same
// window `quota observe devin-free-limit --unparsed` writes.
func TestQuotaStopAnnounceUnknownResetHolds_T4006(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PACELEVER_CACHE", "")
	a := testApp(t)
	cfg := devin.Config{Home: a.Home, Now: a.Now}
	if err := a.quotaStopAnnounce(context.Background(), Job{ID: "job-a", Pane: "w:p1"}, quotastop.Stop{Pool: "devin-free"}); err != nil {
		t.Fatal(err)
	}
	got := devin.LocalFree(cfg)(a.Now())
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(a.Now().Add(time.Hour)) {
		t.Fatalf("an unknown reset holds one hour: %+v", got)
	}
}
