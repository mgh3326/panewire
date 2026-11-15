package fleet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 4670: the hub's per-spawn app records its placement decision in the
// jobs root and never claims the job there, so the dir holds only
// .wrk-events.lock and events/<n>-job.placement.json artifacts. The TTL
// reads that shape as hub-side: no pane exists on this host to check, and
// the hk job record — terminal, failed or refused, or absent once the dir
// is past the TTL — decides. Every dir these tests build lives under
// t.TempDir().

// seedHubPlacement writes the dir the hub leaves for a delegated spawn: the
// events lock and one job.placement artifact, nothing else.
func (r *ttlRig) seedHubPlacement(id string) {
	r.t.Helper()
	err := r.a.event(context.Background(), id, "job.placement", map[string]any{
		"source": "auto", "chose": "ncp", "ranking": "score", "profile": "cmd-dsflash41", "pool": "deepseek",
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.age(id)
}

func TestJobDirTTLHubPlacementOnly(t *testing.T) {
	cases := []struct {
		name   string
		action string
		reason string
		state  string // hk record state; "" seeds no record
		getErr error
		young  bool
	}{
		{"completed record", "delete", "hub-terminal", "completed", nil, false},
		{"lost record", "delete", "hub-terminal", "lost", nil, false},
		{"failed record", "delete", "hub-terminal", "failed", nil, false},
		{"refused record", "delete", "hub-terminal", "refused", nil, false},
		{"no record past ttl", "delete", "hub-no-record", "", nil, false},
		{"claimed record", "keep", "hk-not-terminal", "claimed", nil, false},
		{"spawned record", "keep", "hk-not-terminal", "spawned", nil, false},
		{"escalated record", "keep", "hk-not-terminal", "escalated", nil, false},
		{"no record before ttl", "keep", "too-young", "", nil, true},
		{"hk outage", "keep", "hk-unreadable", "", errHKUnreadable, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedHubPlacement("h1")
			if c.state != "" {
				r.store.records["h1"] = map[string]any{"job_id": "h1", "state": c.state}
			}
			if c.getErr != nil {
				r.store.getErr = c.getErr
			}
			if c.young {
				r.now = r.base.Add(71 * time.Hour)
			}
			out := r.pass(true)
			if c.action == "delete" {
				wantDelete(t, r, out, "h1", c.reason)
			} else {
				wantKeep(t, r, out, "h1", c.reason)
			}
		})
	}
}

// A hub-side dir has no pane on this host: herdr failing must not keep it.
func TestJobDirTTLHubPlacementSkipsPaneCheck(t *testing.T) {
	r := newTTLRig(t)
	r.seedHubPlacement("h1")
	r.store.records["h1"] = map[string]any{"job_id": "h1", "state": "completed"}
	r.herdr, r.paneRC = 1, 1
	out := r.pass(true)
	wantDelete(t, r, out, "h1", "hub-terminal")
}

// A record that names another job is unreadable, not terminal.
func TestJobDirTTLHubPlacementRecordMismatchKeeps(t *testing.T) {
	r := newTTLRig(t)
	r.seedHubPlacement("h1")
	r.store.records["h1"] = map[string]any{"job_id": "h2", "state": "completed"}
	wantKeep(t, r, r.pass(true), "h1", "hk-unreadable")
}

// Only a dir whose every event is a placement decision is hub-side. A
// malformed artifact, an empty events dir, or any other kind mixed in keeps
// the dir as events-unreadable.
func TestJobDirTTLHubPlacementOnlyShape(t *testing.T) {
	t.Run("malformed event", func(t *testing.T) {
		r := newTTLRig(t)
		r.write("h1", "events/00001-job.placement.json", []byte("{not json"))
		r.age("h1")
		wantKeep(t, r, r.pass(true), "h1", "events-unreadable")
	})
	t.Run("empty events dir", func(t *testing.T) {
		r := newTTLRig(t)
		if err := os.MkdirAll(filepath.Join(r.dir("h1"), "events"), 0700); err != nil {
			t.Fatal(err)
		}
		r.age("h1")
		wantKeep(t, r, r.pass(true), "h1", "events-unreadable")
	})
	t.Run("placement and terminal mixed", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedHubPlacement("h1")
		putEvent(t, r.a, "h1", "job.completed", map[string]any{})
		r.store.records["h1"] = map[string]any{"job_id": "h1", "state": "completed"}
		r.age("h1")
		wantKeep(t, r, r.pass(true), "h1", "events-unreadable")
	})
	t.Run("node-side dir still needs its pane gone", func(t *testing.T) {
		r := newTTLRig(t)
		r.seedJob("j1")
		r.agents = `{"result":{"agents":[{"pane_id":"w:j1","tab_id":"w:t1","name":"j1","agent_status":"idle"}]}}`
		wantKeep(t, r, r.pass(true), "j1", "pane-live")
	})
}
