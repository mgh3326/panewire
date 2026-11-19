package fleet

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 3704: a job's dir goes at job end once the pane is closed, job.reaped is
// on record and the relay owes the hub nothing for the job. The at-end
// delete is the TTL gate with the age wait off; these tests pin each of the
// three conditions as a mutant that must keep the dir.

// relayOwes is a RelayOwed reader that answers owed and err for every job.
func relayOwes(owed bool, err error) func(context.Context, string) (bool, error) {
	return func(context.Context, string) (bool, error) { return owed, err }
}

// unreap removes the job.reaped event seedJob wrote, so the job has ended
// without its reap on record.
func (r *ttlRig) unreap(id string) {
	r.t.Helper()
	dir := filepath.Join(r.dir(id), "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, entry := range entries {
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			r.t.Fatal(err)
		}
		if strings.Contains(string(body), "job.reaped") {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	j, err := r.a.job(id)
	if err != nil {
		r.t.Fatal(err)
	}
	if j.Reaped {
		r.t.Fatalf("job %s still reaped after the job.reaped event was removed", id)
	}
}

// Every condition holds at the instant the job ends: its files are no older
// than the clock, so the TTL pass would keep the dir as too-young. The
// at-end pass deletes it.
func TestJobDirEndDeletesWithoutAgeWait(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.seedJob("j2")

	if got := (&JobDirTTL{RelayOwed: relayOwes(false, nil)}).ExpireEnded(context.Background(), r.a, "j1"); got != "confirmed" {
		t.Fatalf("at-end reason = %q, want confirmed", got)
	}
	if r.exists("j1") {
		t.Fatal("j1 still exists after its at-end delete")
	}

	var out bytes.Buffer
	if _, err := (&JobDirTTL{TTL: DefaultJobDirTTL, Apply: true, Out: &out}).Pass(context.Background(), r.a); err != nil {
		t.Fatal(err)
	}
	wantKeep(t, r, out.String(), "j2", "too-young")
}

// A relay row the hub has not persisted keeps the dir: the scan reads the
// job's events, so deleting them would lose a row the hub still owes.
func TestJobDirEndKeepsWhileTheRelayOwes(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	if got := (&JobDirTTL{RelayOwed: relayOwes(true, nil)}).ExpireEnded(context.Background(), r.a, "j1"); got != "relay-owed" {
		t.Fatalf("at-end reason = %q, want relay-owed", got)
	}
	if !r.exists("j1") {
		t.Fatal("j1 was deleted while the relay still owed the hub a row")
	}
}

// Without a relay reader the at-end pass cannot prove the relay settled, so
// it keeps every dir; an unreadable relay keeps it too.
func TestJobDirEndKeepsWithoutAnAnswerFromTheRelay(t *testing.T) {
	for name, want := range map[string]struct {
		reader func(context.Context, string) (bool, error)
		reason string
	}{
		"no reader":  {nil, "relay-unknown"},
		"unreadable": {relayOwes(false, errors.New("outbox locked")), "relay-unreadable"},
		"owed":       {relayOwes(true, nil), "relay-owed"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			if got := (&JobDirTTL{RelayOwed: want.reader}).ExpireEnded(context.Background(), r.a, "j1"); got != want.reason {
				t.Fatalf("at-end reason = %q, want %s", got, want.reason)
			}
			if !r.exists("j1") {
				t.Fatalf("j1 was deleted with reason %s", want.reason)
			}
		})
	}
}

// A pane of the job still in herdr keeps the dir, whichever way herdr names
// it: the at-end pass runs the TTL gate's own pane check.
func TestJobDirEndKeepsWhilePaneIsLive(t *testing.T) {
	for name, panes := range map[string]string{
		"pane id":    `{"result":{"panes":[{"pane_id":"w:j1","tab_id":"w:t1","agent_status":"","cwd":"/work/a"}]}}`,
		"pane label": `{"result":{"panes":[{"pane_id":"w:other","tab_id":"w:t9","label":"j1","agent_status":"","cwd":"/work/a"}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newTTLRig(t)
			r.seedJob("j1")
			r.panes = panes
			if got := (&JobDirTTL{RelayOwed: relayOwes(false, nil)}).ExpireEnded(context.Background(), r.a, "j1"); got != "pane-live" {
				t.Fatalf("at-end reason = %q, want pane-live", got)
			}
			if !r.exists("j1") {
				t.Fatal("j1 was deleted while its pane was live")
			}
		})
	}
}

// A job whose end is not on record in its own ledger keeps its dir, even
// when every other condition holds.
func TestJobDirEndKeepsWithoutJobReaped(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.unreap("j1")
	if got := (&JobDirTTL{RelayOwed: relayOwes(false, nil)}).ExpireEnded(context.Background(), r.a, "j1"); got != "not-reaped" {
		t.Fatalf("at-end reason = %q, want not-reaped", got)
	}
	if !r.exists("j1") {
		t.Fatal("j1 was deleted without job.reaped on record")
	}
}

// The queue retries an owed dir on every pass until the relay settles, then
// deletes it and drops it.
func TestJobDirEndQueueRetriesUntilTheRelaySettles(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	owed := true
	s := &TaskPaneSweeper{RelayOwed: func(context.Context, string) (bool, error) { return owed, nil }}
	s.queueEndedJobDir("j1", r.now)

	s.expireEndedJobDirs(context.Background(), r.a)
	if !r.exists("j1") || len(s.endedDirs) != 1 {
		t.Fatalf("first pass: j1 exists=%t queued=%d, want the dir kept and still queued", r.exists("j1"), len(s.endedDirs))
	}

	owed = false
	s.expireEndedJobDirs(context.Background(), r.a)
	if r.exists("j1") || len(s.endedDirs) != 0 {
		t.Fatalf("second pass: j1 exists=%t queued=%d, want the dir deleted and dropped", r.exists("j1"), len(s.endedDirs))
	}
}

// Past MinJobDirTTL, the relay hold ceiling, the queue drops the dir without
// an attempt: the TTL pass owns it from then on.
func TestJobDirEndQueueDropsAfterTheRelayWindow(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	s := &TaskPaneSweeper{RelayOwed: func(context.Context, string) (bool, error) { return false, nil }}
	s.queueEndedJobDir("j1", r.now)

	r.now = r.base.Add(MinJobDirTTL)
	s.expireEndedJobDirs(context.Background(), r.a)
	if !r.exists("j1") || len(s.endedDirs) != 0 {
		t.Fatalf("past the window: j1 exists=%t queued=%d, want the dir kept and dropped", r.exists("j1"), len(s.endedDirs))
	}
}

// A sweeper without a relay reader queues nothing: the at-end delete never
// runs there, and the TTL pass keeps the dir.
func TestJobDirEndQueueNeedsARelayReader(t *testing.T) {
	s := &TaskPaneSweeper{}
	s.queueEndedJobDir("j1", testApp(t).Now())
	if n := len(s.endedDirs); n != 0 {
		t.Fatalf("queued %d dirs without a relay reader, want none", n)
	}
}
