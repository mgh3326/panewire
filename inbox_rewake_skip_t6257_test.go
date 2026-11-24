package panewire

// hk 6257 (operator decision 492, option A): the re-wake pass reads the store
// only for a lane whose rows were written since it last read it, and the lane
// list only when a lane may have appeared or gone. Every time rule is still
// evaluated at each 10 s scan on the cached state. These tests drive the pass
// on the rig's injected clock and watch its store reads through the seam
// inboxWakes.rewakeRead; a counting connector (countStoreCalls) shows that a
// scan with nothing to read sends the database no statement at all.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// rewakeSkipRig is the t3602 rig on a known store path, with the pass's store
// reads recorded.
type rewakeSkipRig struct {
	*t3602Rig
	path  string
	reads []string
}

func newRewakeSkipRig(t *testing.T) *rewakeSkipRig {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	start, _ := time.Parse(time.RFC3339, t3602Start)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	r := &rewakeSkipRig{t3602Rig: t3602Hub(t, path, "default", fake, hk, &t3602Clock{now: start}), path: path}
	// The pass holds the wake mutex whenever it reads, and the tests read the
	// list between scans.
	r.h.inbox.wakes.rewakeRead = func(what, lane string) { r.reads = append(r.reads, what+":"+lane) }
	return r
}

// sweepReads runs one scan and returns the store reads it made.
func (r *rewakeSkipRig) sweepReads() []string {
	r.reads = nil
	rewakeSweep(r.t3602Rig)
	return r.reads
}

// tick moves the clock one scan interval, scans, and returns the reads.
func (r *rewakeSkipRig) tick() []string {
	r.clock.advance(inboxRewakeScanEvery)
	return r.sweepReads()
}

func wantReads(t *testing.T, when string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: store reads = %v, want %v", when, got, want)
	}
}

// healReads is what a heal scan reads for the rig's one inbox-only lane.
var healReads = []string{"lanes:", "unread:" + t3602Design}

// (a) An unchanged lane with an unread item and an idle pane: for the whole
// healing window (20 scans, 200 s of test clock) no scan reads the store or
// reaches the database; once inboxRewakeHealEvery lapses one scan re-reads the
// lane list and the lane; and the scan that finds the re-wake interval spent
// sends the line exactly as at 43115fa0. Mutant: refreshRewakeUnread without
// its version check reads the lane at every scan.
func TestInboxRewakeSkipUnchangedLaneDueByTime(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	wantReads(t, "first scan", r.sweepReads(), healReads...)
	calls := countStoreCalls(t, r.h, r.path)
	for i := range 20 {
		wantReads(t, "scan inside the healing window", r.tick())
		rewakeNoLine(t, r.t3602Rig, "scan inside the healing window")
		if got := calls.Load(); got != 0 {
			t.Fatalf("scan %d sent %d statements to the store, want 0", i+2, got)
		}
	}
	// 200 s have passed; the first scan at or after 5 m heals, once.
	r.clock.advance(inboxRewakeHealEvery - 20*inboxRewakeScanEvery)
	wantReads(t, "scan when the healing window lapses", r.sweepReads(), healReads...)
	wantReads(t, "scan after the heal", r.tick())
	rewakeNoLine(t, r.t3602Rig, "heal scan, 5 m after the set was first seen")
	// 25 m later the interval is spent: the line goes out at that scan, which
	// also heals because the window lapsed again.
	r.clock.advance(25 * time.Minute)
	wantReads(t, "scan after the interval", r.sweepReads(), healReads...)
	rewakeOneLine(t, r.t3602Rig, "30 m after the set was first seen", t3602Wake(t3602Design, 1, "info"))
}

// The heal exists for a writer that missed its bump: a row changed behind the
// signal's back is found when the healing window lapses, and not before.
// Mutant: no heal, and the stale "unread" sends a line for an acked item.
func TestInboxRewakeSkipHealFindsAWriteTheSignalMissed(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.sweepReads()
	// An ack that does not call noteInboxChanged: straight to the table.
	if _, err := r.h.inbox.store.db.Exec(`UPDATE lane_inbox SET acked_at=1, read_at=1 WHERE lane=?`, t3602Design); err != nil {
		t.Fatal(err)
	}
	wantReads(t, "scan before the heal", r.tick())
	r.clock.advance(inboxRewakeHealEvery)
	wantReads(t, "heal scan", r.sweepReads(), healReads...)
	r.clock.advance(30 * time.Minute)
	r.sweepReads()
	rewakeNoLine(t, r.t3602Rig, "30 m after the missed ack was healed")
}

// (b) The pane is busy when the interval passes and turns idle later with no
// inbox write: the line goes out on the first scan after it turns idle, and no
// scan in between read the store. Mutant: a pass that skips an unchanged lane
// altogether (not only its read) never sends this line.
func TestInboxRewakeSkipUnchangedLaneDueByIdle(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.sweepReads()
	rewakeSetPaneStatus(r.t3602Rig, rewakePane, "working")
	r.clock.advance(31 * time.Minute)
	wantReads(t, "busy scan past the interval (a heal scan)", r.sweepReads(), healReads...)
	rewakeNoLine(t, r.t3602Rig, "busy scan past the interval")
	for range 5 {
		wantReads(t, "busy scan", r.tick())
		rewakeNoLine(t, r.t3602Rig, "busy scan")
	}
	rewakeSetPaneStatus(r.t3602Rig, rewakePane, "idle")
	wantReads(t, "first scan after the pane is idle", r.tick())
	rewakeOneLine(t, r.t3602Rig, "first scan after the pane is idle", t3602Wake(t3602Design, 1, "info"))
}

// (c) Each write that changes the lane's unread set forces one read of that
// lane at the next scan, and nothing else (the lane list stays cached).
// Mutants: drop the noteInboxChanged call from put, read, ack or fileUnread and
// the matching case sees no read and the stale state.
func TestInboxRewakeSkipChangedLaneIsReadAgain(t *testing.T) {
	infoWith := func(cause, head, notify string) hubJobEventPayload {
		e := t3602Event(t3602Design, notify, "hk 6257 "+notify)
		e.Cause, e.Head = cause, head
		return e
	}
	cases := []struct {
		name string
		// write changes the lane's rows; wantUnread is the unread count the
		// line at the interval states, or 0 for no line.
		write      func(t *testing.T, r *rewakeSkipRig)
		wantUnread int
	}{
		{"a new item", func(t *testing.T, r *rewakeSkipRig) {
			r.fileQuiet(t, t3602Event(t3602Design, "merged", "[mergewake] MERGED #401 hk 6257 into main"), r.agent)
		}, 2},
		{"a read", func(t *testing.T, r *rewakeSkipRig) {
			items, err := r.h.inbox.store.list(t3602Design, true)
			if err != nil || len(items) != 1 {
				t.Fatalf("unread = %+v, %v", items, err)
			}
			if _, err := r.h.inbox.store.read(items[0].ID, "", r.clock.Now()); err != nil {
				t.Fatal(err)
			}
		}, 0},
		{"an ack", func(t *testing.T, r *rewakeSkipRig) {
			if n, err := r.h.inbox.store.ack(0, t3602Design, true, r.clock.Now()); err != nil || n != 1 {
				t.Fatalf("ack = %d, %v", n, err)
			}
		}, 0},
		{"a supersede", func(t *testing.T, r *rewakeSkipRig) {
			r.fileQuiet(t, infoWith("pr:o/r#7", "bbb2222", "merged"), r.agent)
		}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRewakeSkipRig(t)
			if c.name == "a supersede" {
				r.fileQuiet(t, infoWith("pr:o/r#7", "aaa1111", "ready"), r.agent)
				t3426Injected(r.agent)
			} else {
				rewakeFileInfo(t, r.t3602Rig)
			}
			r.sweepReads()
			wantReads(t, "unchanged scan", r.tick())
			c.write(t, r)
			t3426Injected(r.agent) // a wake line for the new item is not under test
			wantReads(t, "scan after "+c.name, r.tick(), "unread:"+t3602Design)
			wantReads(t, "next scan", r.tick())
			// The set the write left is announced at the interval, or not at all.
			r.clock.advance(30 * time.Minute)
			wantReads(t, "scan at the interval (a heal scan)", r.sweepReads(), healReads...)
			got := t3426Injected(r.agent)
			if c.wantUnread == 0 {
				if len(got) != 0 {
					t.Fatalf("after %s: injections=%+v, want none", c.name, got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0].Text, " "+strconv.Itoa(c.wantUnread)+" unread ") {
				t.Fatalf("after %s: injections=%+v, want one line counting %d unread", c.name, got, c.wantUnread)
			}
		})
	}
}

// A new item after the set spent its lines starts a new set from the cached
// state exactly as before: the count resets and the lines come again.
func TestInboxRewakeSkipNewItemResetsTheCount(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.sweepReads()
	for _, at := range []string{"30 m", "60 m", "90 m"} {
		r.clock.advance(30 * time.Minute)
		r.sweepReads()
		rewakeOneLine(t, r.t3602Rig, at, t3602Wake(t3602Design, 1, "info"))
	}
	r.clock.advance(30 * time.Minute)
	wantReads(t, "spent set (a heal scan)", r.sweepReads(), healReads...)
	rewakeNoLine(t, r.t3602Rig, "spent set")
	r.fileQuiet(t, t3602Event(t3602Design, "merged", "[mergewake] MERGED #402 hk 6257 into main"), r.agent)
	t3426Injected(r.agent)
	wantReads(t, "scan after the new item", r.tick(), "unread:"+t3602Design)
	r.clock.advance(30 * time.Minute)
	r.sweepReads()
	rewakeOneLine(t, r.t3602Rig, "30 m into the new set", t3602Wake(t3602Design, 2, "info"))
}

// (d) An item on a lane not seen before is found by the next scan: the lane
// list is read again, then the new lane, and its line goes out at the
// interval. Mutant: cache the lane list without the members version.
func TestInboxRewakeSkipNewLaneIsPickedUp(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.sweepReads()
	wantReads(t, "unchanged scan", r.tick())
	const desk = "operator-desk"
	if got := r.fileQuiet(t, t3602Event(desk, "merged", "[mergewake] MERGED #403 hk 6257 into main"), r.agent); len(got) != 0 {
		t.Fatalf("info arrival typed %+v", got)
	}
	wantReads(t, "scan after the new lane's item", r.tick(), "lanes:", "unread:"+desk)
	wantReads(t, "next scan", r.tick())
	r.clock.advance(30 * time.Minute)
	r.sweepReads()
	got := t3426Injected(r.agent)
	panes := map[string]bool{}
	for _, g := range got {
		panes[g.Pane] = true
	}
	if len(got) != 2 || !panes[rewakePane] || !panes["w1:desk"] {
		t.Fatalf("injections=%+v, want one line in %s and one in w1:desk", got, rewakePane)
	}
}

// A hub restart starts with an empty cache: the first scan reads the lane list
// and every inbox-only lane, as before.
func TestInboxRewakeSkipFirstScanReadsEverything(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.fileQuiet(t, t3602Event("operator-desk", "merged", "[mergewake] MERGED #404 hk 6257 into main"), r.agent)
	t3426Injected(r.agent)
	wantReads(t, "first scan", r.sweepReads(), "lanes:", "unread:"+t3602Design, "unread:operator-desk")
}

// A failed read is not "nothing unread" and is retried at the next scan.
func TestInboxRewakeSkipFailedReadIsRetried(t *testing.T) {
	r := newRewakeSkipRig(t)
	rewakeFileInfo(t, r.t3602Rig)
	r.h.inbox.store.abort() // every store call now fails
	wantReads(t, "failing scan", r.sweepReads(), "lanes:")
	wantReads(t, "failing scan again", r.tick(), "lanes:")
}

// (e) Every store method that inserts or deletes a lane_inbox row, or changes a
// column the unread predicate reads, moves the change signal. A control for
// each shows a no-op write leaves it where it was. This runs on every backend
// the store contract runs on.
func TestInboxChangeSignalEveryWriterBumps(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		const lane = "lane-a"
		moved := func(what string, write func(), wantMembers bool) {
			t.Helper()
			lv, mv := s.inboxLaneVersion(lane), s.inboxMembersVersion()
			write()
			if s.inboxLaneVersion(lane) == lv {
				t.Fatalf("%s did not move the lane's change version", what)
			}
			if got := s.inboxMembersVersion() != mv; got != wantMembers {
				t.Fatalf("%s moved the lane list's version = %v, want %v", what, got, wantMembers)
			}
		}
		still := func(what string, write func()) {
			t.Helper()
			lv, mv := s.inboxLaneVersion(lane), s.inboxMembersVersion()
			write()
			if s.inboxLaneVersion(lane) != lv || s.inboxMembersVersion() != mv {
				t.Fatalf("%s moved the change version, want it unchanged", what)
			}
		}
		put := func(key, class, cause, head, notify string) laneInboxItem {
			t.Helper()
			e := quietEvent(key, "hk 6257 "+key)
			e.Cause, e.Head, e.NotifyType = cause, head, notify
			item, err := s.put(key, lane, "lane.event", class, e, now, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			return item
		}
		var first, interrupt laneInboxItem
		moved("put of an item on a new lane", func() { first = put("k1", "inbox", "", "", "") }, true)
		moved("put of an item on a known lane", func() { put("k2", "inbox", "", "", "") }, false)
		still("put of a duplicate", func() { put("k2", "inbox", "", "", "") })

		old := put("k3", "inbox", "pr:o/r#9", "aaa1111", "ready")
		moved("put that supersedes an item", func() { put("k4", "inbox", "pr:o/r#9", "bbb2222", "merged") }, false)
		if got, err := s.list(lane, false); err != nil || !slices.ContainsFunc(got, func(i laneInboxItem) bool { return i.ID == old.ID && i.SupersededAt > 0 }) {
			t.Fatalf("item %d was not superseded: %+v, %v", old.ID, got, err)
		}

		moved("read with its lane", func() {
			if _, err := s.read(first.ID, lane, now); err != nil {
				t.Fatal(err)
			}
		}, false)
		moved("read without a lane", func() {
			if _, err := s.read(first.ID, "", now); err != nil {
				t.Fatal(err)
			}
		}, false)
		still("read of a missing item", func() { _, _ = s.read(first.ID+1000, "", now) })

		interrupt = put("k5", "interrupt", "", "", "")
		moved("fileUnread", func() {
			where, args := s.eventKeyPredicate("k5")
			if items, err := s.fileUnread(now, where, args...); err != nil || len(items) != 1 || items[0].ID != interrupt.ID {
				t.Fatalf("fileUnread = %+v, %v", items, err)
			}
		}, false)
		still("fileUnread of nothing", func() {
			where, args := s.eventKeyPredicate("k5")
			if items, err := s.fileUnread(now, where, args...); err != nil || len(items) != 0 {
				t.Fatalf("second fileUnread = %+v, %v", items, err)
			}
		})
		moved("fileEventUnread of a new row", func() {
			e := quietEvent("k6", "hk 6257 k6")
			if items, err := s.fileEventUnread("k6", lane, "lane.event", e, now); err != nil || len(items) != 1 {
				t.Fatalf("fileEventUnread = %+v, %v", items, err)
			}
		}, false)

		moved("ack by id with its lane", func() {
			if n, err := s.ack(first.ID, lane, false, now); err != nil || n != 1 {
				t.Fatalf("ack = %d, %v", n, err)
			}
		}, false)
		still("ack of nothing", func() { _, _ = s.ack(first.ID, lane, false, now) })
		moved("ack by id without a lane", func() {
			if n, err := s.ack(interrupt.ID, "", false, now); err != nil || n != 1 {
				t.Fatalf("ack = %d, %v", n, err)
			}
		}, true) // the lane is unknown here, so the lane list is stale too
		put("k7", "inbox", "", "", "")
		moved("ack all", func() {
			if n, err := s.ack(0, "", true, now); err != nil || n < 1 {
				t.Fatalf("ack all = %d, %v", n, err)
			}
		}, true)

		still("prune of nothing", func() {
			if n, err := s.prune(now, inboxRetentionPolicy{Days: 1, PerLane: 1000}); err != nil || n != 0 {
				t.Fatalf("early prune = %d, %v", n, err)
			}
		})
		moved("prune that deletes", func() {
			if n, err := s.prune(now.Add(400*24*time.Hour), inboxRetentionPolicy{Days: 1, PerLane: 1000}); err != nil || n < 1 {
				t.Fatalf("prune = %d, %v", n, err)
			}
		}, true)
	})
}

// laneInboxWriteSQL matches the SQL that writes lane_inbox: an UPDATE, DELETE
// or INSERT on the table (not lane_inbox_batches and the like), or a call to
// the dialect's row insert.
var laneInboxWriteSQL = regexp.MustCompile("(UPDATE|DELETE FROM|INSERT INTO|INSERT OR IGNORE INTO) lane_inbox([^_a-zA-Z]|$)|\\.laneInboxInsert\\(\\)")

// Every function that writes lane_inbox is either one that calls
// noteInboxChanged or one named here as touching neither a row's presence nor
// a column the unread predicate reads. A new writer fails this test until it
// bumps the signal or is listed with its reason. The dynamic copy writes of
// `hub store-copy` (hub_store_copy.go) build their SQL from table names; they
// run with the hub stopped (docs/hub-mysql.md#cutover-and-rollback).
func TestInboxChangeSignalEveryLaneInboxWriterBumps(t *testing.T) {
	bumps := []string{"putFrom", "read", "ack", "fileUnread", "prune"}
	exempt := map[string]string{
		"supersede":                      "its rows end in putFrom's transaction, which bumps",
		"laneInboxInsert":                "statement text putFrom runs",
		"makeBatchesDue":                 "sets digest_batch / escalation_batch, not an unread column",
		"makeHeldBatches":                "sets digest_batch, not an unread column",
		"releaseBatchClaims":             "clears digest_batch / escalation_batch, not an unread column",
		"recoverMustAct":                 "start-up: sets due_at / escalation_batch before the hub serves",
		"prepareInbox":                   "start-up: clears due_at / digest_at before the hub serves",
		"backfillLaneInboxEventIDs":      "migration at open: event_id, before the hub serves",
		"backfillLaneInboxDeliveryStage": "migration at open: delivery_stage, before the hub serves",
		"recordDelivery":                 "delivered_at / delivery_* only",
		"recordStage":                    "delivery_* only",
		"recordStageByRelayID":           "delivery_* only",
		"linkRelayID":                    "relay_id only",
	}
	found := map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != "." && !strings.HasPrefix(path, "internal") {
			return fs.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			body := string(src[fset.Position(fd.Pos()).Offset:fset.Position(fd.End()).Offset])
			if !laneInboxWriteSQL.MatchString(body) {
				continue
			}
			name := fd.Name.Name
			found[name] = true
			_, isExempt := exempt[name]
			switch {
			case slices.Contains(bumps, name):
				if !strings.Contains(body, "noteInboxChanged(") {
					t.Errorf("%s (%s) writes lane_inbox and never calls noteInboxChanged", name, path)
				}
			case !isExempt:
				t.Errorf("%s (%s) writes lane_inbox: call noteInboxChanged after its commit, or list it as exempt with its reason", name, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range append(slices.Clone(bumps), slices.Collect(maps.Keys(exempt))...) {
		if !found[name] {
			t.Errorf("%s is listed but no function of that name writes lane_inbox any more; update the list", name)
		}
	}
}
