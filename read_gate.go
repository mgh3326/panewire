package panewire

// Contract: docs/hub-postgres.md, "Scan cost" (hk 5302, phase 0 of hk 4898;
// design hk:doc design/2026-11-18/pw-4898-hub-store-mysql §3).
//
// The hub's polling loops used to read their store on every tick: the spawn
// coordinator every 2 s, the placement-record probe every 2 s, the consult
// loop every 5 s. With nothing to do each tick still cost a transaction. A
// readGate lets a loop read its store only when one of these holds:
//
//   - start: the gate is not primed, so the first pass after a hub start (or
//     after a failed pass) reads the store and rebuilds the earliest due time;
//   - kick: a write that adds work for this loop called noteWork after its
//     commit (or the loop's existing kick channel fired);
//   - due: the earliest time the last pass left behind has passed. A pass
//     reports what is still owed: a retry delay, a parked request's
//     next_check_ms, a held notice's hold expiry, or "look again at the loop's
//     old cadence" while work is in flight;
//   - backstop: readBackstop passed with none of the above. A write the gate
//     was not taught about delays its work by at most this long and never
//     strands it. When a backstop pass finds work, the hub logs it: that line
//     is the evidence that some write path misses its kick.
//
// An idle loop therefore sends no query between backstops. The loops keep
// their in-memory tick (liveness ledger, drain marks); only the store read is
// gated.

import (
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// readKind names a gated loop.
type readKind int

const (
	// readSpawnScan is the coordinator's scan of the spawn queue: waiting
	// rows, parked notices, the runnable page and the stale probes.
	readSpawnScan readKind = iota
	// readSpawnNotice is the coordinator's flush of spawn_notice_outbox.
	readSpawnNotice
	// readSpawnTaskFail is the coordinator's sweep of task_fail_notice_outbox.
	readSpawnTaskFail
	// readPlacementRecord is the placement-record probe of
	// placement_record_outbox.
	readPlacementRecord
	// readConsult is the consult coordinator's read of the active consults.
	readConsult
	readKinds
)

var readKindNames = [readKinds]string{"spawn_scan", "spawn_notice", "spawn_task_fail", "placement_record", "consult"}

// readBackstop bounds how long a loop can go without a read while nothing is
// noted and nothing is due. Every write that adds work calls noteWork and
// every known deadline is kept in memory, so this interval only heals a write
// the gate was not taught about. It is not the loop's cadence.
const readBackstop = time.Minute

type readWhy string

const (
	readWhyStart    readWhy = "start"
	readWhyKick     readWhy = "kick"
	readWhyDue      readWhy = "due"
	readWhyBackstop readWhy = "backstop"
)

// readGate is one loop's gate. gen and wake are the kick side: any goroutine
// may call note. The rest is the loop's own state, guarded by mu so a test
// that drives a pass by hand and the loop cannot race.
type readGate struct {
	name string
	// gen counts the writes noted for this loop; the loop keeps the value its
	// last successful pass began at and compares.
	gen atomic.Uint64
	// wake carries a non-blocking, coalescing signal to the loop's select, so
	// a kick does not wait for the loop's next tick. A nil channel (a store a
	// test built by hand) simply never signals.
	wake chan struct{}

	mu     sync.Mutex
	primed bool
	seen   uint64
	// due is the earliest unix ms the last pass said it must look again; 0
	// means none.
	due   int64
	ranAt time.Time
}

func (g *readGate) init(name string) {
	g.name = name
	g.wake = make(chan struct{}, 1)
}

// note records a write that adds work for the loop and wakes it. Call it after
// the write committed: a reader that took the mark before the commit would
// read nothing, and the mark would be spent.
func (g *readGate) note() {
	g.gen.Add(1)
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// touch records work the loop already woke for through its own kick channel,
// so the pass that follows reads. It does not signal.
func (g *readGate) touch() { g.gen.Add(1) }

// readTicket carries a pass's starting counter and the reason it ran.
type readTicket struct {
	gen uint64
	why readWhy
}

// readResult is what a pass reports. due is the unix ms the loop must read
// again by, 0 for none: a pass that left no work and no deadline leaves the
// loop idle. found says the pass saw work, for the backstop log. failed
// leaves the gate unprimed, so the next tick reads again.
type readResult struct {
	found  bool
	due    int64
	failed bool
}

// begin reports whether the loop reads at now.
func (g *readGate) begin(now time.Time) (readTicket, bool) {
	gen := g.gen.Load()
	g.mu.Lock()
	defer g.mu.Unlock()
	t := readTicket{gen: gen}
	switch {
	case !g.primed:
		t.why = readWhyStart
	case gen != g.seen:
		t.why = readWhyKick
	case g.due > 0 && now.UnixMilli() >= g.due:
		t.why = readWhyDue
	case now.Sub(g.ranAt) >= readBackstop:
		t.why = readWhyBackstop
	default:
		return t, false
	}
	return t, true
}

// end records a finished pass. A backstop pass that found work is logged:
// nothing announced that work.
func (g *readGate) end(t readTicket, now time.Time, r readResult, logger *slog.Logger) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.failed {
		g.primed, g.seen, g.due, g.ranAt = false, 0, 0, time.Time{}
		return
	}
	g.primed, g.seen, g.due, g.ranAt = true, t.gen, r.due, now
	if t.why == readWhyBackstop && r.found && logger != nil {
		logger.Warn("hub read backstop found work that no kick announced", "loop", g.name, "backstop", readBackstop.String())
	}
}

// noteWork records that a committed write added work for each loop. A store
// built without gates (a test's literal) ignores it.
func (s *laneInboxStore) noteWork(kinds ...readKind) {
	for _, k := range kinds {
		if s.reads[k].name != "" {
			s.reads[k].note()
		}
	}
}

// readGate returns a loop's gate.
func (s *laneInboxStore) readGate(k readKind) *readGate { return &s.reads[k] }

// gatedRead runs pass when the loop's gate opens, and reports whether it did.
// The pass reads the store; an idle loop never reaches it.
//
// A loop with its own liveness tick passes its mechanism: an idle pass still
// ticks it, so the ledger sees the loop alive while the store is not read. A
// pass that runs ticks it itself. A loop whose caller ticks passes "".
func (h *HubServer) gatedRead(k readKind, mech loopMechanism, pass func() readResult) bool {
	if h.inbox == nil {
		return false
	}
	g := h.inbox.store.readGate(k)
	ticket, run := g.begin(h.now())
	if !run {
		if mech != "" {
			h.loopTick(mech)
		}
		return false
	}
	result := pass()
	g.end(ticket, h.now(), result, h.logger)
	return true
}

// retryAt is the due time of a pass that left work to look at again after
// delay: the loop's old cadence while work is in flight.
func (h *HubServer) retryAt(delay time.Duration) int64 { return h.now().Add(delay).UnixMilli() }

// commitNoting commits tx and, only if the commit succeeded, notes the loops
// its writes gave work. The mark follows the commit: a loop that took it
// before would read the old state and spend it.
func (s *laneInboxStore) commitNoting(tx *sql.Tx, kinds ...readKind) error {
	if err := tx.Commit(); err != nil {
		return err
	}
	s.noteWork(kinds...)
	return nil
}

// spawnEndedReads are the loops an operation's terminal write gives work:
// its notice, its hk task comment when failed or refused, its placement
// record, and the queue the coordinator scans.
var spawnEndedReads = []readKind{readSpawnScan, readSpawnNotice, readSpawnTaskFail, readPlacementRecord}
