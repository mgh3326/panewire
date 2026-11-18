package panewire

import "time"

// inboxWorkFallback bounds how long held and digest batch building can go
// unrun with nothing noted. Every writer that can create or change batch work
// calls noteWorkChanged, and every known deadline is kept in memory, so this
// interval only heals a writer the gate does not know about: a missed mark
// delays work by at most this long and never strands it. It is not the
// cadence of the work (hk 5301; docs/quiet-delivery.md).
const inboxWorkFallback = time.Minute

// inboxWorkGate decides whether a flush tick builds batches. The ticker fires
// every 100 ms, and each build opens a transaction and scans the hold, digest
// and escalation sets; an idle hub has nothing to find, so a pass runs only
// when one of these holds:
//
//   - the gate is not primed: the first pass after start (or after a failed
//     pass) rebuilds the earliest due time from the store;
//   - the store noted a write that can create or change batch work;
//   - the earliest known hold-release, digest or escalation time has passed;
//   - a lane whose hold waits on a working receiver stopped working, which
//     no store write records;
//   - the safety fallback lapsed.
//
// The flush holds flushMu for every access.
type inboxWorkGate struct {
	primed bool
	// seen is the store's workVersion when the last successful pass began.
	seen uint64
	// due is the earliest unclaimed deadline (unix ms) that pass left behind;
	// 0 means none.
	due int64
	// watch lists the lanes that pass left held while their receiver worked.
	watch []string
	// ranAt is when the last successful pass finished.
	ranAt time.Time
}

// batchWorkDue reports whether this tick must build batches, and the store's
// change counter to record if the pass succeeds. The caller holds flushMu.
func (h *HubServer) batchWorkDue(now time.Time) (bool, uint64) {
	in := h.inbox
	gen := in.store.workVersion()
	if !in.work.primed || gen != in.work.seen || now.Sub(in.work.ranAt) >= inboxWorkFallback {
		return true, gen
	}
	if in.work.due > 0 && now.UnixMilli() >= in.work.due {
		return true, gen
	}
	for _, lane := range in.work.watch {
		if !h.receiverInboxState(lane).working {
			return true, gen
		}
	}
	return false, gen
}

// noteBatchWork records a finished pass. A failed one leaves the gate
// unprimed, so the retry (already paced by the flush backoff) rebuilds it.
func (in *hubInbox) noteBatchWork(gen uint64, due int64, watch []string, now time.Time, ok bool) {
	if !ok {
		in.work = inboxWorkGate{}
		return
	}
	in.work = inboxWorkGate{primed: true, seen: gen, due: due, watch: watch, ranAt: now}
}
