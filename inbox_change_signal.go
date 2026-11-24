package panewire

import "sync"

// inboxChangeSignal tells the re-wake pass (inbox_rewake.go) which lanes'
// unread sets may have changed since it last read them (hk 6257). The unread
// predicate (laneInboxUnread) reads stored columns only and has no time term,
// so a lane's unread set changes only when a row of that lane is written, and
// the set of lanes changes only when a row is inserted or deleted.
//
// Every writer that can insert or delete a lane_inbox row, or change a column
// the unread predicate reads (superseded_at, must_ack, acked_at, class,
// read_at), calls noteInboxChanged after its commit. The writers are listed in
// docs/quiet-delivery.md#the-re-wake and pinned by
// TestInboxChangeSignalEveryLaneInboxWriterBumps. Only the hub process writes
// these rows while it serves (hub store-copy needs the hub stopped), so the
// in-process counter is complete.
//
// A reader loads the version before it queries: a write that commits between
// the load and the query is seen by the query and costs one extra read next
// time, and one that commits after the query bumps past the loaded version.
type inboxChangeSignal struct {
	mu  sync.Mutex
	seq uint64
	// lanes holds, per lane, the sequence of its last change. A lane in it is
	// a lane the signal has seen written.
	lanes map[string]uint64
	// all is the sequence of the last change whose lane is unknown (an ack or
	// read that named none, a prune): it moves every lane's version.
	all uint64
	// members is the sequence of the last change that can add or remove a
	// lane: the first write the signal sees for a lane, an unknown lane, or a
	// delete.
	members uint64
}

// noteInboxChanged records a committed change to lane's unread set. An empty
// lane means the change cannot be placed in one lane (an ack or read that
// named none, a prune's deletes, which can also empty a lane).
func (s *laneInboxStore) noteInboxChanged(lane string) {
	c := &s.inboxChange
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	if lane == "" {
		c.all, c.members = c.seq, c.seq
		return
	}
	if c.lanes == nil {
		c.lanes = map[string]uint64{}
	}
	if _, seen := c.lanes[lane]; !seen {
		c.members = c.seq
	}
	c.lanes[lane] = c.seq
}

// inboxLaneVersion is lane's change counter. A reader keeps the value it read
// at and compares.
func (s *laneInboxStore) inboxLaneVersion(lane string) uint64 {
	c := &s.inboxChange
	c.mu.Lock()
	defer c.mu.Unlock()
	return max(c.lanes[lane], c.all)
}

// inboxMembersVersion is the lane list's change counter.
func (s *laneInboxStore) inboxMembersVersion() uint64 {
	c := &s.inboxChange
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.members
}
