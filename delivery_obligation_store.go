package panewire

// The hub store's side of the delivery obligations (hk 6384, hk 4289 T1;
// docs/r20-relay-persistence.md, "Delivery obligations"). An obligation is one
// relay row the hub owes a delivery until a stored fact closes it. It shares
// the lane_inbox item's event_key and relay_id. Nothing here deletes a row.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"
)

// The states of delivery_obligations.state. confirmed and settled are final;
// the open ones are read again. noticed is exhausted plus an open incident,
// which no build writes yet.
const (
	obligationOwed      = "owed"
	obligationNoticed   = "noticed"
	obligationExhausted = "exhausted"
	obligationConfirmed = "confirmed"
	obligationSettled   = "settled"
)

// obligationOpen is the predicate of the states a pass still reads. Every
// transition carries it, so a final row never changes again.
const obligationOpen = `state IN ('` + obligationOwed + `','` + obligationNoticed + `','` + obligationExhausted + `')`

// deliveryObligationSchema is SQLite's form of the four tables; Postgres has
// them in migration 0040 and MySQL in migrations 0033-0036. result_id and
// open_slot are NULL when unset: every backend keeps NULLs distinct under a
// plain UNIQUE, and MySQL has no partial indexes.
var deliveryObligationSchema = []string{
	`CREATE TABLE IF NOT EXISTS delivery_obligations (
 id INTEGER PRIMARY KEY, event_key TEXT NOT NULL, result_id VARCHAR(255) NULL,
 relay_id BIGINT NOT NULL DEFAULT 0, kind VARCHAR(64) NOT NULL, lane VARCHAR(255) NOT NULL,
 machine VARCHAR(255) NOT NULL DEFAULT '', pane VARCHAR(255) NOT NULL DEFAULT '',
 state VARCHAR(16) NOT NULL, fault VARCHAR(64) NOT NULL DEFAULT '',
 fault_reason VARCHAR(128) NOT NULL DEFAULT '', spent INTEGER NOT NULL DEFAULT 0,
 labelled INTEGER NOT NULL DEFAULT 0, revived INTEGER NOT NULL DEFAULT 0,
 revive_requested BIGINT NOT NULL DEFAULT 0, attempts_seen INTEGER NOT NULL DEFAULT 0,
 next_at BIGINT NOT NULL DEFAULT 0, incident_id BIGINT NOT NULL DEFAULT 0,
 marker_event_id VARCHAR(255) NOT NULL DEFAULT '', settle_reason VARCHAR(64) NOT NULL DEFAULT '',
 last_fault_at BIGINT NOT NULL DEFAULT 0, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL,
 UNIQUE (event_key), UNIQUE (result_id))`,
	`CREATE INDEX IF NOT EXISTS delivery_obligations_due ON delivery_obligations(state, next_at)`,
	`CREATE INDEX IF NOT EXISTS delivery_obligations_relay ON delivery_obligations(relay_id)`,
	`CREATE TABLE IF NOT EXISTS delivery_incidents (
 id INTEGER PRIMARY KEY, design_lane VARCHAR(255) NOT NULL, machine VARCHAR(255) NOT NULL,
 fault VARCHAR(64) NOT NULL, harness VARCHAR(64) NOT NULL DEFAULT '', state VARCHAR(16) NOT NULL,
 open_slot VARCHAR(512) NULL, members INTEGER NOT NULL DEFAULT 0,
 announced INTEGER NOT NULL DEFAULT 0, notice_relay_id BIGINT NOT NULL DEFAULT 0,
 notice_event_id VARCHAR(255) NOT NULL DEFAULT '', opened_at BIGINT NOT NULL,
 notice_at BIGINT NOT NULL DEFAULT 0, last_member_at BIGINT NOT NULL DEFAULT 0,
 closed_at BIGINT NOT NULL DEFAULT 0, close_reason VARCHAR(32) NOT NULL DEFAULT '',
 UNIQUE (open_slot))`,
	`CREATE TABLE IF NOT EXISTS delivery_notice_slots (
 design_lane VARCHAR(255) NOT NULL, machine VARCHAR(255) NOT NULL, fault VARCHAR(64) NOT NULL,
 notice_at BIGINT NOT NULL DEFAULT 0, notices INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY (design_lane, machine, fault))`,
	`CREATE TABLE IF NOT EXISTS delivery_reconciler_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton=1),
 adopt_after_id BIGINT NOT NULL DEFAULT 0, adopt_wrapped_at BIGINT NOT NULL DEFAULT 0)`,
}

// seedDeliveryReconcilerState stores the one reconciler row if it is missing.
// Every backend's opener calls it, after the schema.
func (s *laneInboxStore) seedDeliveryReconcilerState(parent context.Context) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO delivery_reconciler_state(singleton) VALUES(1)`, "singleton")))
	return err
}

// newDeliveryObligation is what the hub knows about a stored relay row when it
// starts tracking it. key is the hub key (relayEventDedupeKey). attempts is
// handoffkeep's counter at the time, which a row the hub never tracked starts
// its spent budget from.
type newDeliveryObligation struct {
	key                       string
	relayID                   int64
	kind, lane, machine, pane string
	attempts                  int
}

// createObligation stores one owed obligation, first due at due. An event key
// that has one keeps it unchanged.
func (s *laneInboxStore) createObligation(parent context.Context, o newDeliveryObligation, now, due time.Time) error {
	// A key with a second stored form (an oversized one copied back from
	// MySQL) is not caught by the unique key, so it is looked up first.
	if where, keys := s.eventKeyPredicate(o.key); len(keys) > 1 {
		ctx, done := s.op(parent)
		defer done()
		var n int
		if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM delivery_obligations WHERE `+where), keys...).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
	return s.insertObligation(parent, o, now, due)
}

// insertObligation is the one INSERT of an obligation. The values are cut to
// their column widths: MySQL refuses a longer one, which would leave the row
// untracked for good.
func (s *laneInboxStore) insertObligation(parent context.Context, o newDeliveryObligation, now, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO delivery_obligations(event_key,relay_id,kind,lane,machine,pane,state,spent,attempts_seen,next_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, "event_key")),
		s.storedKey(o.key), o.relayID, utf8PrefixWithin(o.kind, 64), utf8PrefixWithin(o.lane, 255), utf8PrefixWithin(o.machine, 255), utf8PrefixWithin(o.pane, 255),
		obligationOwed, o.attempts, o.attempts, due.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	return err
}

// adoptObligations creates the obligations missing among rows. It asks once
// which event keys are tracked, then inserts the rest one by one: a row that
// cannot be inserted does not stop the others, and the first such error is
// returned.
func (s *laneInboxStore) adoptObligations(parent context.Context, rows []newDeliveryObligation, now, due time.Time) error {
	if len(rows) == 0 {
		return nil
	}
	var keys []any
	for _, row := range rows {
		keys = append(keys, s.dialect.storedKeys(row.key)...)
	}
	tracked, err := s.trackedObligationKeys(parent, keys)
	if err != nil {
		return err
	}
	var first error
	for _, row := range rows {
		if slices.ContainsFunc(s.dialect.storedKeys(row.key), func(k any) bool { return tracked[string(storeCopyBytes(k))] }) {
			continue
		}
		if err := s.insertObligation(parent, row, now, due); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// trackedObligationKeys returns, of the stored key forms asked about, those
// an obligation has.
func (s *laneInboxStore) trackedObligationKeys(parent context.Context, keys []any) (map[string]bool, error) {
	ctx, done := s.op(parent)
	defer done()
	marks := strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_key FROM delivery_obligations WHERE event_key IN (`+marks+`)`), keys...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tracked := map[string]bool{}
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		tracked[string(key)] = true
	}
	return tracked, rows.Err()
}

// adoptCursor is where the adopt walk resumes (a handoffkeep row id) and when
// it last reached the end of the listing.
func (s *laneInboxStore) adoptCursor(parent context.Context) (afterID, wrappedAt int64, err error) {
	ctx, done := s.op(parent)
	defer done()
	err = s.db.QueryRowContext(ctx, s.q(`SELECT adopt_after_id,adopt_wrapped_at FROM delivery_reconciler_state WHERE singleton=1`)).Scan(&afterID, &wrappedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return afterID, wrappedAt, err
}

// saveAdoptCursor stores adoptCursor's two values. It is an upsert, so a store
// copied without the row cannot pin the walk to its first pages.
func (s *laneInboxStore) saveAdoptCursor(parent context.Context, afterID, wrappedAt int64) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(s.dialect.upsert(`INSERT INTO delivery_reconciler_state(singleton,adopt_after_id,adopt_wrapped_at) VALUES(1,?,?)`, "singleton", s.dialect.assignIncoming("adopt_after_id", "adopt_wrapped_at"))), afterID, wrappedAt)
	return err
}

// openObligation is the part of an open obligation a pass classifies by.
type openObligation struct {
	id, relayID int64
	// storedKey is the event_key column's bytes, in the dialect's form.
	storedKey                     []byte
	state                         string
	createdAt, nextAt             int64
	fault                         string
	spent, labelled, attemptsSeen int
	// revived counts the one revived copy once taken; reviveRequested is when
	// an idle wake asked for it, and lastFaultAt when the fault was recorded.
	revived                      int
	reviveRequested, lastFaultAt int64
}

// openObligationColumns are what scanOpenObligation reads, in its order.
const openObligationColumns = `id,relay_id,event_key,state,created_at,next_at,fault,spent,labelled,attempts_seen,revived,revive_requested,last_fault_at`

func scanOpenObligation(row interface{ Scan(...any) error }) (openObligation, error) {
	var o openObligation
	err := row.Scan(&o.id, &o.relayID, &o.storedKey, &o.state, &o.createdAt, &o.nextAt, &o.fault, &o.spent, &o.labelled, &o.attemptsSeen, &o.revived, &o.reviveRequested, &o.lastFaultAt)
	return o, err
}

// tracks reports whether the obligation is the one for the hub key. A key is
// never read back from its stored form, only compared with it.
func (s *laneInboxStore) tracks(o openObligation, key string) bool {
	return slices.ContainsFunc(s.dialect.storedKeys(key), func(k any) bool { return bytes.Equal(storeCopyBytes(k), o.storedKey) })
}

// dueObligations is at most limit open obligations due by now, the longest due
// first.
func (s *laneInboxStore) dueObligations(parent context.Context, now time.Time, limit int) ([]openObligation, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+openObligationColumns+` FROM delivery_obligations WHERE `+obligationOpen+` AND next_at<=? ORDER BY next_at,id LIMIT ?`), now.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var due []openObligation
	for rows.Next() {
		o, err := scanOpenObligation(rows)
		if err != nil {
			return nil, err
		}
		due = append(due, o)
	}
	return due, rows.Err()
}

// closeObligation ends an open obligation as confirmed, or as settled for
// reason.
func (s *laneInboxStore) closeObligation(parent context.Context, id int64, state, reason string, now time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET state=?,settle_reason=?,next_at=0,updated_at=? WHERE id=? AND `+obligationOpen), state, utf8PrefixWithin(reason, 64), now.UnixMilli(), id)
	return err
}

// exhaustObligation records that no copy is owed any more and none was proven:
// an owed obligation becomes exhausted for fault, still open to a later
// confirmation. A noticed one keeps its state.
func (s *laneInboxStore) exhaustObligation(parent context.Context, id int64, fault string, now, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET state=CASE WHEN state='`+obligationOwed+`' THEN '`+obligationExhausted+`' ELSE state END,fault=?,next_at=?,updated_at=? WHERE id=? AND `+obligationOpen), fault, due.UnixMilli(), now.UnixMilli(), id)
	return err
}

// deferObligation leaves an open obligation as it is and reads it again at due.
func (s *laneInboxStore) deferObligation(parent context.Context, id int64, now, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET next_at=?,updated_at=? WHERE id=? AND `+obligationOpen), due.UnixMilli(), now.UnixMilli(), id)
	return err
}

// deferObservedObligation updates the schedule only if it still belongs to
// the snapshot the caller classified.
func (s *laneInboxStore) deferObservedObligation(parent context.Context, o openObligation, now, due time.Time) error {
	return s.updateObservedObligation(parent, o, `next_at=?,updated_at=?`, due.UnixMilli(), now.UnixMilli())
}

// exhaustObservedObligation is exhaustObligation for a retry decision: it keeps
// the fault, and does nothing if the row changed after the caller read it.
func (s *laneInboxStore) exhaustObservedObligation(parent context.Context, o openObligation, now, due time.Time) error {
	return s.updateObservedObligation(parent, o, `state=CASE WHEN state='`+obligationOwed+`' THEN '`+obligationExhausted+`' ELSE state END,next_at=?,updated_at=?`, due.UnixMilli(), now.UnixMilli())
}

// markObligationUnrouted names the missing route as the fault of a row the
// caller found safe to copy, and reads it again at due.
func (s *laneInboxStore) markObligationUnrouted(parent context.Context, o openObligation, now, due time.Time) error {
	return s.updateObservedObligation(parent, o, `fault=?,next_at=?,updated_at=?`, obligationFaultUnrouted, due.UnixMilli(), now.UnixMilli())
}

// updateObservedObligation applies set to an open obligation that still matches
// the snapshot o.
func (s *laneInboxStore) updateObservedObligation(parent context.Context, o openObligation, set string, values ...any) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET `+set+`
		WHERE id=? AND state=? AND fault=? AND spent=? AND labelled=? AND next_at=? AND revive_requested=? AND `+obligationOpen),
		append(values, o.id, o.state, o.fault, o.spent, o.labelled, o.nextAt, o.reviveRequested)...)
	return err
}

// obligationInboxSettled reports whether the receiver already has the
// obligation's inbox item (laneInboxSettled). An item whose delivery was
// recorded at or after deliveredBefore does not count yet: the hub records it
// just ahead of the handoffkeep write that would confirm the row.
func (s *laneInboxStore) obligationInboxSettled(parent context.Context, id int64, deliveredBefore time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var stored any
	err := s.db.QueryRowContext(ctx, s.q(`SELECT event_key FROM delivery_obligations WHERE id=?`), id).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	forms := s.dialect.storedKeyForms(stored)
	marks := strings.TrimSuffix(strings.Repeat("?,", len(forms)), ",")
	var n int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM lane_inbox WHERE event_key IN (`+marks+`) AND delivered_at<? AND `+laneInboxSettled), append(forms, deliveredBefore.UnixMilli())...).Scan(&n)
	return n > 0, err
}

// digestBatchReleased reports whether eventID is a digest or escalation batch
// of lane that no item is still claimed by: releaseBatchClaims would change
// nothing, so each item was read, acked, superseded, or released to a later
// batch. It only reads; releasing is the replay's act.
func (s *laneInboxStore) digestBatchReleased(parent context.Context, lane, eventID string) (bool, error) {
	isBatch, err := s.isDigestBatch(lane, eventID)
	if err != nil || !isBatch {
		return false, err
	}
	ctx, done := s.op(parent)
	defer done()
	var claimed int
	err = s.db.QueryRowContext(ctx, s.q(`SELECT (SELECT count(*) FROM lane_inbox WHERE digest_batch<>'' AND `+batchDigestClaim+`)+(SELECT count(*) FROM lane_inbox WHERE escalation_batch<>'' AND `+batchEscalationClaim+`)`), eventID, eventID).Scan(&claimed)
	return err == nil && claimed == 0, err
}
