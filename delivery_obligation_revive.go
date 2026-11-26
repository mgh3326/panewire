package panewire

// The one revived copy of a working_wait_expired obligation (hk 6820, hk 4289
// T3). An idle wake only asks for it, on the obligation; the next pass takes
// it. The request and the copy are both durable, so a restart between them
// loses neither, and revived bounds the row to one such copy in its life.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

const (
	// obligationReviveBackoff is the wait after a revived copy before the row
	// is read again. It is also how long an untaken request holds back the next
	// row of the pane, so one that cannot be queued does not starve the rest.
	obligationReviveBackoff = 5 * time.Minute
	// obligationReviveRecheck is the wait for a revive the node's own hold, an
	// open send or a registration still blocks. Each of these clears in moments.
	obligationReviveRecheck = 10 * time.Second
)

// dropWindowOpen reports whether a working-expired drop recorded at
// droppedAtMillis is inside the replay window. A revive's age is counted from
// the drop, not the receipt, since the node held the row up to
// relay.cmd_hold_max.
func dropWindowOpen(now time.Time, droppedAtMillis int64) bool {
	return now.Sub(time.UnixMilli(droppedAtMillis)) <= relayReplayMaxAge
}

// revivable reports whether a requested revive may still take its copy: it has
// not been taken, the text is not maybe typed, the lane's job has not ended,
// and the drop window is open.
func (o openObligation) revivable(now time.Time) bool {
	class := classifyObligationFault(o.fault)
	return o.reviveRequested > 0 && o.revived == 0 &&
		(class == obligationWorkingExpired || class == obligationSafe) &&
		dropWindowOpen(now, o.lastFaultAt)
}

// requestObligationRevive asks for the revived copy of one row: the oldest
// working_wait_expired lane.event of lanes still inside its window. While a
// recent request among them is untaken no further row is asked for, so two
// rows are not queued into one idle turn. It reports whether a row was marked.
func (s *laneInboxStore) requestObligationRevive(parent context.Context, lanes []string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	marks := strings.TrimSuffix(strings.Repeat("?,", len(lanes)), ",")
	eligible := obligationOpen + ` AND kind='lane.event' AND fault=? AND revived=0 AND lane IN (` + marks + `)`
	args := []any{relayDroppedWorkingExpired}
	for _, lane := range lanes {
		args = append(args, lane)
	}
	var pending int
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM delivery_obligations WHERE `+eligible+` AND revive_requested>?`),
		append(args, now.Add(-obligationReviveBackoff).UnixMilli())...).Scan(&pending); err != nil || pending > 0 {
		return false, err
	}
	var id int64
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id FROM delivery_obligations WHERE `+eligible+` AND revive_requested=0 AND last_fault_at>=? ORDER BY relay_id,id LIMIT 1`),
		append(args, now.Add(-relayReplayMaxAge).UnixMilli())...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// next_at=0 puts the row at the head of the next pass.
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET revive_requested=?,next_at=0,updated_at=?
		WHERE id=? AND fault=? AND revived=0 AND revive_requested=0 AND `+obligationOpen), now.UnixMilli(), now.UnixMilli(), id, relayDroppedWorkingExpired)
	if err != nil {
		return false, err
	}
	count, err := s.dialect.rowsMatched(result)
	return count == 1, err
}

// spendObligationRevive claims the one revived copy with a compare-and-swap,
// before any handoffkeep call or queue operation. It leaves spent alone: the
// revive is not a budget retry, so a spent budget does not refuse it.
func (s *laneInboxStore) spendObligationRevive(parent context.Context, o openObligation, now, next time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET state='`+obligationOwed+`',incident_id=0,revived=1,labelled=labelled+1,next_at=?,updated_at=?
		WHERE id=? AND state=? AND revived=0 AND revive_requested=? AND spent=? AND labelled=? AND next_at=? AND fault=?`),
		next.UnixMilli(), now.UnixMilli(), o.id, o.state, o.reviveRequested, o.spent, o.labelled, o.nextAt, o.fault)
	if err != nil {
		return false, err
	}
	count, err := s.dialect.rowsMatched(result)
	return count == 1, err
}

// releaseObligationRevive returns the revive and its label when nothing was
// queued. The request stays, so a pass at due tries again.
func (s *laneInboxStore) releaseObligationRevive(parent context.Context, o openObligation, next, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET revived=0,labelled=labelled-1,next_at=?
		WHERE id=? AND state='`+obligationOwed+`' AND revived=1 AND labelled=? AND next_at=?`),
		due.UnixMilli(), o.id, o.labelled+1, next.UnixMilli())
	return err
}
