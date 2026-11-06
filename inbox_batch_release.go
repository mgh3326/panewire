package panewire

import "strings"

// quietDigestBatchPrefix starts the id makeBatches gives every digest and
// escalation batch. Producers may choose any event id, so the prefix alone
// never marks a row as a batch.
const quietDigestBatchPrefix = "quiet-digest-"

// isDigestBatch reports whether event id is a digest or escalation batch of
// lane that the inbox store made (never a held batch, which carries a document
// the receiver cannot rebuild from counts). lane_inbox_batches is the only
// authority: retention prunes lane_inbox but never this table.
func (s *laneInboxStore) isDigestBatch(lane, id string) (bool, error) {
	if strings.HasPrefix(id, "held-") {
		return false, nil
	}
	ctx, done := s.op(s.life)
	defer done()
	var n int
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM lane_inbox_batches WHERE id=? AND lane=?`), id, lane).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// releaseBatchClaims returns the items a sent batch still claims to the
// unclaimed pool, so the next makeBatches escalates them again with the count
// that is true then. Items the receiver has since read, acked or had
// superseded stay as they are. Releasing twice, or for a batch nothing claims,
// changes nothing.
func (s *laneInboxStore) releaseBatchClaims(id string) error {
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, release := range []string{
		`UPDATE lane_inbox SET digest_batch='' WHERE digest_batch=? AND acked_at=0 AND superseded_at=0`,
		`UPDATE lane_inbox SET escalation_batch='' WHERE escalation_batch=? AND read_at=0 AND acked_at=0 AND superseded_at=0`,
	} {
		if _, err := tx.ExecContext(ctx, s.q(release), id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
