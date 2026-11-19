package panewire

import "context"

// A reviewer holds no task/role reservation (hk 4737). Reservations written by
// an older hub would outlive the rule: the stored key still names the reviewer
// and every reader of task_role would keep treating it as a held slot, so a
// later review of the same task would be refused. The startup pass clears them
// once, before the hub serves a request. It is data-only (decision 254): no
// table or column changes, and a second run finds nothing to clear.
//
// A reviewer's key is "<task>:reviewer" or "<task>:reviewer:p<N>", the exact
// shape fleet.Spawn.RoleKey writes, so the stored text is the whole test — no
// request payload is parsed and an unreadable old request cannot leave a
// reviewer slot behind.
//
// Rolling back to an older hub is safe: it reads a NULL task_role as no slot,
// so these rows simply hold nothing. A reviewer that a rolled-back hub spawns
// afterwards takes its old slot again, which is the older behaviour, not a
// broken row.
func (s *laneInboxStore) clearReviewerReservations() (int64, error) {
	ctx, done := s.op(context.Background())
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_reservations SET task_role=NULL WHERE task_role LIKE '%:reviewer' OR task_role LIKE '%:reviewer:p_'`))
	if err != nil {
		return 0, err
	}
	return s.dialect.rowsMatched(result)
}
