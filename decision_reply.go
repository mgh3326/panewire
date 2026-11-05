package panewire

import "context"

// DecisionReplyAfter reads only proven prompt submissions and delivered lane
// relays, using the daemon's own store regardless of its configured path.
func (s *Store) DecisionReplyAfter(ctx context.Context, pane, lane string, sinceMS int64) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (
  SELECT 1 FROM deliveries WHERE resolved_pane_id = ? AND completed_at_ms > ?
   AND submission_result NOT IN ('', 'unproven', 'composer_residue')
  UNION ALL
  SELECT 1 FROM relay_delivered WHERE pane = ? AND lane = ? AND lane <> '' AND delivered_at > ?
 )`, pane, sinceMS, pane, lane, sinceMS).Scan(&found)
	return found, err
}
