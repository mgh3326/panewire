package panewire

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestLaneInboxPendingBatchesQueryUsesClaimIndexes guards the batch page's
// plan. The page counts each pending batch's items through digest_batch and
// escalation_batch; a correlated OR over those two columns cannot use an index
// on either, so every pending batch re-read the whole of lane_inbox (hk 4889).
// Both indexes are partial on <>”, and the statement repeats that predicate
// for the planner, so this fails if either half drifts back to a full scan.
func TestLaneInboxPendingBatchesQueryUsesClaimIndexes(t *testing.T) {
	s, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	rows, err := s.db.Query(`EXPLAIN QUERY PLAN ` + laneInboxPendingBatchesQuery + `rowid LIMIT 100`)
	if err != nil {
		t.Fatalf("explain batch page: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	for _, index := range []string{"lane_inbox_digest_batch", "lane_inbox_escalation_batch"} {
		if !strings.Contains(joined, index) {
			t.Fatalf("batch page plan does not search %s:\n%s", index, joined)
		}
	}
	if strings.Contains(joined, "SCAN lane_inbox\n") || strings.HasSuffix(joined, "SCAN lane_inbox") {
		t.Fatalf("batch page plan scans lane_inbox in full:\n%s", joined)
	}
}
