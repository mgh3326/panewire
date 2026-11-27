package panewire

// hk 6935 (round 4 review of #935): a node's drop is one UPDATE, and it must
// assign each column once. Postgres rejects a column assigned twice in one SET
// list, and a job_ended or maybe-typed drop did so on this branch. The first
// test checks the statement text for every fault, so it runs on any build; the
// second runs the same drops through each store backend, Postgres and MySQL
// included when CI sets their URLs.

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// obligationStatementColumns are the columns a drop's SET list assigns.
var obligationStatementColumns = []string{"next_at", "incident_id", "notice_due_at", "state", "last_fault_at", "fault_reason", "fault"}

// obligationAssignment matches one assignment of a SET list: the column, then
// its CASE or the job_ended literal `notice_due_at=0,`. A predicate such as
// `notice_due_at=0 AND` is not an assignment.
var obligationAssignment = regexp.MustCompile(`(?:^|[\s,])(\w+)=(?:CASE\b|0,)`)

func TestObligationDropStatementAssignsEachColumnOnce(t *testing.T) {
	keys := []any{int64(7), "lane-a"}
	for _, fault := range []string{
		relayDroppedJobEnded,
		relayDroppedMaybeInPane,
		relayDroppedCheckPane,
		obligationFaultUnproven,
		obligationFaultPromptUnproven,
		obligationFaultReadUnproven,
		relayDroppedPaneReadFailed,
		relayDroppedRetriesExhausted,
		relayDroppedWorkingExpired,
	} {
		for _, harness := range []string{"claude", "devin", ""} {
			t.Run(fault+"/"+harness, func(t *testing.T) {
				query, args := obligationDropStatement(fault, harness, time.UnixMilli(1000), "relay_id=? AND lane=?", keys)
				_, set, ok := strings.Cut(query, "SET")
				if !ok {
					t.Fatalf("no SET list in %q", query)
				}
				set, _, _ = strings.Cut(set, "WHERE")
				counts := map[string]int{}
				for _, m := range obligationAssignment.FindAllStringSubmatch(set, -1) {
					if slices.Contains(obligationStatementColumns, m[1]) {
						counts[m[1]]++
					}
				}
				for column, n := range counts {
					if n != 1 {
						t.Fatalf("%s is assigned %d times in one SET list: %s", column, n, query)
					}
				}
				// The placeholders in the statement, not the arguments, are what the
				// driver binds, so they must match one for one.
				if marks := strings.Count(query, "?"); marks != len(args) {
					t.Fatalf("%d placeholders for %d arguments: %s", marks, len(args), query)
				}
			})
		}
	}
}

// TestObligationStoreContractDropStatements runs a job_ended drop, a maybe-typed
// drop and a safe drop through each backend's store. The statement must run on
// every dialect and leave the fault and the due mark the drop names.
func TestObligationStoreContractDropStatements(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			s := backend.fresh(t)()
			ctx := t.Context()
			now := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
			row := func(relayID int64) {
				t.Helper()
				key := "lane.event/lane-a/drop-" + strconv.FormatInt(relayID, 10)
				o := newDeliveryObligation{key: key, relayID: relayID, kind: "lane.event", lane: "lane-a", machine: "host-a", pane: "w1:p1", attempts: 1}
				if err := s.flagObligationRetry(ctx, o, obligationAckTimeout, "", now); err != nil {
					t.Fatal(err)
				}
			}
			stored := func(relayID int64) (fault string, due int64) {
				t.Helper()
				if err := s.db.QueryRowContext(ctx, s.q(`SELECT fault,notice_due_at FROM delivery_obligations WHERE relay_id=?`), relayID).Scan(&fault, &due); err != nil {
					t.Fatal(err)
				}
				return fault, due
			}
			drop := func(relayID int64, reason, harness string) {
				t.Helper()
				if _, err := s.noteObligationDrop(ctx, relayID, "lane-a", "host-a", true, reason, harness, now); err != nil {
					t.Fatalf("drop %s on %s: %v", reason, backend.name, err)
				}
			}

			row(21)
			drop(21, relayDroppedJobEnded, "claude")
			if fault, due := stored(21); fault != relayDroppedJobEnded || due != 0 {
				t.Fatalf("job_ended drop stored fault=%q due=%d", fault, due)
			}

			row(22)
			drop(22, relayDroppedMaybeInPane, "claude")
			if fault, due := stored(22); fault != relayDroppedMaybeInPane || due == 0 {
				t.Fatalf("maybe-typed drop stored fault=%q due=%d, want the fault with a due notice", fault, due)
			}

			row(23)
			drop(23, relayDroppedPaneReadFailed, "claude")
			if fault, due := stored(23); fault != relayDroppedPaneReadFailed || due != 0 {
				t.Fatalf("safe drop stored fault=%q due=%d", fault, due)
			}
		})
	}
}
