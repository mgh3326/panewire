package panewire

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// oldLaneInboxStore runs the pre-hk6219 queries with (?=” OR lane=?) predicates
// so it can serve as a reference oracle for table tests comparing old vs new.
type oldLaneInboxStore struct {
	*laneInboxStore
}

func (s *oldLaneInboxStore) list(lane string, unread bool) ([]laneInboxItem, error) {
	q := `SELECT ` + laneInboxColumns + ` FROM lane_inbox WHERE (?='' OR lane=?)`
	if unread {
		q += ` AND ` + laneInboxUnread
	}
	q += ` ORDER BY id DESC LIMIT 500`
	ctx, done := s.op(s.life)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	items, err := s.listItems(ctx, tx, q, lane, lane)
	if err != nil {
		return nil, err
	}
	if err := s.fillSurfacedAt(ctx, tx, items); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *oldLaneInboxStore) ackStates(eventIDs []string, sender string) (map[string]laneInboxAckState, error) {
	args := make([]any, 0, len(eventIDs)+2)
	marks := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		marks[i] = "?"
		args = append(args, id)
	}
	args = append(args, sender, sender)
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_id,lane,delivery_stage,delivery_reason,delivery_machine,delivery_pane,delivery_at,delivered_at,acked_at,superseded_at FROM lane_inbox WHERE event_id IN (`+strings.Join(marks, ",")+`) AND (?='' OR sender=?) ORDER BY event_id,id`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type aggregate struct {
		state   laneInboxAckState
		total   int64
		settled int64
		latest  int64
		at      int64
		rank    int
		have    bool
	}
	aggregates := map[string]*aggregate{}
	for rows.Next() {
		var eventID string
		var row laneInboxAckRow
		if err := rows.Scan(&eventID, &row.lane, &row.stage, &row.reason, &row.machine, &row.pane, &row.deliveryAt, &row.deliveredAt, &row.ackedAt, &row.supersededAt); err != nil {
			return nil, err
		}
		agg := aggregates[eventID]
		if agg == nil {
			agg = &aggregate{}
			aggregates[eventID] = agg
		}
		agg.total++
		settledAt := row.ackedAt
		if row.supersededAt > settledAt {
			settledAt = row.supersededAt
		}
		if settledAt > 0 {
			agg.settled++
			if settledAt > agg.latest {
				agg.latest = settledAt
			}
		}
		stage := laneInboxStageFor(row.stage, row.deliveredAt)
		at := row.deliveryAt
		if at == 0 {
			at = row.deliveredAt
		}
		rank := laneInboxStageRank(stage)
		if !agg.have || at > agg.at || (at == agg.at && rank > agg.rank) {
			agg.have, agg.at, agg.rank = true, at, rank
			agg.state.Lane, agg.state.Stage, agg.state.Reason = row.lane, stage, row.reason
			agg.state.Machine, agg.state.Pane = row.machine, row.pane
			agg.state.At = ""
			if at > 0 {
				agg.state.At = time.UnixMilli(at).UTC().Format(time.RFC3339)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	states := map[string]laneInboxAckState{}
	for id, agg := range aggregates {
		state := agg.state
		state.EventID = id
		state.Acked = agg.settled == agg.total
		if state.Acked {
			state.AckedAt = time.UnixMilli(agg.latest).UTC().Format(time.RFC3339)
		}
		states[id] = state
	}
	return states, nil
}

func (s *oldLaneInboxStore) read(id int64, lane string, now time.Time) (laneInboxItem, error) {
	ctx, done := s.op(s.life)
	defer done()
	updated, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE id=? AND (?='' OR lane=?)`), now.UnixMilli(), id, lane, lane)
	if err != nil {
		return laneInboxItem{}, err
	}
	if n, _ := s.dialect.rowsMatched(updated); n > 0 {
		s.noteBatchesChanged()
		s.noteWorkChanged()
	}
	return scanLaneInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE id=? AND (?='' OR lane=?)`), id, lane, lane))
}

func (s *oldLaneInboxStore) ack(id int64, lane string, all bool, now time.Time) (int64, error) {
	ctx, done := s.op(s.life)
	defer done()
	q := `UPDATE lane_inbox SET acked_at=?,read_at=CASE WHEN read_at=0 THEN ? ELSE read_at END WHERE acked_at=0 AND superseded_at=0 AND ((? AND (?='' OR lane=?)) OR (NOT ? AND id=? AND (?='' OR lane=?)))`
	result, err := s.db.ExecContext(ctx, s.q(q), now.UnixMilli(), now.UnixMilli(), all, lane, lane, all, id, lane, lane)
	if err != nil {
		return 0, err
	}
	acked, err := s.dialect.rowsMatched(result)
	if err != nil {
		return 0, err
	}
	if acked > 0 {
		s.noteBatchesChanged()
		s.noteWorkChanged()
	}
	return acked, nil
}

func (s *oldLaneInboxStore) counts(lane string) (map[string]int, error) {
	ctx, done := s.op(s.life)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT lane,count(*) FROM lane_inbox WHERE `+laneInboxUnread+` AND (?='' OR lane=?) GROUP BY lane`), lane, lane)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	if lane != "" {
		counts[lane] = 0
	}
	for rows.Next() {
		var l string
		var n int
		if err := rows.Scan(&l, &n); err != nil {
			return nil, err
		}
		counts[l] = n
	}
	return counts, rows.Err()
}

// TestLaneInboxStoreOptionalLaneMatchesOldOracle verifies that for each site
// (list, ackStates, read, ack, counts) with lane empty and lane given,
// the refactored Go WHERE building returns exactly what the old (?=” OR lane=?)
// queries returned across backends.
//
// Each case opens fresh storage for both stores, so no case inherits rows that
// an earlier case read or acked (hk 6221).
func TestLaneInboxStoreOptionalLaneMatchesOldOracle(t *testing.T) {
	for _, backend := range laneInboxBackends() {
		t.Run(backend.name, func(t *testing.T) {
			// seed puts five unread rows in lanes lane-a, lane-b, lane-c, lane-a, lane-b.
			seed := func(t *testing.T, s *laneInboxStore) ([]int64, []string) {
				now := time.UnixMilli(1_700_000_000_000).UTC()
				var ids []int64
				var eventIDs []string
				lanes := []string{"lane-a", "lane-b", "lane-c", "lane-a", "lane-b"}
				for i, lane := range lanes {
					key := fmt.Sprintf("ev-%d", i+1)
					evID := fmt.Sprintf("event-id-%d", i+1)
					item, err := s.put(key, lane, "lane.event", "inbox", quietEvent(key, "hello"), now, 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, item.ID)
					eventIDs = append(eventIDs, evID)
					sender := "sender-1"
					if i%2 == 0 {
						sender = "sender-2"
					}
					if _, err := s.db.Exec(s.q(`UPDATE lane_inbox SET event_id=?, sender=? WHERE id=?`), evID, sender, item.ID); err != nil {
						t.Fatal(err)
					}
				}
				return ids, eventIDs
			}

			// freshPair opens a new store and an old-oracle store over fresh,
			// identically seeded storage.
			freshPair := func(t *testing.T) (sNew *laneInboxStore, oldStore *oldLaneInboxStore, idsNew, idsOld []int64, eventIDs []string) {
				t.Helper()
				sNew = backend.fresh(t)()
				sOld := backend.fresh(t)()
				idsNew, eventIDs = seed(t, sNew)
				idsOld, _ = seed(t, sOld)
				return sNew, &oldLaneInboxStore{laneInboxStore: sOld}, idsNew, idsOld, eventIDs
			}

			// readAndAck marks row 0 (lane-a) read and row 1 (lane-b) acked, so the
			// populated lanes hold both read and unread rows and unread=true differs
			// from unread=false. It only sets up state; both stores get the same setup.
			readAndAck := func(t *testing.T, s *laneInboxStore, ids []int64) {
				t.Helper()
				now := time.UnixMilli(1_700_000_050_000).UTC()
				if _, err := s.read(ids[0], "", now); err != nil {
					t.Fatal(err)
				}
				if _, err := s.ack(ids[1], "", false, now); err != nil {
					t.Fatal(err)
				}
			}

			// 1. Test list (lane empty vs lane given, unread true and false)
			t.Run("list", func(t *testing.T) {
				sNew, oldStore, idsNew, idsOld, _ := freshPair(t)
				readAndAck(t, sNew, idsNew)
				readAndAck(t, oldStore.laneInboxStore, idsOld)

				allRows, err := oldStore.list("", false)
				if err != nil {
					t.Fatal(err)
				}
				unreadRows, err := oldStore.list("", true)
				if err != nil {
					t.Fatal(err)
				}
				if len(allRows) == len(unreadRows) {
					t.Fatalf("fixture does not distinguish unread: both return %d rows", len(allRows))
				}

				for _, lane := range []string{"", "lane-a", "lane-b", "lane-c", "lane-z"} {
					for _, unread := range []bool{false, true} {
						got, errGot := sNew.list(lane, unread)
						want, errWant := oldStore.list(lane, unread)
						if (errGot != nil) != (errWant != nil) {
							t.Fatalf("lane=%q unread=%v: error mismatch: got %v, want %v", lane, unread, errGot, errWant)
						}
						if len(got) != len(want) {
							t.Fatalf("lane=%q unread=%v: count mismatch: got %d, want %d", lane, unread, len(got), len(want))
						}
						for i := range got {
							if got[i].ID != want[i].ID || got[i].Lane != want[i].Lane {
								t.Fatalf("lane=%q unread=%v row %d mismatch: got %+v, want %+v", lane, unread, i, got[i], want[i])
							}
						}
					}
				}
			})

			// 2. Test ackStates (sender empty vs sender given)
			t.Run("ackStates", func(t *testing.T) {
				sNew, oldStore, _, _, evIDs := freshPair(t)

				for _, sender := range []string{"", "sender-1", "sender-2", "sender-z"} {
					got, errGot := sNew.ackStates(evIDs, sender)
					want, errWant := oldStore.ackStates(evIDs, sender)
					if (errGot != nil) != (errWant != nil) {
						t.Fatalf("sender=%q: error mismatch: got %v, want %v", sender, errGot, errWant)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("sender=%q: result mismatch:\ngot:  %+v\nwant: %+v", sender, got, want)
					}
				}
			})

			// 3. Test counts (lane empty vs lane given)
			t.Run("counts", func(t *testing.T) {
				sNew, oldStore, idsNew, idsOld, _ := freshPair(t)
				readAndAck(t, sNew, idsNew)
				readAndAck(t, oldStore.laneInboxStore, idsOld)

				for _, lane := range []string{"", "lane-a", "lane-b", "lane-c", "lane-z"} {
					got, errGot := sNew.counts(lane)
					want, errWant := oldStore.counts(lane)
					if (errGot != nil) != (errWant != nil) {
						t.Fatalf("lane=%q: error mismatch: got %v, want %v", lane, errGot, errWant)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("lane=%q: counts mismatch: got %v, want %v", lane, got, want)
					}
				}
			})

			// 4. Test read (lane empty vs correct lane vs wrong lane). Each case
			// reads row 0, which is in lane-a.
			t.Run("read", func(t *testing.T) {
				now := time.UnixMilli(1_700_000_100_000).UTC()
				for _, tc := range []struct {
					name      string
					lane      string
					populated bool // the filter selects row 0, so read must change it
				}{
					{"empty lane", "", true},
					{"matching lane", "lane-a", true},
					{"mismatched lane", "lane-b", false},
					{"nonexistent lane", "lane-z", false},
				} {
					t.Run(tc.name, func(t *testing.T) {
						sNew, oldStore, idsNew, idsOld, _ := freshPair(t)

						got, errGot := sNew.read(idsNew[0], tc.lane, now)
						want, errWant := oldStore.read(idsOld[0], tc.lane, now)
						if (errGot != nil) != (errWant != nil) {
							t.Fatalf("error mismatch: got %v, want %v", errGot, errWant)
						}
						if errGot == nil {
							if got.ID != want.ID || got.Lane != want.Lane || got.ReadAt != want.ReadAt {
								t.Fatalf("read mismatch: got %+v, want %+v", got, want)
							}
						}
						if tc.populated && (errGot != nil || got.ReadAt != now.UnixMilli()) {
							t.Fatalf("populated lane but read changed nothing: got %+v, err %v", got, errGot)
						}
					})
				}
			})

			// 5. Test ack (single item and all=true, lane empty vs given)
			t.Run("ack", func(t *testing.T) {
				now := time.UnixMilli(1_700_000_200_000).UTC()
				for _, all := range []bool{false, true} {
					for _, lane := range []string{"", "lane-a", "lane-b", "lane-z"} {
						t.Run(fmt.Sprintf("all=%v/lane=%q", all, lane), func(t *testing.T) {
							sNew, oldStore, idsNew, idsOld, _ := freshPair(t)

							var targetIDNew, targetIDOld int64
							if !all {
								targetIDNew = idsNew[0]
								targetIDOld = idsOld[0]
							}

							got, errGot := sNew.ack(targetIDNew, lane, all, now)
							want, errWant := oldStore.ack(targetIDOld, lane, all, now)
							if (errGot != nil) != (errWant != nil) {
								t.Fatalf("error mismatch: got %v, want %v", errGot, errWant)
							}
							if got != want {
								t.Fatalf("targetIDNew=%d targetIDOld=%d: ack count mismatch: got %d, want %d", targetIDNew, targetIDOld, got, want)
							}
							// Lanes lane-a and "" select row 0, so these cases must ack something.
							if (lane == "" || lane == "lane-a") && got == 0 {
								t.Fatalf("populated lane acked no rows; the table is drained")
							}
						})
					}
				}
			})
		})
	}
}

// TestLaneInboxStoreNoOptionalPredicateInGeneratedSQL records every statement
// executed during list, ackStates, read, ack, and counts when lane/sender is given
// and when empty, asserting that none contains `?=”` or `”=`.
func TestLaneInboxStoreNoOptionalPredicateInGeneratedSQL(t *testing.T) {
	path := freshLaneInboxFile(t)
	s, err := openLaneInboxStoreTimed(t, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })

	now := time.UnixMilli(1_700_000_000_000).UTC()
	item, err := s.put("k1", "lane-a", "lane.event", "inbox", quietEvent("k1", "hello"), now, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE lane_inbox SET event_id='ev-1', sender='sender-1' WHERE id=?`, item.ID); err != nil {
		t.Fatal(err)
	}

	rec := &statementRecorder{}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(recordingConnector{inner: raw.Driver(), dsn: path, rec: rec})
	raw.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	oldDB := s.db
	s.db = db
	oldDB.Close()
	rec.take() // clear setup statements

	// Exercise each call with lane/sender empty and with lane/sender provided
	_, _ = s.list("", false)
	_, _ = s.list("lane-a", false)
	_, _ = s.list("", true)
	_, _ = s.list("lane-a", true)

	_, _ = s.ackStates([]string{"ev-1"}, "")
	_, _ = s.ackStates([]string{"ev-1"}, "sender-1")

	_, _ = s.counts("")
	_, _ = s.counts("lane-a")

	_, _ = s.read(item.ID, "", now)
	_, _ = s.read(item.ID, "lane-a", now)

	_, _ = s.ack(item.ID, "", false, now)
	_, _ = s.ack(item.ID, "lane-a", false, now)
	_, _ = s.ack(0, "", true, now)
	_, _ = s.ack(0, "lane-a", true, now)

	queries := rec.take()
	if len(queries) == 0 {
		t.Fatal("expected recorded queries, got none")
	}

	forbidden := []string{"?=''", "''=", "? = ''", "'' = ?"}
	for _, q := range queries {
		for _, f := range forbidden {
			if strings.Contains(q, f) {
				t.Fatalf("generated SQL contains forbidden optional-lane predicate %q:\n%s", f, q)
			}
		}
	}
}
