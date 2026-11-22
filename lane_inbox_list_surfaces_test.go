package panewire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"sync"
	"testing"
	"time"
)

// listSurfacedAtOracle is the correlated subquery list() ran before hk 6080,
// with one lane_inbox_surfaces read per output row. It stays here as the
// oracle: list() must give every item the SurfacedAt this query gives it.
const listSurfacedAtOracle = `SELECT id,(SELECT COALESCE(max(delivered_at),0) FROM lane_inbox_surfaces surfaced WHERE surfaced.lane=lane_inbox.lane AND surfaced.through_id>=lane_inbox.id) FROM lane_inbox`

// TestLaneInboxListSurfacedAtMatchesCorrelatedOracle runs list() on every
// backend and compares each item with the oracle. The fixture puts a row at
// exactly each item's id (through_id == id), so an off-by-one on >= drops that
// row and changes the answer: a1, a2 and b1 each take their maximum from such
// a row. A second row with the same through_id, an undelivered row, and a
// lower delivery at a later through_id keep the running maximum honest.
func TestLaneInboxListSurfacedAtMatchesCorrelatedOracle(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		ids := map[string]int64{}
		put := func(key, lane string) {
			t.Helper()
			item, err := s.put(key, lane, "lane.event", "inbox", quietEvent(key, "FYI "+key), now, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			ids[key] = item.ID
		}
		// The lanes interleave, so no lane's ids are contiguous.
		put("a1", "lane-a")
		put("b1", "lane-b")
		put("a2", "lane-a")
		put("b2", "lane-b")
		put("a3", "lane-a")
		put("a4", "lane-a")
		// surface saves a summary through the named item and, when deliveredMs
		// is positive, records its delivery; zero leaves it undelivered.
		surface := func(summary, lane, through string, deliveredMs int64) {
			t.Helper()
			if _, err := s.saveInboxSummary(summary, lane, "summary "+summary, ids[through]); err != nil {
				t.Fatal(err)
			}
			if deliveredMs > 0 {
				if err := s.surfaceInboxSummary(summary, time.UnixMilli(deliveredMs)); err != nil {
					t.Fatal(err)
				}
			}
		}
		surface("sum-a1", "lane-a", "a1", 900)
		surface("sum-a2", "lane-a", "a2", 800)
		surface("sum-a2-again", "lane-a", "a2", 1000)
		surface("sum-a3", "lane-a", "a3", 700)
		surface("sum-a4", "lane-a", "a4", 600)
		surface("sum-a4-undelivered", "lane-a", "a4", 0)
		surface("sum-b2", "lane-b", "b2", 50)
		surface("sum-b1", "lane-b", "b1", 70)

		// Check the oracle against the values the fixture implies first, so a
		// wrong fixture fails here and not as a list mismatch.
		want := map[int64]int64{ids["a1"]: 1000, ids["a2"]: 1000, ids["a3"]: 700, ids["a4"]: 600, ids["b1"]: 70, ids["b2"]: 50}
		oracle := map[int64]int64{}
		rows, err := s.db.Query(s.q(listSurfacedAtOracle))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, surfaced int64
			if err := rows.Scan(&id, &surfaced); err != nil {
				t.Fatal(err)
			}
			oracle[id] = surfaced
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		for id, v := range want {
			if oracle[id] != v {
				t.Fatalf("oracle SurfacedAt for item %d = %d, want %d", id, oracle[id], v)
			}
		}

		for _, call := range []struct {
			name   string
			lane   string
			unread bool
			count  int
		}{
			{"all lanes", "", false, 6},
			{"lane-a", "lane-a", false, 4},
			{"lane-b unread", "lane-b", true, 2},
			{"all lanes unread", "", true, 6},
		} {
			items, err := s.list(call.lane, call.unread)
			if err != nil {
				t.Fatalf("%s: list: %v", call.name, err)
			}
			if len(items) != call.count {
				t.Fatalf("%s: %d items, want %d", call.name, len(items), call.count)
			}
			for _, item := range items {
				if item.SurfacedAt != oracle[item.ID] {
					t.Fatalf("%s: item %d (lane %s) SurfacedAt = %d, oracle %d", call.name, item.ID, item.Lane, item.SurfacedAt, oracle[item.ID])
				}
			}
		}
	})
}

// statementRecorder keeps the text of each statement a connection prepares.
type statementRecorder struct {
	mu      sync.Mutex
	queries []string
}

func (r *statementRecorder) record(q string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, q)
}

// take returns the statements recorded since the last take and clears them.
func (r *statementRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.queries
	r.queries = nil
	return out
}

// recordingConnector opens SQLite connections whose statements are recorded.
// The wrapper hides the driver's direct query methods, so database/sql prepares
// every statement through Prepare, where it is recorded.
type recordingConnector struct {
	inner driver.Driver
	dsn   string
	rec   *statementRecorder
}

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.inner.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return recordingConn{Conn: conn, rec: c.rec}, nil
}

func (c recordingConnector) Driver() driver.Driver { return c.inner }

type recordingConn struct {
	driver.Conn
	rec *statementRecorder
}

func (c recordingConn) Prepare(q string) (driver.Stmt, error) {
	c.rec.record(q)
	return c.Conn.Prepare(q)
}

// TestLaneInboxListReadsSurfacesOncePerCall counts the statements list() sends
// on SQLite. Each call reads the items in one statement and the surfaced
// summaries of the page's lanes in one more, however many items the page holds.
// Before hk 6080 one statement read both, with a surfaces read for every row.
// A lane with no items on the page reads no surfaces at all. The statements are
// SQLite's; the Go that builds them is shared by every backend, which
// TestLaneInboxListSurfacedAtMatchesCorrelatedOracle covers.
func TestLaneInboxListReadsSurfacesOncePerCall(t *testing.T) {
	path := freshLaneInboxFile(t)
	s, err := openLaneInboxStoreTimed(t, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	now := time.UnixMilli(1_700_000_000_000).UTC()
	for _, key := range []string{"a1", "a2", "a3", "b1", "b2"} {
		lane := "lane-a"
		if key[0] == 'b' {
			lane = "lane-b"
		}
		item, err := s.put(key, lane, "lane.event", "inbox", quietEvent(key, "FYI "+key), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.saveInboxSummary("sum-"+key, lane, "summary "+key, item.ID); err != nil {
			t.Fatal(err)
		}
		if err := s.surfaceInboxSummary("sum-"+key, now); err != nil {
			t.Fatal(err)
		}
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
	old := s.db
	s.db = db
	old.Close()
	rec.take() // the setup above is not list's traffic

	itemRead := regexp.MustCompile(`FROM lane_inbox\b`)
	surfaceRead := regexp.MustCompile(`FROM lane_inbox_surfaces\b`)
	for _, call := range []struct {
		name         string
		lane         string
		count        int
		wantSurfaces int
	}{
		{"all lanes", "", 5, 1},
		{"lane-a", "lane-a", 3, 1},
		{"lane with no items", "lane-z", 0, 0},
	} {
		items, err := s.list(call.lane, false)
		if err != nil {
			t.Fatalf("%s: list: %v", call.name, err)
		}
		if len(items) != call.count {
			t.Fatalf("%s: %d items, want %d", call.name, len(items), call.count)
		}
		var itemReads, surfaceReads int
		for _, q := range rec.take() {
			isItem, isSurface := itemRead.MatchString(q), surfaceRead.MatchString(q)
			if isItem && isSurface {
				t.Fatalf("%s: one statement reads both lane_inbox and lane_inbox_surfaces:\n%s", call.name, q)
			}
			if isItem {
				itemReads++
			}
			if isSurface {
				surfaceReads++
			}
		}
		if itemReads != 1 || surfaceReads != call.wantSurfaces {
			t.Fatalf("%s: %d item reads and %d surfaces reads, want 1 and %d", call.name, itemReads, surfaceReads, call.wantSurfaces)
		}
	}
}
