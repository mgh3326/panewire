package panewire

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

func TestLaneInboxStorePostgresCopyBackLongKey(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	s, err := openLaneInboxStorePostgresConfig(t.Context(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	for _, form := range []string{"original", "digest"} {
		t.Run(form, func(t *testing.T) {
			eventID := form + strings.Repeat("x", mysqlEventKeyMax)
			key := relayLaneEventOutboxKey("lane-a", eventID)
			event := quietEvent(form, "test")
			first, err := s.put(key, "lane-a", "lane.event", "inbox", event, time.Now(), 0, 0)
			if err != nil || !first.Fresh {
				t.Fatalf("insert: fresh=%v err=%v", first.Fresh, err)
			}
			if form == "digest" {
				digest := hex.EncodeToString(dialectMySQL.storedKey(key).([]byte))
				if _, err := s.db.ExecContext(t.Context(), `UPDATE lane_inbox SET event_key=$1 WHERE id=$2`, digest, first.ID); err != nil {
					t.Fatal(err)
				}
			}
			again, err := s.put(key, "lane-a", "lane.event", "inbox", event, time.Now(), 0, 0)
			if err != nil || again.Fresh || again.ID != first.ID {
				t.Fatalf("replay: id=%d fresh=%v err=%v, want id=%d", again.ID, again.Fresh, err, first.ID)
			}
			held, err := s.holds(key)
			if err != nil || !held {
				t.Fatalf("holds: %v %v", held, err)
			}
			id, err := s.rowIDForKey(key)
			if err != nil || id != first.ID {
				t.Fatalf("row lookup: %d %v", id, err)
			}
			owned, err := s.poolNoticeOwned(t.Context(), "lane-a", eventID)
			if err != nil || !owned {
				t.Fatalf("notice ownership: %v %v", owned, err)
			}
			if err := s.recordStage(key, "held", "reason", "machine", "pane", time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := s.linkRelayID(key, 123); err != nil {
				t.Fatal(err)
			}
			var stage string
			var relay int64
			if err := s.db.QueryRowContext(t.Context(), `SELECT delivery_stage,relay_id FROM lane_inbox WHERE id=$1`, first.ID).Scan(&stage, &relay); err != nil || stage != "held" || relay != 123 {
				t.Fatalf("stage/relay: %q %d %v", stage, relay, err)
			}
			if _, err := s.db.ExecContext(t.Context(), `UPDATE lane_inbox SET class='interrupt',read_at=0 WHERE id=$1`, first.ID); err != nil {
				t.Fatal(err)
			}
			filed, err := s.fileEventUnread(key, "lane-a", "lane.event", event, time.Now())
			if err != nil || len(filed) != 1 || filed[0].ID != first.ID {
				t.Fatalf("file unread: %+v %v", filed, err)
			}
			if err := s.recordDelivery(key, "machine", "pane", time.Now()); err != nil {
				t.Fatal(err)
			}
			settled, err := s.settled(key)
			if err != nil || !settled {
				t.Fatalf("settled: %v %v", settled, err)
			}
		})
	}
}

func TestHubStoreCopyLongKeyForms(t *testing.T) {
	key := "event" + strings.Repeat("x", mysqlEventKeyMax)
	keys := dialectPostgres.storedKeys(key)
	if len(keys) != 2 || keys[0] != dialectPostgres.storedKey(key) || keys[1] != hex.EncodeToString(dialectMySQL.storedKey(key).([]byte)) {
		t.Fatalf("Postgres lookup forms: %v", keys)
	}
	for _, dialect := range []sqlDialect{dialectSQLite, dialectMySQL} {
		if got := dialect.storedKeys(key); len(got) != 1 {
			t.Fatalf("unexpected lookup forms: %v", got)
		}
	}
}
