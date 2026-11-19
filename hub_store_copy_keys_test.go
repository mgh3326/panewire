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
			key := form + strings.Repeat("x", mysqlEventKeyMax)
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
		})
	}
}
