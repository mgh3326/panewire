package panewire

import (
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 6486: obligationInboxSettled joined the inbox item on the obligation's
// stored key alone. Killed by: matching only that one form, which misses the
// digest a copied-back Postgres item keeps for an oversized key.
func TestStoredEventKeyFormsPostgres_t6486(t *testing.T) {
	long := relayLaneEventOutboxKey("lane-a", strings.Repeat("x", mysqlEventKeyMax))
	short := relayLaneEventOutboxKey("lane-a", "short")
	digest := hex.EncodeToString(dialectMySQL.storedKey(long).([]byte))
	cases := []struct {
		name    string
		dialect sqlDialect
		key     string
		want    []string
	}{
		{"postgres oversized, raw stored", dialectPostgres, long, []string{hex.EncodeToString([]byte(long)), digest}},
		{"postgres short", dialectPostgres, short, []string{hex.EncodeToString([]byte(short))}},
		{"mysql oversized digest", dialectMySQL, long, []string{string(dialectMySQL.storedKey(long).([]byte))}},
		{"sqlite", dialectSQLite, short, []string{short}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, form := range c.dialect.storedKeyForms(c.dialect.storedKey(c.key)) {
				got = append(got, string(storeCopyBytes(form)))
			}
			if !slices.Equal(got, c.want) {
				t.Fatalf("forms %q, want %q", got, c.want)
			}
		})
	}
	// A Postgres form that is not hex is kept as it is, so the query still
	// compares the stored value.
	if forms := dialectPostgres.storedKeyForms("not-hex!"); len(forms) != 1 || forms[0] != "not-hex!" {
		t.Fatalf("non-hex stored key forms %v", forms)
	}
	// Each form keeps the type the dialect binds: SQLite's text stays text.
	if forms := dialectSQLite.storedKeyForms("short"); len(forms) != 1 || forms[0] != "short" {
		t.Fatalf("sqlite forms %v", forms)
	}
}

// hk 6486, CI only (Postgres service container): a copied-back Postgres store
// keeps the digest form of an oversized key on the lane_inbox item, while an
// obligation created afterwards stores the raw key. The item delivered to the
// receiver must still settle the obligation. Killed by: an exact-form join.
func TestObligationStoreContractSettlesDigestItem_t6486(t *testing.T) {
	config, _ := hubPostgresTestSchema(t)
	s, err := openLaneInboxStorePostgresConfig(t.Context(), config.Copy())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	ctx := t.Context()
	now := time.UnixMilli(1_800_000_000_000).UTC()
	key := relayLaneEventOutboxKey("lane-a", strings.Repeat("x", mysqlEventKeyMax))
	item, err := s.put(key, "lane-a", "lane.event", "inbox", quietEvent("digest", "body"), now, 0, 0)
	if err != nil || !item.Fresh {
		t.Fatalf("insert: fresh=%v err=%v", item.Fresh, err)
	}
	digest := hex.EncodeToString(dialectMySQL.storedKey(key).([]byte))
	if _, err := s.db.ExecContext(ctx, `UPDATE lane_inbox SET event_key=$1,delivered_at=$2 WHERE id=$3`, digest, now.UnixMilli(), item.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.createObligation(ctx, newDeliveryObligation{key: key, relayID: 1, kind: "lane.event", lane: "lane-a", machine: "host-a", pane: "w1:p1", attempts: 1}, now, now); err != nil {
		t.Fatal(err)
	}
	var id int64
	var stored string
	if err := s.db.QueryRowContext(ctx, `SELECT id,event_key FROM delivery_obligations WHERE relay_id=$1`, 1).Scan(&id, &stored); err != nil {
		t.Fatal(err)
	}
	// The obligation keeps the raw key, so the test exercises the mismatch.
	if stored != hex.EncodeToString([]byte(key)) {
		t.Fatalf("obligation stores %q, want the raw key form", stored)
	}
	settled, err := s.obligationInboxSettled(ctx, id, now.Add(time.Second))
	if err != nil || !settled {
		t.Fatalf("settled = %v, err %v; want the digest item to settle the obligation", settled, err)
	}
}
