package panewire

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Selection drops (decision 427) count per UTC day, reason and account into the
// daily loops row's selection_drops group. A date's counts leave with its row,
// and a date with drops and no ticks is still written at midnight.
func TestSelectionDropsGroupCountsByDayReasonAndAccount(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	h := loopLedgerTestHub(t, &now, nil)
	const aaa, bbb = "devin:aaaaaaaaaaaaaaaa", "devin:bbbbbbbbbbbbbbbb"

	h.noteSelectionDrops([]fleet.SelectionDrop{
		{Reason: "pool_at_cap", Account: aaa},
		{Reason: "pool_at_cap", Account: aaa},
		{Reason: "free_limit", Account: bbb},
	})
	now = time.Date(2026, 11, 18, 23, 59, 0, 0, time.UTC)
	h.noteSelectionDrops([]fleet.SelectionDrop{{Reason: "pool_at_cap", Account: aaa}})
	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.noteSelectionDrops([]fleet.SelectionDrop{{Reason: "pool_cooling", Account: aaa}})

	gotDay := h.loopsRowDocument("2026-11-18", now).SelectionDrops
	wantDay := []selectionDropRow{
		{Machine: "hub", Reason: "free_limit", Account: bbb, Dropped: 1},
		{Machine: "hub", Reason: "pool_at_cap", Account: aaa, Dropped: 3},
	}
	if !slices.Equal(gotDay, wantDay) {
		t.Fatalf("2026-11-18 selection_drops = %+v, want %+v", gotDay, wantDay)
	}
	gotNext := h.loopsRowDocument("2026-11-19", now).SelectionDrops
	wantNext := []selectionDropRow{{Machine: "hub", Reason: "pool_cooling", Account: aaa, Dropped: 1}}
	if !slices.Equal(gotNext, wantNext) {
		t.Fatalf("2026-11-19 selection_drops = %+v, want %+v", gotNext, wantNext)
	}

	body, err := json.Marshal(h.loopsRowDocument("2026-11-18", now))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"selection_drops":[{"machine":"hub","reason":"free_limit","account":"`+bbb+`","dropped":1}`) {
		t.Fatalf("row body lacks the selection_drops group: %s", body)
	}
}

// A date that has only selection drops, no loop ticks, is written at the
// midnight write with its group, and the row is pruned after the write.
func TestSelectionDropsDayWrittenAtMidnightWithItsGroup(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	published := map[string]string{}
	h := loopLedgerTestHub(t, &now, nil)
	h.loopsDocPublish = func(_ context.Context, key, body string) error {
		published[key] = body
		return nil
	}
	h.loopsDocExists = func(_ context.Context, key string) (bool, error) { _, ok := published[key]; return ok, nil }
	h.noteSelectionDrops([]fleet.SelectionDrop{{Reason: "pool_at_cap", Account: "devin:aaaaaaaaaaaaaaaa"}})

	now = time.Date(2026, 11, 19, 0, 1, 0, 0, time.UTC)
	h.maybeWriteLoopsRow(context.Background())
	body, ok := published[loopsRowKey("2026-11-18")]
	if !ok {
		t.Fatal("2026-11-18 row not written at midnight")
	}
	if !strings.Contains(body, `"selection_drops":[{"machine":"hub","reason":"pool_at_cap","account":"devin:aaaaaaaaaaaaaaaa","dropped":1}]`) {
		t.Fatalf("written row lacks its selection_drops group: %s", body)
	}
	if _, kept := h.loopLedger().drops["2026-11-18"]; kept {
		t.Fatal("2026-11-18 drop counts kept after its row was written")
	}
}
