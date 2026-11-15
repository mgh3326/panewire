package panewire

import (
	"context"
	"testing"
	"time"
)

func TestStallSafetyEpisode(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	state, err := newStallSafetyState(store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		at            time.Duration
		working, busy bool
		want          bool
	}{
		{0, false, false, false},
		{29 * time.Minute, false, false, false},
		{31 * time.Minute, false, true, false},
		{60 * time.Minute, false, false, false},
		{62 * time.Minute, false, false, true},
	} {
		due, err := state.observe(ctx, "job", "pane", tc.working, tc.busy, start.Add(tc.at), 30*time.Minute)
		if err != nil || due != tc.want {
			t.Fatalf("at %s: due=%v err=%v; want %v", tc.at, due, err, tc.want)
		}
	}
	if saved, err := state.recordExamination(ctx, "job", "pane", "last lines token=secretvalue123", "idle", ""); err != nil || !saved {
		t.Fatalf("record: saved=%v err=%v", saved, err)
	}
	if saved, err := state.recordExamination(ctx, "job", "pane", "different", "different", ""); err != nil || saved {
		t.Fatalf("duplicate: saved=%v err=%v", saved, err)
	}
	state, err = newStallSafetyState(store)
	if err != nil {
		t.Fatal(err)
	}
	if due, err := state.observe(ctx, "job", "pane", false, false, start.Add(2*time.Hour), 30*time.Minute); err != nil || due {
		t.Fatalf("repeat: due=%v err=%v", due, err)
	}
	if _, err := state.observe(ctx, "job", "pane", true, false, start.Add(3*time.Hour), 30*time.Minute); err != nil {
		t.Fatal(err)
	}
	var screen, explanation string
	if err := store.db.QueryRowContext(ctx, `SELECT screen,explanation FROM stall_safety_examinations WHERE job=? AND pane=?`, "job", "pane").Scan(&screen, &explanation); err != nil || screen != "last lines [redacted]" || explanation != "idle" {
		t.Fatalf("evidence: screen=%q explanation=%q err=%v", screen, explanation, err)
	}
	if due, err := state.observe(ctx, "job", "pane", false, false, start.Add(4*time.Hour), 30*time.Minute); err != nil || due {
		t.Fatalf("new episode: due=%v err=%v", due, err)
	}
}
