package panewire

import (
	"context"
	"testing"
	"time"
)

func TestStallSafetyExaminesOnce(t *testing.T) {
	store := NewMemoryStore(t)
	defer store.Close()
	state, err := newStallSafetyState(store)
	if err != nil {
		t.Fatal(err)
	}
	reads, explanations := 0, 0
	probe := stallSafetyProbe{
		state:   state,
		read:    func(context.Context, string) (string, error) { reads++; return "last screen", nil },
		explain: func(context.Context, string) (string, error) { explanations++; return "idle", nil },
	}
	ctx := context.Background()
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for _, at := range []time.Time{start, start.Add(31 * time.Minute), start.Add(32 * time.Minute)} {
		if err := probe.observe(ctx, "job", "pane", false, false, at, 30*time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if reads != 1 || explanations != 1 {
		t.Fatalf("screen reads=%d explanations=%d; want one each", reads, explanations)
	}
}
