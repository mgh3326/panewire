package devin

import (
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// hk 4330: devin 7-day reset moved 07:42:55Z→07:56:01Z between readings 13 min apart.
// The /usage panel prints an absolute date ("Weekly ... resets Nov 15, 8:00 AM (UTC)"),
// but ParseScreen ignored the date and borrowed the plan line's relative "resets in 3d 5h",
// turning it into now+N so the reset moved with every reading.
func TestDevinWeeklyResetDoesNotMoveWithReading_t4330(t *testing.T) {
	t0 := time.Date(2026, 11, 12, 7, 42, 55, 0, time.UTC)
	t1 := time.Date(2026, 11, 12, 7, 56, 1, 0, time.UTC)

	live := fixture(t, "screen_usage_live.txt")

	o0, err := ParseScreen(live, t0)
	if err != nil {
		t.Fatal(err)
	}
	o1, err := ParseScreen(live, t1)
	if err != nil {
		t.Fatal(err)
	}

	b0 := bucketOf(t, o0, "7d")
	b1 := bucketOf(t, o1, "7d")

	const want = "2026-11-15T08:00:00Z"
	if b0.ResetsAt != want {
		t.Fatalf("at t0: 7d resets_at = %q, want %q", b0.ResetsAt, want)
	}
	if b1.ResetsAt != want {
		t.Fatalf("at t1: 7d resets_at = %q, want %q", b1.ResetsAt, want)
	}
}

func bucketOf(t *testing.T, o quota.Observation, window string) quota.Bucket {
	t.Helper()
	for _, b := range o.Buckets {
		if b.Window == window {
			return b
		}
	}
	t.Fatalf("no %s bucket in %+v", window, o.Buckets)
	return quota.Bucket{}
}
