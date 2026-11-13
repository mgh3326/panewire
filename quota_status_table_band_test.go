package panewire

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// TestQuotaResetsInForPeople pins the relative "resets in" boundaries: a
// window already reset reads "passed", under a minute "<1m", and otherwise the
// two most significant units.
func TestQuotaResetsInForPeople(t *testing.T) {
	cases := []struct {
		left time.Duration
		want string
	}{
		{-time.Minute, "passed"},
		{0, "passed"},
		{30 * time.Second, "<1m"},
		{45 * time.Minute, "45m"},
		{2*time.Hour + 5*time.Minute, "2h 5m"},
		{6*24*time.Hour + 2*time.Hour + 30*time.Minute, "6d 2h"},
	}
	for _, c := range cases {
		if got := quotaResetsIn(c.left); got != c.want {
			t.Errorf("quotaResetsIn(%v) = %q, want %q", c.left, got, c.want)
		}
	}
}

// TestQuotaStatusTableCreditRow: a credit pool's row carries the manual
// entry's used share and expiry in the window's columns, and its band is the
// credit tier selection gives a credit pool.
func TestQuotaStatusTableCreditRow(t *testing.T) {
	policy, err := fleet.LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	if len(policy.CreditPools) == 0 {
		t.Fatal("default pool policy names no credit pool")
	}
	pool := policy.CreditPools[0]
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	host := quotaStatusHost{Host: "h", Readings: []quotaReading{{
		Pool: pool, Account: "abcd", Source: "manual", Credit: "live", Presence: "present", Buckets: []quotaBucket{},
	}}}
	var out bytes.Buffer
	writeQuotaStatusTable(&out, now, host, map[string]fleet.CreditWindow{
		pool: {UsedPct: 30, ExpiresAt: now.Add(2*24*time.Hour + 3*time.Hour)},
	}, policy)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want header and one credit row, got %q", out.String())
	}
	if got := strings.Join(strings.Fields(lines[1])[2:], " "); got != "abcd credit 30.0% 2d 3h fresh credit" {
		t.Fatalf("credit row = %q", got)
	}
}
