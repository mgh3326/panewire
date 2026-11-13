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

// TestQuotaStatusTableCreditRow: a credit pool's row carries the credit
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
	used := 30.0
	var out bytes.Buffer
	writeQuotaStatusTable(&out, now, host, map[string]fleet.CreditWindow{
		pool: {UsedPct: &used, ExpiresAt: now.Add(2*24*time.Hour + 3*time.Hour)},
	}, map[string]string{pool: "spend"}, policy)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want header and one credit row, got %q", out.String())
	}
	if got := strings.Join(strings.Fields(lines[1])[2:], " "); got != "abcd credit 30.0% 2d 3h fresh credit" {
		t.Fatalf("credit row = %q", got)
	}
}

// TestQuotaStatusBandFollowsAdmissionClass: a native pool's band is the tier
// placement gives the operator's admission class for it. A preserved pool is
// not ranked, since tierOf gives a non-spend class no tier; an excluded pool is
// excluded; a spent pool ranks by its windows.
func TestQuotaStatusBandFollowsAdmissionClass(t *testing.T) {
	policy, err := fleet.LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	used := 42.0
	windows := []fleet.StatusWindow{{Window: "5h", UsedPct: &used, ResetsAt: now.Add(3 * time.Hour).Format(time.RFC3339Nano), Scope: "account"}}
	cases := map[string]string{"spend": "ranked", "preserve": "unknown", "exclude": "excluded"}
	for class, want := range cases {
		if got := fleet.StatusBand(policy, "claude", class, false, windows, now); got != want {
			t.Errorf("band for class %q = %q, want %q", class, got, want)
		}
	}
}
