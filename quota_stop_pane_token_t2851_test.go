package panewire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// paneQuotaReport is one call the manager made to the pane token reporter.
type paneQuotaReport struct {
	pane, value string
	ttl         time.Duration
}

// recordPaneQuotaReports substitutes the reporter and collects its calls.
func recordPaneQuotaReports(q *quotaStopManager) *[]paneQuotaReport {
	var reports []paneQuotaReport
	q.show = func(_ context.Context, pane, value string, ttl time.Duration) error {
		reports = append(reports, paneQuotaReport{pane: pane, value: value, ttl: ttl})
		return nil
	}
	return &reports
}

func newPaneTokenManager(t *testing.T, act bool) *quotaStopManager {
	t.Helper()
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	q, err := newQuotaStopManager(store, t.TempDir(), act, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// A worker wait publishes panewire_quota on the pane with the pool and the
// reset, and the resume at that reset clears it (hk 2851).
func TestQuotaStopPaneTokenShowsWaitAndClearsOnResume(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	reports := recordPaneQuotaReports(rig.q)
	rig.run()
	var reset string
	if err := rig.store.db.QueryRow(`SELECT reset FROM quota_stop_decisions WHERE decision='wait'`).Scan(&reset); err != nil {
		t.Fatal(err)
	}
	if len(*reports) != 2 {
		t.Fatalf("reports=%+v, want one publish and one clear", *reports)
	}
	publish := (*reports)[0]
	if publish.pane != "workspace:pane-1" || publish.value != "devin-free wait until "+reset {
		t.Fatalf("publish=%+v, want the pane's pool and reset", publish)
	}
	// devin.txt resets 29 s after the observation; the token outlives the
	// reset by the refresh margin.
	if publish.ttl < 5*time.Minute || publish.ttl >= 6*time.Minute {
		t.Fatalf("ttl=%s, want the reset plus the refresh margin", publish.ttl)
	}
	if clear := (*reports)[1]; clear.pane != "workspace:pane-1" || clear.value != "" || clear.ttl != 0 {
		t.Fatalf("clear=%+v, want an empty value on the same pane", clear)
	}
	if rig.sends != 1 {
		t.Fatalf("sends=%d, want the one resume the test is about", rig.sends)
	}
}

// A failed resume hands the job over, so the pane stops advertising the wait.
func TestQuotaStopPaneTokenClearsOnHandover(t *testing.T) {
	rig := newRoleStopRig(t, "worker", time.Hour, roleStopTask)
	reports := recordPaneQuotaReports(rig.q)
	rig.q.send = func(context.Context, string, string, string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectMaybeInPane}
	}
	rig.run()
	if len(*reports) != 2 || (*reports)[1].value != "" {
		t.Fatalf("reports=%+v, want a publish then a clear", *reports)
	}
	if decision, reason := rig.decision(t); decision != "handover" || reason != "submission_unproven" {
		t.Fatalf("decision=%s reason=%s, want a hand-over", decision, reason)
	}
}

// A reset beyond herdr's 24 h token maximum is re-reported shortly before the
// token lapses; a reset inside it is reported once and left alone.
func TestQuotaStopPaneTokenRefreshesCappedWait(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		act    bool
		role   string
		reset  time.Duration
		waits  int
		report bool
	}{
		{"worker wait inside the maximum TTL", false, "worker", 5 * time.Minute, 1, true},
		{"reviewer wait in shadow mode", false, "reviewer", 5 * time.Minute, 0, false},
		{"act flag waits for every role", true, "reviewer", 5 * time.Minute, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newPaneTokenManager(t, tc.act)
			reports := recordPaneQuotaReports(q)
			reset := now.Add(tc.reset).Format(time.RFC3339)
			if _, err := q.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,role) VALUES('evt','job-active','workspace:pane-1','worker-synthetic','claude',?,'wait',?)`, reset, tc.role); err != nil {
				t.Fatal(err)
			}
			q.refreshQuotaWaits(context.Background(), now)
			if len(*reports) != tc.waits {
				t.Fatalf("reports=%+v, want %d", *reports, tc.waits)
			}
		})
	}
}

// A weekly reset (further out than one report can carry) is re-reported after
// the capped TTL, not on every tick.
func TestQuotaStopPaneTokenRefreshSchedule(t *testing.T) {
	q := newPaneTokenManager(t, true)
	reports := recordPaneQuotaReports(q)
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	reset := now.Add(48 * time.Hour).Format(time.RFC3339)
	if _, err := q.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,role) VALUES('evt','job-active','workspace:pane-1','worker-synthetic','claude',?,'wait','worker')`, reset); err != nil {
		t.Fatal(err)
	}
	q.refreshQuotaWaits(context.Background(), now)
	if len(*reports) != 1 {
		t.Fatalf("reports=%+v, want the first publish", *reports)
	}
	if first := (*reports)[0]; first.ttl != paneQuotaTokenTTL || first.value != "claude wait until "+reset {
		t.Fatalf("first report=%+v, want a capped TTL", first)
	}
	q.refreshQuotaWaits(context.Background(), now.Add(time.Hour))
	if len(*reports) != 1 {
		t.Fatalf("reports=%+v, want no refresh an hour in", *reports)
	}
	due := now.Add(paneQuotaTokenTTL - paneQuotaTokenRefreshBefore)
	q.refreshQuotaWaits(context.Background(), due.Add(-time.Second))
	if len(*reports) != 1 {
		t.Fatalf("reports=%+v, want no refresh just before the margin", *reports)
	}
	q.refreshQuotaWaits(context.Background(), due)
	if len(*reports) != 2 {
		t.Fatalf("reports=%+v, want a refresh at the margin", *reports)
	}
}

// The default reporter's argv is herdr's documented report-metadata shape:
// source panewire.quota, the panewire_quota token and a TTL, or --clear-token.
func TestQuotaStopPaneTokenReportCommand(t *testing.T) {
	dir := t.TempDir()
	argvLog := filepath.Join(dir, "argv")
	binary := filepath.Join(dir, "herdr")
	script := "#!/bin/sh\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\"; done > \"$PANEWIRE_TEST_ARGV\"\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HERDR_BIN", binary)
	t.Setenv("PANEWIRE_TEST_ARGV", argvLog)
	q := newPaneTokenManager(t, true)
	q.session = func(string, string) string { return "" }
	reads := func() []string {
		t.Helper()
		b, err := os.ReadFile(argvLog)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Fields(string(b))
	}
	if err := q.reportQuotaWait(context.Background(), "workspace:pane-1", "claude wait until 2026-11-14T09:00:00Z", 90*time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(reads(), " "), "pane report-metadata workspace:pane-1 --source panewire.quota --token panewire_quota=claude wait until 2026-11-14T09:00:00Z --ttl-ms 5400000"; got != want {
		t.Fatalf("set argv=%q, want %q", got, want)
	}
	if err := q.reportQuotaWait(context.Background(), "workspace:pane-1", "", 0); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(reads(), " "), "pane report-metadata workspace:pane-1 --source panewire.quota --clear-token panewire_quota"; got != want {
		t.Fatalf("clear argv=%q, want %q", got, want)
	}
}
