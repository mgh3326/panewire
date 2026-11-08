package panewire

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A due wait whose pane is blocked when the resume runs is handed over as
// blocked_dialog without an attempt: herdr refuses agent.prompt on a blocked
// pane, so a resume would only fail and spend the job's one wait.
func TestQuotaStopResumeSkipsBlockedPane(t *testing.T) {
	for _, tc := range []struct {
		status            string
		wantSends         int
		wantDecision      string
		wantReason        string
		wantAttempted     int
		wantActionWritten int
	}{
		{"blocked", 0, "handover", "blocked_dialog", 0, 0},
		{"idle", 1, "wait", "reset_within_expected", 1, 1},
	} {
		t.Run(tc.status, func(t *testing.T) {
			store := NewMemoryStore(t)
			t.Cleanup(func() { _ = store.Close() })
			root := t.TempDir()
			events := filepath.Join(root, "jobs", "job-active", "events")
			if err := os.MkdirAll(events, 0700); err != nil {
				t.Fatal(err)
			}
			stamp := time.Now().UTC().Format(time.RFC3339)
			for name, body := range map[string]string{
				"00001-job.claimed.json": `{"type":"job.claimed","created_at":"` + stamp + `","agent_label":"worker-synthetic","owner_lane":"owner","epoch":1}`,
				"00002-job.spawned.json": `{"type":"job.spawned","created_at":"` + stamp + `","pane_id":"workspace:pane-1"}`} {
				if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			q, err := newQuotaStopManager(store, root, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Second)
			reset := now.Add(time.Minute)
			screen := "You've hit your usage limit. Please try again at " + reset.Format("15:04") + " UTC"
			stop, ok := classifyQuotaStop(screen, now, time.Local)
			if !ok {
				t.Fatal("fixture screen must classify as a quota stop")
			}
			reads := 0
			q.read = func(context.Context, string) (string, error) { reads++; return screen, nil }
			q.hk = quotaStopHKFixture(`{}`, `{}`, nil)
			sends := 0
			q.send = func(_ context.Context, _, _, text string) relayInjectResult {
				sends++
				return relayInjectResult{Outcome: relayInjectDelivered, Evidence: "visible:nonce_echo", Proven: relayNoncesIn(text)}
			}
			const wake = "idle-wake:test:1"
			for _, stmt := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO idle_wake_panes(pane_id,agent_status,state_change_seq,changed_at) VALUES(?,?,?,?)`, []any{"workspace:pane-1", tc.status, 7, now.UnixMilli()}},
				{`INSERT INTO idle_wake_candidates(event_id,pane_id,workspace_id,label,agent_status,state_change_seq,changed_at,due_at) VALUES(?,?,?,?,?,?,?,?)`, []any{wake, "workspace:pane-1", "workspace", "worker-synthetic", tc.status, 7, now.UnixMilli(), now.UnixMilli()}},
				{`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,resume_text,due,hk_recorded,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, []any{wake, "job-active", "workspace:pane-1", "worker-synthetic", stop.Pool, reset.Format(time.RFC3339), "wait", "reset_within_expected", "continue", now.UnixMilli(), 1, now.UnixMilli()}},
			} {
				if _, err := store.db.Exec(stmt.sql, stmt.args...); err != nil {
					t.Fatalf("%s: %v", stmt.sql, err)
				}
			}
			q.tick(context.Background(), now.Add(time.Second), false)
			var decision, reason string
			var attempted, action int
			if err := store.db.QueryRow(`SELECT decision,reason,attempted,action_recorded FROM quota_stop_decisions WHERE event_id=?`, wake).Scan(&decision, &reason, &attempted, &action); err != nil {
				t.Fatal(err)
			}
			if tc.status == "blocked" && reads != 0 {
				t.Fatalf("a blocked pane's screen is not read before the handover: reads=%d", reads)
			}
			if sends != tc.wantSends || decision != tc.wantDecision || reason != tc.wantReason || attempted != tc.wantAttempted || action != tc.wantActionWritten {
				t.Fatalf("%s pane: sends=%d decision=%s reason=%s attempted=%d action=%d", tc.status, sends, decision, reason, attempted, action)
			}
		})
	}
}
