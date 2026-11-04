package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// quotaShadowFixture is one active job bound to workspace:pane-1, a manager
// in shadow mode, and a log buffer.
func quotaShadowFixture(t *testing.T) (*quotaStopManager, *Store, *bytes.Buffer) {
	t.Helper()
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
	logs := &bytes.Buffer{}
	q, err := newQuotaStopManager(store, root, false, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	q.command = func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "doc" {
			return json.Marshal(map[string]string{"body": "## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue\"\n"})
		}
		return []byte(`{"tasks":[{"priority":3,"refs":{"job_id":"job-active"}}]}`), nil
	}
	return q, store, logs
}

func quotaShadowWake(id string) idleWakeCandidate {
	return idleWakeCandidate{EventID: id, PaneID: "workspace:pane-1", Label: "worker-synthetic"}
}

// The screen read has its own budget: it is never what is left of the job
// scan's budget, which is how every shadow decision on one host ended as
// screen_read_timeout.
func TestQuotaStopReadHasItsOwnBudget(t *testing.T) {
	q, _, _ := quotaShadowFixture(t)
	var left time.Duration
	q.read = func(ctx context.Context, _ string) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("the screen read must be bounded")
		}
		left = time.Until(deadline)
		return "idle prompt\n", nil
	}
	q.observeBounded(context.Background(), quotaShadowWake("wake-budget"), time.Now())
	if left <= quotaStopScanBudget || left > quotaStopReadBudget {
		t.Fatalf("read budget %v: want its own %v, not the remainder of the %v scan budget", left, quotaStopReadBudget, quotaStopScanBudget)
	}
}

// Every screen read is counted with its phase timings, quota or not, and the
// screen text never reaches the log.
func TestQuotaStopObservationLogsEveryClassification(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name, screen, outcome string
	}{
		{"not a quota screen", "SECRET-SCREEN-TEXT working on the brief\n❯ ", "not_quota"},
		{"quota screen", "SECRET-SCREEN-TEXT earlier output\nYou've hit your usage limit. Please try again at " + now.Add(time.Hour).Format("15:04") + " UTC", "quota"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _, logs := quotaShadowFixture(t)
			q.read = func(context.Context, string) (string, error) { return tc.screen, nil }
			q.observeBounded(context.Background(), quotaShadowWake("wake-"+tc.outcome), now)
			line := ""
			for _, l := range strings.Split(logs.String(), "\n") {
				if strings.Contains(l, `msg="quota-stop observation"`) {
					line = l
				}
			}
			if !strings.Contains(line, "outcome="+tc.outcome) || !strings.Contains(line, "scan_ms=") || !strings.Contains(line, "read_ms=") || !strings.Contains(line, "pane=workspace:pane-1") {
				t.Fatalf("observation log line: %q\nall logs:\n%s", line, logs)
			}
			if strings.Contains(logs.String(), "SECRET-SCREEN-TEXT") {
				t.Fatalf("screen text reached the log:\n%s", logs)
			}
		})
	}
}

// The handover reason names what ended the read.
func TestQuotaStopReadFailureReasons(t *testing.T) {
	for _, tc := range []struct {
		name   string
		read   func(context.Context, string) (string, error)
		cancel bool
		reason string
	}{
		{"read command fails", func(context.Context, string) (string, error) { return "", errors.New("exit status 1") }, false, "screen_read_failed"},
		{"caller canceled during the read", func(ctx context.Context, _ string) (string, error) { <-ctx.Done(); return "", ctx.Err() }, true, "observation_canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, store, logs := quotaShadowFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			q.read = func(rctx context.Context, pane string) (string, error) {
				if tc.cancel {
					cancel()
				}
				return tc.read(rctx, pane)
			}
			q.observeBounded(ctx, quotaShadowWake("wake-fail"), time.Now())
			var decision, reason string
			if err := store.db.QueryRow(`SELECT decision,reason FROM quota_stop_decisions WHERE event_id='wake-fail'`).Scan(&decision, &reason); err != nil || decision != "handover" || reason != tc.reason {
				t.Fatalf("decision=%s reason=%s err=%v", decision, reason, err)
			}
			if !strings.Contains(logs.String(), "outcome="+tc.reason) {
				t.Fatalf("the observation log names the reason:\n%s", logs)
			}
		})
	}
}
