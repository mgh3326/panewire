package panewire

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuotaStopWakeShadowAndResume(t *testing.T) {
	for _, act := range []bool{false, true} {
		t.Run(map[bool]string{false: "shadow", true: "act"}[act], func(t *testing.T) {
			rig := newIdleWakeTestRig(t, time.Second)
			root := rig.manager.inboxRoot
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
			q, err := newQuotaStopManager(rig.store, root, act, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			rig.manager.quota = q
			reads, sends, records, handovers := 0, 0, 0, 0
			q.enqueue = func(e hubScannedRelayEvent) bool {
				if e.OwnerLane == "operator-desk" {
					handovers++
				}
				return true
			}
			screen, err := os.ReadFile("testdata/quota-stop/devin.txt")
			if err != nil {
				t.Fatal(err)
			}
			q.read = func(context.Context, string) (string, error) { reads++; return string(screen), nil }
			policy := `## Values
- default_expected: 1d
- urgent_priorities: [1, 2]
- max_waits: 1
- resume_text: "continue"
`
			q.command = func(_ context.Context, args ...string) ([]byte, error) {
				switch strings.Join(args[:2], " ") {
				case "doc get":
					return json.Marshal(map[string]string{"body": policy})
				case "tasks list":
					return []byte(`{"tasks":[{"priority":3,"expected":"4h","refs":{"job_id":"job-active"}}]}`), nil
				case "doc put":
					records++
					return []byte(`{}`), nil
				}
				t.Fatalf("unexpected command %v", args)
				return nil, nil
			}
			q.send = func(_ context.Context, pane, pool, text string) relayInjectResult {
				sends++
				if pane != "workspace:pane-1" || !strings.HasPrefix(text, "continue [r0") {
					t.Fatalf("send %s %s", pane, text)
				}
				return relayInjectResult{Outcome: relayInjectDelivered, Evidence: "visible:nonce_echo", Proven: relayNoncesIn(text)}
			}
			rig.observe("working", 1, rig.now)
			rig.observe("blocked", 2, rig.now.Add(time.Second))
			rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
			if len(rig.requests) != 0 || len(rig.emitted) != 0 {
				t.Fatalf("blocked affected idle wake requests=%d emitted=%d", len(rig.requests), len(rig.emitted))
			}
			rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
			rig.manager.Tick(context.Background(), rig.now.Add(40*time.Second))
			rig.manager.Tick(context.Background(), rig.now.Add(41*time.Second))
			wantSends := 0
			wantReads := 1
			if act {
				wantSends = 1
				wantReads = 2
			}
			if sends != wantSends || reads != wantReads || records != 1 {
				t.Fatalf("reads=%d sends=%d records=%d", reads, sends, records)
			}
			if act {
				rig.now = rig.now.Add(50 * time.Second)
				rig.observe("working", 3, rig.now)
				rig.observe("idle", 4, rig.now.Add(time.Second))
				rig.manager.Tick(context.Background(), rig.now.Add(3*time.Second))
				if len(rig.requests) != 1 {
					t.Fatalf("requests=%d", len(rig.requests))
				}
				rig.route(0, "owner")
				rig.manager.Tick(context.Background(), rig.now.Add(4*time.Second))
				rig.manager.Tick(context.Background(), rig.now.Add(5*time.Second))
				if sends != 1 || handovers != 1 || records != 2 {
					t.Fatalf("second stop sends=%d handovers=%d records=%d", sends, handovers, records)
				}
			}
		})
	}
}

func TestQuotaStopPolicyCacheAndExclusions(t *testing.T) {
	for _, s := range []string{"", "operator-desk", "abc-integrator", "director", "director-1", "fleet-tooling-one", "consult", "consult-one"} {
		if !quotaExcluded(s) {
			t.Errorf("not excluded %q", s)
		}
	}
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	q, err := newQuotaStopManager(store, t.TempDir(), false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	q.command = func(context.Context, ...string) ([]byte, error) { return nil, os.ErrNotExist }
	if _, ok := q.loadPolicy(context.Background(), time.Now()); ok {
		t.Fatal("missing policy accepted")
	}
}

func TestQuotaStopGuardRejectsDialogBeforePrompt(t *testing.T) {
	for _, harness := range []string{"claude", "devin"} {
		t.Run(harness, func(t *testing.T) {
			_, calls := unprovenSetupHerdr(t, []string{"You've hit your weekly limit · resets Nov 1, 2am\nChoose an option: upgrade or wait"})
			result := relayInjectForHarnessGuarded(context.Background(), "fake-pane", harness, "continue [r0000000]", nil, func(string) bool { return false })
			if result.Outcome != relayInjectMaybeInPane || !strings.Contains(result.Evidence, "guard_rejected") {
				t.Fatalf("result=%+v", result)
			}
			for _, call := range calls() {
				if call == "prompt" || call == "send-keys" {
					t.Fatalf("unexpected fake herdr call %q", call)
				}
			}
		})
	}
}

func TestQuotaStopBlockedKeepsIdleCancellation(t *testing.T) {
	rig := newIdleWakeTestRig(t, 5*time.Second)
	q, err := newQuotaStopManager(rig.store, rig.manager.inboxRoot, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	rig.manager.quota = q
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.observe("blocked", 3, rig.now.Add(2*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(10*time.Second))
	if len(rig.requests) != 0 {
		t.Fatalf("idle flap routed: %+v", rig.requests)
	}
}

func TestQuotaStopOutboxBackoffIsPerRow(t *testing.T) {
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	root := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(root, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	q, err := newQuotaStopManager(store, root, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	q.command = func(context.Context, ...string) ([]byte, error) { calls++; return nil, os.ErrNotExist }
	now := time.Now().UTC()
	for _, id := range []string{"a", "b"} {
		if _, err := store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,created) VALUES(?,?,?,?,?,?,?,?,?)`, id, "job-"+id, "pane-"+id, "worker", "codex", "unknown", "handover", "urgent", now.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	q.tick(context.Background(), now, false)
	q.tick(context.Background(), now.Add(time.Second), false)
	q.tick(context.Background(), now.Add(2*time.Second), false)
	if calls != 2 {
		t.Fatalf("hk calls=%d, want one per row", calls)
	}
	var backed int
	if err := store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE hk_retry>0 AND action_retry>0`).Scan(&backed); err != nil || backed != 2 {
		t.Fatalf("backed=%d err=%v", backed, err)
	}
}

func TestQuotaStopDeduplicatesPaneAndReset(t *testing.T) {
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
	q, err := newQuotaStopManager(store, root, false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(time.Hour).Format("15:04")
	q.read = func(context.Context, string) (string, error) {
		return "You've hit your usage limit. Please try again at " + reset + " UTC", nil
	}
	q.command = func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "doc" {
			return json.Marshal(map[string]string{"body": "## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue\"\n"})
		}
		return []byte(`{"tasks":[{"priority":3,"refs":{"job_id":"job-active"}}]}`), nil
	}
	for _, id := range []string{"wake-one", "wake-two"} {
		q.observeBounded(context.Background(), idleWakeCandidate{EventID: id, PaneID: "workspace:pane-1", Label: "worker-synthetic"}, now)
	}
	var n int
	if err := store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE decision<>''`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("decisions=%d err=%v", n, err)
	}
}

func TestQuotaStopReadTimeoutRecordsHandover(t *testing.T) {
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
	q.read = func(ctx context.Context, _ string) (string, error) { <-ctx.Done(); return "", ctx.Err() }
	q.observeBounded(context.Background(), idleWakeCandidate{EventID: "wake-timeout", PaneID: "workspace:pane-1", Label: "worker-synthetic"}, time.Now())
	var decision, reason, reset string
	if err := store.db.QueryRow(`SELECT decision,reason,reset FROM quota_stop_decisions WHERE event_id='wake-timeout'`).Scan(&decision, &reason, &reset); err != nil || decision != "handover" || reason != "screen_read_timeout" || reset != "unknown" {
		t.Fatalf("decision=%s reason=%s reset=%s err=%v", decision, reason, reset, err)
	}
}

func TestQuotaStopUnknownResetDedupExpires(t *testing.T) {
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	q, err := newQuotaStopManager(store, t.TempDir(), false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		id    string
		at    time.Time
		reset string
	}{
		{"a", start, "unknown"}, {"b", start.Add(20 * time.Minute), "unknown"}, {"c", start.Add(time.Hour), "unknown"},
		{"d", start, "2026-10-29T03:00:00Z"}, {"e", start.Add(time.Hour), "2026-10-29T03:00:00Z"},
	} {
		if _, err := store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,created) VALUES(?,?,?,?,?)`, tc.id, "job", "pane", "worker", tc.at.UnixMilli()); err != nil {
			t.Fatal(err)
		}
		q.recordDecision(context.Background(), idleWakeCandidate{EventID: tc.id, PaneID: "pane"}, tc.at, "claude", tc.reset, "handover", "unknown", "", 0)
	}
	var n int
	if err := store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE decision<>'' AND reset='unknown'`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("unknown count=%d err=%v", n, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM quota_stop_decisions WHERE decision<>'' AND reset<>'unknown'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("known count=%d err=%v", n, err)
	}
}

func TestQuotaStopAndWakeTargetFlagsRemainHubFlags(t *testing.T) {
	for _, flag := range []string{"--quota-stop-act", "--wake-targets"} {
		if !hubFlagsProvided([]string{flag}) {
			t.Errorf("%s was lost in merge", flag)
		}
	}
}
