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
			if len(rig.requests) != 1 {
				t.Fatalf("requests=%d", len(rig.requests))
			}
			rig.route(0, "owner")
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
				if len(rig.requests) != 2 {
					t.Fatalf("requests=%d", len(rig.requests))
				}
				rig.route(1, "owner")
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
	for _, s := range []string{"", "operator-desk", "abc-integrator", "director-1", "fleet-tooling-one", "consult-one"} {
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
