package panewire

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func TestCheckoutRefusalMovesToNextHost(t *testing.T) {
	for _, test := range []struct {
		name, code string
		retryable  bool
	}{
		{"dirty", "dirty", true},
		{"sha-missing", "sha-missing", true},
		{"branch-elsewhere", "branch-elsewhere", true},
		{"dirty without verified cleanup", "dirty", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewTextHandler(&logs, nil))
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			other := t14NamedNode(t, h, server, "machine-b", true, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", h.now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			type answer struct {
				reply fleet.HubSpawnReply
				err   error
			}
			first := make(chan answer, 1)
			go func() {
				reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 940, "machine-a"))
				first <- answer{reply, err}
			}()
			sent := t14ReadNodeSpawn(t, node)
			if err = wsjson.Write(context.Background(), node, map[string]any{
				"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 81, "job_id": "", "pane": "", "stdout_tail": "",
				"error": "checkout_refused:" + test.code, "stage": "checkout", "retryable": test.retryable,
			}); err != nil {
				t.Fatal(err)
			}
			got := <-first
			if got.err != nil || got.reply.NextHost != test.retryable || got.reply.Error != "checkout_refused:"+test.code {
				t.Fatalf("placement was not handed the refusal: %+v", got)
			}
			saved, err := h.inbox.store.spawnTask(context.Background(), request.RequestID)
			if !test.retryable {
				if err != nil || saved.Status != "failed" || saved.Outcome.RC != 81 || !strings.HasPrefix(saved.Outcome.Reason, "checkout_refused:dirty") {
					t.Fatalf("unverified cleanup did not stay terminal: %+v %v", saved, err)
				}
				h.mu.Lock()
				entry := h.launchBackoffs[launchKey{"machine-a", "claude"}]
				h.mu.Unlock()
				if entry != nil {
					t.Fatalf("terminal checkout refusal counted toward launch backoff: %+v", entry)
				}
				return
			}
			if err != nil || saved.Status != "accepted" || saved.Node != nil {
				t.Fatalf("refusal ended the operation: %+v %v", saved, err)
			}
			h.mu.Lock()
			failures := 0
			if entry := h.launchBackoffs[launchKey{"machine-a", "claude"}]; entry != nil {
				failures = entry.failures
			}
			h.mu.Unlock()
			if failures != 0 {
				t.Fatalf("checkout refusal counted toward launch backoff: %d", failures)
			}
			if line := logs.String(); !strings.Contains(line, "host=machine-a") || !strings.Contains(line, "profile=sonnet") || !strings.Contains(line, "checkout_refused:"+test.code) {
				t.Fatalf("handoff log lacks host, profile, or reason: %q", line)
			}
			if !spawnRefusalTriesNextHost("checkout_refused:" + test.code) {
				t.Fatalf("replay would pin to the refused host for %q", test.code)
			}

			second := make(chan answer, 1)
			go func() {
				reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 941, "machine-b"))
				second <- answer{reply, err}
			}()
			sent = t14ReadNodeSpawn(t, other)
			if sent.RequestID != t14RequestID(941) {
				t.Fatalf("second candidate did not bind under the same operation: %+v", sent)
			}
			if err = wsjson.Write(context.Background(), other, nodeRefusal(sent.RequestID, "checkout_refused:dirty", false)); err != nil {
				t.Fatal(err)
			}
			<-second
			saved, err = h.inbox.store.spawnTask(context.Background(), request.RequestID)
			if err != nil || saved.Node == nil || saved.Node.RequestID != t14RequestID(941) {
				t.Fatalf("second candidate was not bound to operation: %+v %v", saved, err)
			}
		})
	}
}

func TestCheckoutRefusalRetryabilityAndScopeGuards(t *testing.T) {
	for reason, want := range map[string]bool{
		"checkout_refused:dirty":            true,
		"checkout_refused:sha-missing":      true,
		"checkout_refused:branch-elsewhere": true,
		"checkout_refused:origin-mismatch":  false,
		"checkout_refused:locked":           false,
		"checkout_refused:git-failed":       false,
		"checkout_refused:git_too_old":      false,
		"checkout_refused:work_root_unset":  false,
		"checkout_refused:cancelled":        false,
		"checkout_refused:busy":             false,
	} {
		if got := spawnRefusalTriesNextHost(reason); got != want {
			t.Errorf("spawnRefusalTriesNextHost(%q)=%v, want %v", reason, got, want)
		}
	}
}

func TestCheckoutRefusalDetailDoesNotCountTowardLaunchBackoff(t *testing.T) {
	kind := fleet.ProfileKind("sonnet")
	if kind == "" {
		t.Fatal("sonnet profile has no harness kind for backoff accounting")
	}
	h := asyncSpawnHub(t)
	for _, code := range []string{"dirty", "sha-missing", "branch-elsewhere"} {
		reason := "checkout_refused:" + code + ": local checkout state refused"
		h.observeLaunchOutcome("machine-a", "sonnet", 81, reason, time.Now().UTC())
		h.mu.Lock()
		entry := h.launchBackoffs[launchKey{"machine-a", kind}]
		h.mu.Unlock()
		if entry != nil {
			t.Errorf("%s with detail counted toward launch backoff: %+v", code, entry)
		}
	}
}

func TestPinnedCheckoutRefusalStaysTerminal(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	request.Args = append([]string(nil), request.Args...)
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "--host" {
			request.Args[i+1] = "machine-a"
		}
	}
	request.Args = append(request.Args, "--containment", "hk:12")
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", h.now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	done := make(chan fleet.HubSpawnReply, 1)
	go func() {
		reply, _ := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 942, "machine-a"))
		done <- reply
	}()
	sent := t14ReadNodeSpawn(t, node)
	if err = wsjson.Write(context.Background(), node, map[string]any{
		"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 81, "job_id": "", "pane": "", "stdout_tail": "",
		"error": "checkout_refused:dirty", "stage": "checkout", "retryable": true,
	}); err != nil {
		t.Fatal(err)
	}
	if reply := <-done; reply.NextHost {
		t.Fatalf("pinned checkout refusal moved to another host: %+v", reply)
	}
	saved, err := h.inbox.store.spawnTask(context.Background(), request.RequestID)
	if err != nil || saved.Status != "retryable" || !strings.HasPrefix(saved.Outcome.Reason, "checkout_refused:dirty") {
		t.Fatalf("pinned refusal was not retained as the answer: %+v %v", saved, err)
	}
}

func TestCheckoutRefusalExhaustionSavedOutcome(t *testing.T) {
	h := asyncSpawnHub(t)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(context.Background(), request, "test-job", "parent", h.now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	wantReason := "admission_refused: local spawns are disabled on this host; no remote candidate completed (tried: h1 hub_checkout_refused:dirty; h2 hub_checkout_refused:dirty)"
	if changed, err := h.inbox.store.completeSpawn(context.Background(), task, hubSpawnOutcome{
		RC: 80, Stage: "placement", Reason: wantReason, Retryable: true,
	}, h.now().UTC()); err != nil || !changed {
		t.Fatalf("complete refused operation: changed=%v err=%v", changed, err)
	}
	saved, err := h.inbox.store.spawnTask(context.Background(), request.RequestID)
	if err != nil || saved.Status != "retryable" || saved.Outcome.RC != 80 || !strings.HasPrefix(saved.Outcome.Reason, "admission_refused:") || !strings.Contains(saved.Outcome.Reason, "h1 hub_checkout_refused:dirty") || !strings.Contains(saved.Outcome.Reason, "h2 hub_checkout_refused:dirty") {
		t.Fatalf("exhaustion outcome=%+v err=%v", saved, err)
	}
}
