package panewire

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	hooksTail  = "stderr: HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen; brief withheld\nFix: review the hooks"
	trustTail  = "stderr: WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for /work/x; brief withheld\nFix: approve"
	ompTail    = "stderr: OMP_SETUP_WIZARD: omp is in its setup wizard despite OMP_SKIP_SETUP; seat withheld\nFix: run omp"
	loginTail  = "stderr: no usable login for the selected account\nFix: sign in"
	deskNotice = " needs a one-time action: "
)

// firstRunRefusal is a node answer that stops at a first-run dialog or a
// missing login after the engine launched a pane and cleaned it up.
func firstRunRefusal(requestID, tail string, rc int) map[string]any {
	return map[string]any{"type": "job.spawn.result", "request_id": requestID, "rc": rc, "job_id": "", "pane": "", "stdout_tail": tail, "error": "spawn_failed", "stage": "launch", "retryable": true}
}

func firstRunNodeRequest(request fleet.Submission, id int, profile string) fleet.HubSpawnRequest {
	node := refusalNodeRequest(request, id, "machine-a")
	node.Args = []string{"--job", "test-job", "-m", profile, "-l", "test-job", "--role", "implementer"}
	return node
}

// deskNotices returns the desk notices filed since the last call. The desk is
// inbox-only (hk 3602): its pane gets only the one-line wake, never a notice
// body, and the notice is read from the desk's inbox, the way the desk
// session reads it; reading marks it read.
func deskNotices(t *testing.T, h *HubServer, destination *hubAgent) []string {
	t.Helper()
	for _, relay := range spawnFailedInjections(destination) {
		if relay.EventID != 0 || !strings.HasPrefix(relay.Text, "[inbox] operator-desk: ") {
			t.Fatalf("the desk pane got a body: %+v", relay)
		}
	}
	items, err := h.inbox.store.list("operator-desk", true)
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for i := len(items) - 1; i >= 0; i-- {
		texts = append(texts, items[i].Event.Text)
		if _, err := h.inbox.store.read(items[i].ID, "operator-desk", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	return texts
}

// A first-run refusal comes after the node's hk job claim (stage launch), so hk
// holds the job id for that host and another host's claim would be a 409 (hk
// 4614). A cleaned-up harness may be replaced on that host. The host's launch
// backoff still counts the refusal and the desk hears which action it needs.
func TestFirstRunRefusalAfterClaimPinsTheHost(t *testing.T) {
	for _, test := range []struct {
		name, profile, kind, tail, action string
		rc                                int
	}{
		{"codex hooks review", "codex-sol", "codex", hooksTail, `run codex once and choose "Trust all and continue"`, 90},
		{"codex workspace trust", "codex-sol", "codex", trustTail, "trust the workspace", 90},
		{"omp setup wizard", "omp-sol", "omp", ompTail, "run omp once and finish its setup", 70},
		{"claude workspace trust", "sonnet", "claude", trustTail, "trust the workspace", 90},
		{"login missing", "sonnet", "claude", loginTail, "log in", 88},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
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
				reply, err := dispatchPlacedSpawn(boundedContext(t), transport, firstRunNodeRequest(request, 911, test.profile))
				first <- answer{reply, err}
			}()
			sent := t14ReadNodeSpawn(t, node)
			if err = wsjson.Write(t.Context(), node, firstRunRefusal(sent.RequestID, test.tail, test.rc)); err != nil {
				t.Fatal(err)
			}
			got := <-first
			if got.err != nil || got.reply.NextHost || got.reply.RC != test.rc {
				t.Fatalf("a refusal after the hk claim moved to another host: %+v", got)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "accepted" || saved.Node != nil || !saved.Outcome.Retryable {
				t.Fatalf("refusal did not pin the retry to its claimed host: status=%s host=%s err=%v", saved.Status, spawnTaskHost(saved), err)
			}
			foreign := boundaryTestNode(saved, 912, "machine-b")
			if _, err := h.inbox.store.bindSpawn(t.Context(), request.RequestID, saved.Attempt, foreign, time.Now()); err == nil {
				t.Fatal("retry was allowed to bind to a different host")
			}
			h.mu.Lock()
			entry := h.launchBackoffs[launchKey{"machine-a", test.kind}]
			failures := 0
			if entry != nil {
				failures = entry.failures
			}
			h.mu.Unlock()
			if failures != 1 {
				t.Fatalf("launch backoff for machine-a/%s counts %d failures, want 1", test.kind, failures)
			}
			want := "[machine-a] " + test.kind + deskNotice + test.action
			var notices []string
			eventually(t, 5*time.Second, "desk notice", func() bool {
				notices = append(notices, deskNotices(t, h, destination)...)
				return len(notices) > 0
			})
			if len(notices) != 1 || notices[0] != want {
				t.Fatalf("notices=%q want %q", notices, want)
			}
		})
	}
}

// A dialog refusal whose cleanup was not verified may have left a pane holding
// the job id, so no other host takes the job: the refusal is the answer.
func TestFirstRunRefusalWithoutVerifiedCleanupStaysTerminal(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	done := make(chan fleet.HubSpawnReply, 1)
	go func() {
		reply, _ := dispatchPlacedSpawn(boundedContext(t), transport, firstRunNodeRequest(request, 913, "codex-sol"))
		done <- reply
	}()
	sent := t14ReadNodeSpawn(t, node)
	refusal := firstRunRefusal(sent.RequestID, hooksTail, 90)
	refusal["retryable"] = false
	if err = wsjson.Write(t.Context(), node, refusal); err != nil {
		t.Fatal(err)
	}
	if reply := <-done; reply.NextHost {
		t.Fatalf("an uncleaned pane moved on: %+v", reply)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "failed" || !strings.HasPrefix(saved.Outcome.Reason, "spawn_failed:hooks_review") {
		t.Fatalf("saved=%+v err=%v", saved, err)
	}
}

func TestFirstRunRefusalCodesStayExact(t *testing.T) {
	for reason, want := range map[string]bool{
		"spawn_failed:hooks_review":       true,
		"spawn_failed:trust_prompt: path": true,
		"spawn_failed:omp_setup_wizard":   true,
		"spawn_failed:auth_unavailable":   true,
		"spawn_failed:trust_store":        false,
		"spawn_failed:trust_too_broad":    false,
		"spawn_failed:foreground_missing": false,
		"spawn_failed":                    false,
		"hooks_review":                    false,
	} {
		if got := spawnRefusalTriesNextHost(reason); got != want {
			t.Errorf("%q: got %v", reason, got)
		}
	}
}

func readinessWith(checked time.Time, kind string, harness fleet.HarnessReadiness) *fleet.Readiness {
	return &fleet.Readiness{CheckedAt: checked, Harnesses: map[string]fleet.HarnessReadiness{kind: harness}}
}

// The desk hears a need once while the host keeps reporting it, and again when
// it returns after the readiness cleared.
func TestOneTimeActionNoticeIsToldOncePerUnclearedNeed(t *testing.T) {
	h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	dialog := readinessWith(start, "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview})
	want := `[oci] codex needs a one-time action: run codex once and choose "Trust all and continue"`
	collect := func() (all []string) {
		eventually(t, 5*time.Second, "desk notice", func() bool {
			all = append(all, deskNotices(t, h, destination)...)
			return len(all) > 0
		})
		return all
	}

	h.noteReadinessActions("oci", dialog, start)
	if got := collect(); len(got) != 1 || got[0] != want {
		t.Fatalf("first report: %q", got)
	}
	h.noteReadinessActions("oci", readinessWith(start.Add(45*time.Second), "codex", dialog.Harnesses["codex"]), start.Add(45*time.Second))
	h.noteReadinessActions("oci", readinessWith(start.Add(90*time.Second), "codex", dialog.Harnesses["codex"]), start.Add(90*time.Second))
	// A refusal for the same pair while it is uncleared adds nothing either.
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
	if got := deskNotices(t, h, destination); len(got) != 0 {
		t.Fatalf("an uncleared need was told again: %q", got)
	}

	// A report the node computed before the notice cannot clear it.
	h.noteReadinessActions("oci", readinessWith(start.Add(-time.Minute), "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), start.Add(100*time.Second))
	h.noteReadinessActions("oci", dialog, start.Add(100*time.Second))
	if got := deskNotices(t, h, destination); len(got) != 0 {
		t.Fatalf("a stale clear re-told the need: %q", got)
	}

	// Another host's clean report leaves oci alone.
	h.noteReadinessActions("other", readinessWith(start.Add(2*time.Minute), "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), start.Add(2*time.Minute))
	h.noteReadinessActions("oci", readinessWith(start.Add(3*time.Minute), "codex", dialog.Harnesses["codex"]), start.Add(3*time.Minute))
	if got := deskNotices(t, h, destination); len(got) != 0 {
		t.Fatalf("another host's report cleared oci: %q", got)
	}

	// The readiness clears, then the dialog comes back: one new notice.
	h.noteReadinessActions("oci", readinessWith(start.Add(4*time.Minute), "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), start.Add(4*time.Minute))
	if got := deskNotices(t, h, destination); len(got) != 0 {
		t.Fatalf("a clear report sent a notice: %q", got)
	}
	h.noteReadinessActions("oci", readinessWith(start.Add(5*time.Minute), "codex", dialog.Harnesses["codex"]), start.Add(5*time.Minute))
	if got := collect(); len(got) != 1 || got[0] != want {
		t.Fatalf("returned dialog: %q", got)
	}
}

func TestOneTimeActionNoticeNamesLoginAndTrustAndKeepsScreenOut(t *testing.T) {
	h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	report := &fleet.Readiness{CheckedAt: now, Harnesses: map[string]fleet.HarnessReadiness{
		"claude": {Installed: true, Login: fleet.ReadinessLoginMissing},
		"codex":  {Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexTrust},
		"cmd":    {Installed: true, Login: fleet.ReadinessLoginMissing},
		"devin":  {Installed: true, Login: fleet.ReadinessLoginUnknown},
	}}
	h.noteReadinessActions("oci", report, now)
	var got []string
	eventually(t, 5*time.Second, "two notices", func() bool {
		got = append(got, deskNotices(t, h, destination)...)
		return len(got) >= 2
	})
	if len(got) != 2 {
		t.Fatalf("notices=%q", got)
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"[oci] claude needs a one-time action: log in", "[oci] codex needs a one-time action: trust the workspace"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("notices lack %q: %q", want, got)
		}
	}
	// A refusal tail with screen content reaches no notice.
	secret := "stderr: HOOKS_REVIEW_REQUIRED: SECRET-SCREEN-LINE token=abc123\nFix: x"
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: secret})
	h.noteOneTimeActionRefusal("fresh", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: secret})
	eventually(t, 5*time.Second, "refusal notice", func() bool {
		got = append(got, deskNotices(t, h, destination)...)
		return len(got) >= 3
	})
	if strings.Contains(strings.Join(got, "\n"), "SECRET-SCREEN-LINE") || strings.Contains(strings.Join(got, "\n"), "abc123") {
		t.Fatalf("screen content reached the desk: %q", got)
	}
}

func TestOneTimeActionSendsOneSentryWarning(t *testing.T) {
	h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
	transport := &spawnSentryTransport{}
	h.spawnSentry = spawnReasonSentryClient(t, transport)
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	dialog := readinessWith(now, "codex", fleet.HarnessReadiness{Installed: true, Dialog: fleet.DialogCodexHooksReview})
	h.noteReadinessActions("oci", dialog, now)
	h.noteReadinessActions("oci", dialog, now.Add(time.Minute))
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
	eventually(t, 5*time.Second, "desk notice", func() bool { return len(deskNotices(t, h, destination)) > 0 })
	transport.mu.Lock()
	n := len(transport.events)
	transport.mu.Unlock()
	if n != 1 {
		t.Fatalf("sentry events=%d, want 1 while the need is uncleared", n)
	}
	// Cleared and back: the same fingerprint is sent once more.
	h.noteReadinessActions("oci", readinessWith(now.Add(2*time.Minute), "codex", fleet.HarnessReadiness{Installed: true}), now.Add(2*time.Minute))
	h.noteReadinessActions("oci", readinessWith(now.Add(3*time.Minute), "codex", dialog.Harnesses["codex"]), now.Add(3*time.Minute))
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.events) != 2 || strings.Join(transport.events[1].Fingerprint, "|") != strings.Join(transport.events[0].Fingerprint, "|") {
		t.Fatalf("sentry events after recurrence=%d", len(transport.events))
	}
	event := transport.events[0]
	wantTitle := "ACTION NEEDED: oci codex — run codex once and choose Trust all and continue"
	if event.Message != wantTitle || strings.Join(event.Fingerprint, "|") != "action-needed|oci|codex|run codex once and choose Trust all and continue" {
		t.Fatalf("title=%q fingerprint=%q", event.Message, event.Fingerprint)
	}
	if event.Level != sentry.LevelWarning || event.Tags["host"] != "oci" || event.Tags["kind"] != "codex" || event.Tags["need"] != fleet.DialogCodexHooksReview {
		t.Fatalf("event=%+v", event)
	}
}
