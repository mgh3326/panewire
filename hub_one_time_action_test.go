package panewire

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/getsentry/sentry-go"
	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	hooksTail = "stderr: HOOKS_REVIEW_REQUIRED: Codex is waiting at its Hooks need review screen; brief withheld\nFix: review the hooks"
	trustTail = "stderr: WORKSPACE_TRUST_REQUIRED: Codex directory trust prompt for /work/x; brief withheld\nFix: approve"
	ompTail   = "stderr: OMP_SETUP_WIZARD: omp is in its setup wizard despite OMP_SKIP_SETUP; seat withheld\nFix: run omp"
	loginTail = "stderr: no usable login for the selected account\nFix: sign in"
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

// oneTimeSentry makes h's notifier record each ACTION NEEDED event and accept
// it, and answers the recording transport.
func oneTimeSentry(t *testing.T, h *HubServer) *spawnSentryTransport {
	t.Helper()
	transport := &spawnSentryTransport{}
	h.oneTimeNotify = func(event *sentry.Event) (bool, error) {
		transport.SendEvent(event)
		return true, nil
	}
	return transport
}

// settleOneTimeNotices waits until the notice worker has applied every
// observation handed to it before this call and finished any delivery those
// started. The barrier is queued behind them, so the wait ends on that outcome,
// not on a timer.
func settleOneTimeNotices(t *testing.T, h *HubServer) {
	t.Helper()
	done := make(chan struct{})
	h.enqueueOneTimeObservation(oneTimeObservation{done: done})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the one-time notice worker did not settle")
	}
}

// oneTimeMessages is the message of each Sentry event the hub sent, oldest
// first, once the worker has settled. The ACTION NEEDED event is the only
// operator notice of a one-time action.
func oneTimeMessages(t *testing.T, h *HubServer, transport *spawnSentryTransport) []string {
	t.Helper()
	settleOneTimeNotices(t, h)
	transport.mu.Lock()
	defer transport.mu.Unlock()
	var out []string
	for _, event := range transport.events {
		out = append(out, event.Message)
	}
	return out
}

// deskInboxTexts is every operator-desk inbox item. A one-time action is never
// a lane event, so this stays empty.
func deskInboxTexts(t *testing.T, h *HubServer) []string {
	t.Helper()
	items, err := h.inbox.store.list("operator-desk", false)
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, item := range items {
		texts = append(texts, item.Event.Text)
	}
	return texts
}

// A first-run refusal comes after the node's hk job claim (stage launch), so hk
// holds the job id for that host and another host's claim would be a 409 (hk
// 4614). A cleaned-up harness may be replaced on that host. The host's launch
// backoff still counts the refusal and the operator hears which action it needs.
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
			h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
			transport := oneTimeSentry(t, h)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			spawnTransport := &directSpawnTransport{h: h, task: task}
			type answer struct {
				reply fleet.HubSpawnReply
				err   error
			}
			first := make(chan answer, 1)
			go func() {
				reply, err := dispatchPlacedSpawn(boundedContext(t), spawnTransport, firstRunNodeRequest(request, 911, test.profile))
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
			want := "ACTION NEEDED: machine-a " + test.kind + " — " + strings.ReplaceAll(test.action, `"`, "")
			if got := oneTimeMessages(t, h, transport); len(got) != 1 || got[0] != want {
				t.Fatalf("sentry messages=%q want exactly %q", got, want)
			}
			if got := deskInboxTexts(t, h); len(got) != 0 {
				t.Fatalf("a one-time action reached the desk inbox: %q", got)
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

// The operator hears a need once while it stands, and again only after it has
// cleared for a full report cycle and come back.
func TestOneTimeActionNoticeIsToldOncePerUnclearedNeed(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	dialog := readinessWith(start, "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview})
	clean := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}
	want := `ACTION NEEDED: oci codex — run codex once and choose Trust all and continue`

	h.noteReadinessActions("oci", dialog, start)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 || got[0] != want {
		t.Fatalf("first report: %q", got)
	}
	h.noteReadinessActions("oci", readinessWith(start.Add(45*time.Second), "codex", dialog.Harnesses["codex"]), start.Add(45*time.Second))
	h.noteReadinessActions("oci", readinessWith(start.Add(90*time.Second), "codex", dialog.Harnesses["codex"]), start.Add(90*time.Second))
	// A refusal for the same pair while it is uncleared adds nothing either.
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("an uncleared need was told again: %q", got)
	}

	// A report the node computed before the notice cannot clear it.
	h.noteReadinessActions("oci", readinessWith(start.Add(-time.Minute), "codex", clean), start.Add(100*time.Second))
	h.noteReadinessActions("oci", dialog, start.Add(100*time.Second))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("a stale clear re-told the need: %q", got)
	}

	// Another host's clean report leaves oci alone.
	h.noteReadinessActions("other", readinessWith(start.Add(2*time.Minute), "codex", clean), start.Add(2*time.Minute))
	h.noteReadinessActions("oci", readinessWith(start.Add(3*time.Minute), "codex", dialog.Harnesses["codex"]), start.Add(3*time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("another host's report cleared oci: %q", got)
	}

	// The need clears only once it has stayed gone for one full cycle.
	gone := start.Add(4 * time.Minute)
	h.noteReadinessActions("oci", readinessWith(gone, "codex", clean), gone)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("a clear report sent a notice: %q", got)
	}
	h.noteReadinessActions("oci", readinessWith(gone.Add(cycle), "codex", clean), gone.Add(cycle))
	back := gone.Add(cycle + time.Minute)
	h.noteReadinessActions("oci", readinessWith(back, "codex", dialog.Harnesses["codex"]), back)
	if got := oneTimeMessages(t, h, transport); len(got) != 2 || got[1] != want {
		t.Fatalf("returned dialog: %q", got)
	}
	if got := deskInboxTexts(t, h); len(got) != 0 {
		t.Fatalf("a one-time action reached the desk inbox: %q", got)
	}
}

// The flapping report: need, gone for less than one cycle, need again tells
// the operator once, and the same readiness flapping on past a full cycle is
// still not a second notice until the need has truly cleared.
func TestOneTimeActionFlappingReportTellsOnce(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	dialog := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexHooksReview}
	clean := fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}
	report := func(d time.Duration, harness fleet.HarnessReadiness) {
		when := start.Add(d)
		h.noteReadinessActions("oci", readinessWith(when, "codex", harness), when)
	}

	report(0, dialog)
	report(time.Minute, clean)
	report(2*time.Minute, dialog)
	report(3*time.Minute, clean)
	report(3*time.Minute+cycle-time.Second, clean)
	report(9*time.Minute, dialog)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("a flapping report told the need %d times: %q", len(got), got)
	}
}

// A hub restart on the same store keeps the standing need told: no second
// notice. The need clears on the restarted hub and returns: one new notice.
func TestOneTimeActionRestartKeepsStandingNeed(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	dialog := readinessWith(start, "codex", fleet.HarnessReadiness{Installed: true, Dialog: fleet.DialogCodexHooksReview})
	clean := func(at time.Time) *fleet.Readiness {
		return readinessWith(at, "codex", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent})
	}
	want := `ACTION NEEDED: oci codex — run codex once and choose Trust all and continue`

	first := newOneTimeHub(t, path)
	firstSentry := oneTimeSentry(t, first)
	first.noteReadinessActions("oci", dialog, start)
	if got := oneTimeMessages(t, first, firstSentry); len(got) != 1 || got[0] != want {
		t.Fatalf("first hub: %q", got)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := newOneTimeHub(t, path)
	secondSentry := oneTimeSentry(t, second)
	standing := start.Add(time.Minute)
	second.noteReadinessActions("oci", readinessWith(standing, "codex", dialog.Harnesses["codex"]), standing)
	if got := oneTimeMessages(t, second, secondSentry); len(got) != 0 {
		t.Fatalf("a restart told a standing need again: %q", got)
	}
	second.noteReadinessActions("oci", clean(start.Add(2*time.Minute)), start.Add(2*time.Minute))
	second.noteReadinessActions("oci", clean(start.Add(2*time.Minute+cycle)), start.Add(2*time.Minute+cycle))
	back := start.Add(time.Hour)
	second.noteReadinessActions("oci", readinessWith(back, "codex", dialog.Harnesses["codex"]), back)
	if got := oneTimeMessages(t, second, secondSentry); len(got) != 1 || got[0] != want {
		t.Fatalf("returned need after the restart: %q", got)
	}
}

// newOneTimeHub opens a hub on the store at path. Close is safe to repeat, so
// a test may close the hub early and still leave the cleanup in place.
func newOneTimeHub(t *testing.T, path string) *HubServer {
	t.Helper()
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func TestOneTimeActionNoticeNamesLoginAndTrustAndKeepsScreenOut(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	report := &fleet.Readiness{CheckedAt: now, Harnesses: map[string]fleet.HarnessReadiness{
		"claude": {Installed: true, Login: fleet.ReadinessLoginMissing},
		"codex":  {Installed: true, Login: fleet.ReadinessLoginPresent, Dialog: fleet.DialogCodexTrust},
		"cmd":    {Installed: true, Login: fleet.ReadinessLoginMissing},
		"devin":  {Installed: true, Login: fleet.ReadinessLoginUnknown},
	}}
	h.noteReadinessActions("oci", report, now)
	got := oneTimeMessages(t, h, transport)
	if len(got) != 2 {
		t.Fatalf("notices=%q", got)
	}
	joined := strings.Join(got, "\n")
	for _, want := range []string{"ACTION NEEDED: oci claude — log in", "ACTION NEEDED: oci codex — trust the workspace"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("notices lack %q: %q", want, got)
		}
	}
	// A refusal tail with screen content reaches no notice.
	secret := "stderr: HOOKS_REVIEW_REQUIRED: SECRET-SCREEN-LINE token=abc123\nFix: x"
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: secret})
	h.noteOneTimeActionRefusal("fresh", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: secret})
	all := strings.Join(oneTimeMessages(t, h, transport), "\n")
	if strings.Contains(all, "SECRET-SCREEN-LINE") || strings.Contains(all, "abc123") {
		t.Fatalf("screen content reached the operator: %q", all)
	}
	if n := len(oneTimeMessages(t, h, transport)); n != 4 {
		t.Fatalf("notices after the refusals = %d, want 4", n)
	}
}

func TestOneTimeActionSendsOneSentryWarning(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	dialog := readinessWith(now, "codex", fleet.HarnessReadiness{Installed: true, Dialog: fleet.DialogCodexHooksReview})
	h.noteReadinessActions("oci", dialog, now)
	h.noteReadinessActions("oci", dialog, now.Add(time.Minute))
	h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
	if n := len(oneTimeMessages(t, h, transport)); n != 1 {
		t.Fatalf("sentry events=%d, want 1 while the need is uncleared", n)
	}
	// Cleared for a full cycle and back: the same fingerprint is sent once more.
	gone := now.Add(2 * time.Minute)
	clean := readinessWith(gone, "codex", fleet.HarnessReadiness{Installed: true})
	h.noteReadinessActions("oci", clean, gone)
	h.noteReadinessActions("oci", readinessWith(gone.Add(cycle), "codex", fleet.HarnessReadiness{Installed: true}), gone.Add(cycle))
	back := gone.Add(cycle + time.Minute)
	h.noteReadinessActions("oci", readinessWith(back, "codex", dialog.Harnesses["codex"]), back)
	settleOneTimeNotices(t, h)
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

// A need of a kind no readiness report covers (kiro, opencode) is never
// cleared by a report: the report says nothing about it, so its refusal is not
// told again on every cycle.
func TestOneTimeActionUncoveredKindIsNotClearedByReports(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	h.noteOneTimeAction("oci", "kiro", oneTimeLoginMissing, start)
	covered := func(at time.Time) *fleet.Readiness {
		return &fleet.Readiness{CheckedAt: at, Harnesses: map[string]fleet.HarnessReadiness{
			"claude": {Installed: true, Login: fleet.ReadinessLoginPresent},
		}}
	}
	for _, at := range []time.Time{start.Add(time.Minute), start.Add(time.Minute + cycle), start.Add(time.Minute + 2*cycle)} {
		h.noteReadinessActions("oci", covered(at), at)
	}
	later := start.Add(time.Minute + 3*cycle)
	h.noteOneTimeAction("oci", "kiro", oneTimeLoginMissing, later)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 || got[0] != "ACTION NEEDED: oci kiro — log in" {
		t.Fatalf("an uncovered need was told again after reports that do not cover it: %q", got)
	}
}

// Without a configured Sentry reporter nothing is delivered, so the need is not
// recorded as told: a later report, once reporting is configured, tells it.
func TestOneTimeActionUntoldWhenSentryIsNotConfigured(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now)
	settleOneTimeNotices(t, h)
	h.oneTimeMu.Lock()
	recorded := len(h.oneTimeActions)
	h.oneTimeMu.Unlock()
	if recorded != 0 {
		t.Fatalf("an undelivered need was recorded as told: %d", recorded)
	}
	told, err := h.inbox.store.oneTimeActions(t.Context())
	if err != nil || len(told) != 0 {
		t.Fatalf("an undelivered need was stored as told: %v %v", told, err)
	}

	transport := oneTimeSentry(t, h)
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now.Add(5*time.Minute))
	if got := oneTimeMessages(t, h, transport); len(got) != 1 || got[0] != "ACTION NEEDED: oci codex — run codex once and choose Trust all and continue" {
		t.Fatalf("the need was not told once reporting is configured: %q", got)
	}
}

// oneTimeSentryServer is a Sentry stand-in answering each event with status and
// counting the requests it received. It returns the DSN that reaches it.
func oneTimeSentryServer(t *testing.T, status *atomic.Int32, requests *atomic.Int32) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(server.Close)
	return strings.Replace(server.URL, "http://", "http://public@", 1) + "/1"
}

// A need is told only when Sentry answers its notice with a 2xx. A rejected
// notice (403 here) is not recorded and the next report sends it again; once
// Sentry accepts it, the need is told and not sent again.
func TestOneTimeActionNeedsSentryAcceptance(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusForbidden)
	var requests atomic.Int32
	t.Setenv(crashReportDSNEnv, oneTimeSentryServer(t, &status, &requests))
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	toldCount := func() int {
		settleOneTimeNotices(t, h)
		h.oneTimeMu.Lock()
		defer h.oneTimeMu.Unlock()
		return len(h.oneTimeActions)
	}

	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now)
	settleOneTimeNotices(t, h)
	if requests.Load() == 0 {
		t.Fatal("the notice never reached Sentry")
	}
	if n := toldCount(); n != 0 {
		t.Fatalf("a notice Sentry rejected was recorded as told: %d", n)
	}

	status.Store(http.StatusOK)
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now.Add(time.Minute))
	if n := toldCount(); n != 1 {
		t.Fatalf("a notice Sentry accepted was not recorded as told: %d", n)
	}
	sent := requests.Load()
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now.Add(2*time.Minute))
	settleOneTimeNotices(t, h)
	if requests.Load() != sent {
		t.Fatal("a told need was sent to Sentry again")
	}
}

// The notice goes only to the panewire DSN. With SENTRY_DSN_PANEWIRE unset, the
// SDK's SENTRY_DSN fallback must not carry it, and the need is not told.
func TestOneTimeActionIgnoresAmbientSentryDSN(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	var requests atomic.Int32
	t.Setenv("SENTRY_DSN", oneTimeSentryServer(t, &status, &requests))
	t.Setenv(crashReportDSNEnv, "")
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC))
	settleOneTimeNotices(t, h)
	if n := requests.Load(); n != 0 {
		t.Fatalf("the ambient SENTRY_DSN received %d notices", n)
	}
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	if len(h.oneTimeActions) != 0 {
		t.Fatal("a need with no panewire DSN was recorded as told")
	}
}

// An unmeasured login (the probe failed or was rate limited) is neither gone nor
// a clear: a need that the node last saw present stays told through it, and
// the login coming back missing is not a second notice.
func TestOneTimeActionUnmeasuredLoginDoesNotClear(t *testing.T) {
	h, _, _ := spawnNotifyHubLane(t, "", "operator-desk")
	transport := oneTimeSentry(t, h)
	start := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	cycle := oneTimeActionClearCycle
	claude := func(at time.Time, login string) *fleet.Readiness {
		return readinessWith(at, "claude", fleet.HarnessReadiness{Installed: true, Login: login})
	}
	h.noteReadinessActions("oci", claude(start, fleet.ReadinessLoginMissing), start)
	h.noteReadinessActions("oci", claude(start.Add(time.Minute), fleet.ReadinessLoginPresent), start.Add(time.Minute))
	unknownAt := start.Add(time.Minute + cycle)
	h.noteReadinessActions("oci", claude(unknownAt, fleet.ReadinessLoginUnknown), unknownAt)
	missingAt := start.Add(2*time.Minute + cycle)
	h.noteReadinessActions("oci", claude(missingAt, fleet.ReadinessLoginMissing), missingAt)
	if got := oneTimeMessages(t, h, transport); len(got) != 1 {
		t.Fatalf("an unmeasured login cleared the need and it was told again: %q", got)
	}
}

// A 2xx that arrives after its attempt's deadline is not the notice's
// acceptance. The first attempt's request is held past its two-second deadline
// and only then answered 2xx; the retry's own answer is 403, so neither attempt
// is accepted and nothing is stored. A later attempt's own 2xx tells the need.
// The first attempt waits out its deadline, so this test takes about two seconds.
func TestOneTimeActionLateAcceptanceNeverTellsTheNeed(t *testing.T) {
	release := make(chan struct{})
	answered := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch requests.Add(1) {
		case 1:
			<-release
			w.WriteHeader(http.StatusOK)
			close(answered)
		case 2:
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(server.Close)
	unhold := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unhold)
	t.Setenv(crashReportDSNEnv, strings.Replace(server.URL, "http://", "http://public@", 1)+"/1")
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	told := func() int {
		settleOneTimeNotices(t, h)
		h.oneTimeMu.Lock()
		defer h.oneTimeMu.Unlock()
		return len(h.oneTimeActions)
	}

	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now)
	settleOneTimeNotices(t, h)
	unhold()
	<-answered
	if n := told(); n != 0 {
		t.Fatalf("a 2xx that arrived after its deadline told the need: %d", n)
	}

	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now.Add(time.Minute))
	if n := told(); n != 0 {
		t.Fatalf("the retry's own 403 told the need: %d", n)
	}

	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now.Add(2*time.Minute))
	if n := told(); n != 1 {
		t.Fatalf("a later attempt's own 2xx did not tell the need: %d", n)
	}
	stored, err := h.inbox.store.oneTimeActions(t.Context())
	if err != nil || len(stored) != 1 {
		t.Fatalf("the told need was not stored once its own 2xx arrived: %v %v", stored, err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("delivery requests = %d, want 3 (held, 403, 2xx)", got)
	}
}

// A stalled receiver delays neither a refusal result nor hub shutdown, and holds
// no lock while it waits: the refusal returns while its request is open,
// another goroutine takes oneTimeMu, and Close cancels the request and joins
// the notice worker before it returns.
func TestOneTimeActionStalledReceiverDelaysNeitherRefusalNorClose(t *testing.T) {
	release := make(chan struct{})
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	unhold := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unhold)
	t.Setenv(crashReportDSNEnv, strings.Replace(server.URL, "http://", "http://public@", 1)+"/1")
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))

	refused := make(chan struct{})
	go func() {
		h.noteOneTimeActionRefusal("oci", "codex-sol", hubSpawnResult{RC: 90, StdoutTail: hooksTail})
		close(refused)
	}()
	select {
	case <-refused:
	case <-time.After(5 * time.Second):
		t.Fatal("a refusal waited on the stalled receiver")
	}
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("the notice never reached the receiver")
	}
	locked := make(chan struct{})
	go func() {
		h.oneTimeMu.Lock()
		h.oneTimeMu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("the stalled delivery held oneTimeMu")
	}

	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown waited on the stalled receiver")
	}
	h.oneTimeQueue.mu.Lock()
	joined := h.oneTimeQueue.done
	h.oneTimeQueue.mu.Unlock()
	select {
	case <-joined:
	default:
		t.Fatal("shutdown returned before the notice worker was joined")
	}
}

// Only a delivery's own 2xx tells its need: a 2xx for one need does not tell
// another, whose own answer is 403.
func TestOneTimeActionOnlyItsOwnTwoXXTellsANeed(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	var requests atomic.Int32
	t.Setenv(crashReportDSNEnv, oneTimeSentryServer(t, &status, &requests))
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	now := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, now)
	settleOneTimeNotices(t, h)

	status.Store(http.StatusForbidden)
	h.noteOneTimeAction("oci", "claude", fleet.DialogClaudeTrust, now.Add(time.Minute))
	settleOneTimeNotices(t, h)
	if requests.Load() != 2 {
		t.Fatalf("delivery requests = %d, want 2", requests.Load())
	}
	h.oneTimeMu.Lock()
	defer h.oneTimeMu.Unlock()
	if _, told := h.oneTimeActions[oneTimeActionKey{"oci", "claude", fleet.DialogClaudeTrust}]; told {
		t.Fatal("another need's 2xx told a need whose own answer was 403")
	}
}

// A delivery holds no lock while it is in flight, and shutdown waits for it. The
// delivery blocks until the test releases it: oneTimeMu must stay free, and
// Close must not return before the delivery has.
func TestOneTimeActionDeliveryHoldsNoLockAndCloseJoinsIt(t *testing.T) {
	t.Setenv(crashReportDSNEnv, "")
	h := newOneTimeHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	releaseDelivery := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(releaseDelivery)
	h.oneTimeNotify = func(*sentry.Event) (bool, error) {
		started <- struct{}{}
		<-unblock
		return true, nil
	}
	h.noteOneTimeAction("oci", "codex", fleet.DialogCodexHooksReview, time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC))
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivery never started")
	}

	locked := make(chan struct{})
	go func() {
		h.oneTimeMu.Lock()
		h.oneTimeMu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("the delivery held oneTimeMu")
	}

	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	// Shutdown must still be waiting on the delivery. This 200 ms window only
	// proves that Close does not return early; it waits for nothing else.
	select {
	case <-closed:
		t.Fatal("shutdown returned while a delivery was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	releaseDelivery()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return once the delivery finished")
	}
}
