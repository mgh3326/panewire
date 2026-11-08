package panewire

// hk 3517: Command Code sometimes never reports the end of a turn, so herdr
// keeps a cmd pane `working` after its final answer. A held row for such a pane
// is typed once the node proves it stuck from herdr's own reads: the visible
// ANSI screen byte-identical across re-checks at least relay.cmd_stale_after
// apart, and an empty composer. The screen is the real capture from the
// incident (testdata/cmd_stuck_working_screen.ansi, hk:doc
// evidence/2026-11-08/cmd-stuck-working-screen). Everything runs on fakes: a
// real hub, a real node relay and a shell herdr.

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var t3517SGR = regexp.MustCompile("\x1b\\[[0-9;]*m")

// t3517Pane is a shell herdr for one cmd pane stuck at working. Its screen is
// the incident capture; a test swaps the screen, the composer or the occupant
// between re-checks. Typing a row and pressing return redraws the pane
// according to the redraw mode, and the status stays working throughout.
type t3517Pane struct {
	dir string
	t   *testing.T
}

const (
	// t3517Redraw: the post-return screen echoes the row and differs.
	t3517Redraw = "redraw"
	// t3517NoRedraw: the pane shows the stale screen again after the return.
	t3517NoRedraw = "noredraw"
	// t3517EchoPlainOnly: the plain read echoes the row, but the ANSI screen is
	// still byte-identical to the stale one.
	t3517EchoPlainOnly = "echo_plain_only"
	// t3517ChangedNoEcho: the ANSI screen changes, but nothing echoes the row.
	t3517ChangedNoEcho = "changed_no_echo"
)

func t3517Install(t *testing.T) *t3517Pane {
	t.Helper()
	dir := t.TempDir()
	pane := &t3517Pane{dir: dir, t: t}
	pane.write("status", "working\n")
	pane.write("terminal", "term-a\n")
	pane.write("mode", t3517Redraw)
	pane.setScreen(pane.fixture())
	script := `#!/bin/sh
d='` + dir + `'
div='` + t3330CmdDivider + `'
echo "$1 $2" >> "$d/calls"
mode=$(cat "$d/mode")
ansi=0
for a in "$@"; do [ "$a" = ansi ] && ansi=1; done
case "$1 $2" in
"agent get")
  printf '{"result":{"type":"agent_info","agent":{"agent":"cmd","agent_status":"%s","name":"` + t3276Worker + `","pane_id":"w1:pW","terminal_id":"%s"}}}\n' "$(cat "$d/status")" "$(cat "$d/terminal")" ;;
"agent read"|"pane read")
  if [ "$ansi" = 1 ] && [ -f "$d/ansi_fail" ]; then echo 'pane read failed' >&2; exit 1; fi
  if [ -f "$d/composer" ]; then
    printf '%s\n❯ %s\n%s\n' "$div" "$(cat "$d/composer")" "$div"
  else
    if [ -f "$d/submitted" ]; then
      case "$mode" in
      redraw) printf '❯ %s\n' "$(cat "$d/submitted")" ;;
      echo_plain_only) [ "$ansi" = 1 ] || printf '❯ %s\n' "$(cat "$d/submitted")" ;;
      changed_no_echo) [ "$ansi" = 1 ] && printf 'thinking...\n' ;;
      esac
    fi
    if [ "$ansi" = 1 ]; then cat "$d/screen.ansi"; else cat "$d/screen.plain"; fi
  fi ;;
"pane send-text") printf '%s' "$4" > "$d/composer"; printf '%s\n' "$4" >> "$d/typed" ;;
"pane send-keys")
  if [ "$4" = return ] && [ -f "$d/composer" ]; then mv "$d/composer" "$d/submitted"; fi ;;
"agent prompt"|"agent send-keys")
  echo '{"error":{"code":"agent_not_ready","message":"not an active named agent"}}' >&2; exit 1 ;;
esac
`
	installFakeHerdr(t, dir, script)
	return pane
}

func (pane *t3517Pane) fixture() string {
	pane.t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "cmd_stuck_working_screen.ansi"))
	if err != nil {
		pane.t.Fatal(err)
	}
	return string(body)
}

func (pane *t3517Pane) write(name, body string) {
	pane.t.Helper()
	if err := os.WriteFile(filepath.Join(pane.dir, name), []byte(body), 0o600); err != nil {
		pane.t.Fatal(err)
	}
}

// setScreen is what the pane draws while it sits stuck.
func (pane *t3517Pane) setScreen(ansi string) {
	pane.t.Helper()
	pane.write("screen.ansi", ansi)
	pane.write("screen.plain", strings.ReplaceAll(t3517SGR.ReplaceAllString(ansi, ""), "\r", "")+"\n")
}

func (pane *t3517Pane) setAnsiFails(fail bool) {
	pane.t.Helper()
	path := filepath.Join(pane.dir, "ansi_fail")
	if !fail {
		_ = os.Remove(path)
		return
	}
	pane.write("ansi_fail", "")
}

func (pane *t3517Pane) typed() []string { return (&t3384Pane{dir: pane.dir}).typed(pane.t) }

func (pane *t3517Pane) calls(pair string) int {
	return (&t3330CmdPane{dir: pane.dir}).calls(pane.t, pair)
}

// assertNothingTyped fails when anything reached the pane.
func (pane *t3517Pane) assertNothingTyped() {
	pane.t.Helper()
	for _, pair := range []string{"pane send-text", "pane send-keys", "agent prompt", "agent send-keys"} {
		if got := pane.calls(pair); got != 0 {
			pane.t.Fatalf("a stuck cmd pane got %d %q calls, typed=%q", got, pair, pane.typed())
		}
	}
}

// t3517Recheck is one re-check of the held rows: the node clock moves on and
// the wait that ran out wakes the release, the way relayCmdHoldRecheck arms it.
func t3517Recheck(t *testing.T, rig *t3276Rig, clock *t3411Clock, after time.Duration) {
	t.Helper()
	clock.advance(after)
	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", true)
}

// t3517Rig starts the node with a working cmd pane and rows held on it, with the
// observation window short enough for a send that never proves.
func t3517Rig(t *testing.T, policies ...string) (*t3276Rig, *t3411Clock, *t3517Pane, []hubRelayInjectEvent) {
	t.Helper()
	prevWindow := relayObserveWindow
	relayObserveWindow = 300 * time.Millisecond
	t.Cleanup(func() { relayObserveWindow = prevWindow })
	rig, clock := t3411Start(t)
	pane := t3517Install(t)
	held := t3384Hold(t, rig, policies...)
	return rig, clock, pane, held
}

// t3517Warnings captures slog for the test.
func t3517Warnings(t *testing.T) func() string {
	t.Helper()
	var mu sync.Mutex
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &buffer}, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return buffer.String()
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (writer *lockedWriter) Write(p []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.w.Write(p)
}

func TestCmdIncidentFixtureReadsComposerEmpty_t3517(t *testing.T) {
	pane := &t3517Pane{t: t}
	if got := relayAnsiComposerState(pane.fixture()); got != "composer_empty" {
		t.Fatalf("incident screen composer=%q, want composer_empty", got)
	}
}

// Acceptance 1: a screen frozen across re-checks spanning stale_after types one
// row, even with several held, and the row is recorded delivered on the nonce
// echo plus a screen that differs from the stale one.
func TestCmdStaleWorkingFrozenScreenTypesOneRow_t3517(t *testing.T) {
	warnings := t3517Warnings(t)
	rig, clock, pane, held := t3517Rig(t, "idle", "idle", "idle")

	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	pane.assertNothingTyped()
	if kinds, _ := rig.drainEvents(); len(kinds) != 0 {
		t.Fatalf("events=%v before stale_after, want none", kinds)
	}
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)

	if got := pane.calls("pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want exactly one row typed (typed=%q)", got, pane.typed())
	}
	if got := pane.calls("pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys called %d times, want 1", got)
	}
	nonce := relayNonce(relayHeld{EventID: held[0].EventID, Text: held[0].Text})
	if typed := pane.typed(); len(typed) != 1 || !strings.Contains(typed[0], nonce) || len(relayNoncesIn(typed[0])) != 1 {
		t.Fatalf("typed=%q, want only the first row's nonce %s", typed, nonce)
	}
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want one relay.delivered", kinds)
	}
	reason := payloads["relay.delivered"].Reason
	if !strings.HasPrefix(reason, relayStaleWorkingEvidence) || !strings.Contains(reason, "nonce_echo+screen_changed") {
		t.Fatalf("relay.delivered reason=%q, want the stale_working: prefix and the screen-changed proof", reason)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("the proven row stayed undelivered: %+v", row)
	}
	rows := t3384HeldRows(t, rig)
	if len(rows) != 2 || rows[0].EventID == held[0].EventID || rows[1].EventID == held[0].EventID {
		t.Fatalf("held rows=%+v, want the two rows behind the typed one still held", rows)
	}
	log := warnings()
	if strings.Count(log, "relay cmd pane stale working") != 1 || !strings.Contains(log, "pane=w1:pW") || !strings.Contains(log, "lane="+t3276Worker) {
		t.Fatalf("slog=%q, want one stale-working warning naming the pane and lane", log)
	}
}

// A directive (an ESC reply) takes the same path, and a send that proves
// nothing is never typed a second time.
func TestCmdStaleWorkingDirectiveDelivered_t3517(t *testing.T) {
	rig, clock, pane, held := t3517Rig(t, relayDeliverDirective)
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter)

	if got := pane.calls("pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want 1 (typed=%q)", got, pane.typed())
	}
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || !strings.HasPrefix(payloads["relay.delivered"].Reason, relayStaleWorkingEvidence) {
		t.Fatalf("events=%v reason=%q, want one stale_working: delivery", kinds, payloads["relay.delivered"].Reason)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("row stayed undelivered: %+v", row)
	}
}

// Acceptance 2: the screen changes between re-checks (a spinner glyph) for the
// whole window, so it is never frozen for stale_after. Nothing is typed.
func TestCmdStaleWorkingChangingScreenNotTyped_t3517(t *testing.T) {
	warnings := t3517Warnings(t)
	rig, clock, pane, held := t3517Rig(t, "idle")
	fixture := pane.fixture()
	spinner := strings.Replace(fixture, "Worked for 33m 0s", "Worked for 33m 1s", 1)
	if spinner == fixture {
		t.Fatal("the spinner variant did not change the screen")
	}
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	for index := 0; index < 6; index++ {
		if index%2 == 0 {
			pane.setScreen(spinner)
		} else {
			pane.setScreen(fixture)
		}
		t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	}

	pane.assertNothingTyped()
	if rows := t3384HeldRows(t, rig); len(rows) != 1 || rows[0].EventID != held[0].EventID {
		t.Fatalf("held rows=%+v, want the row still held", rows)
	}
	if kinds, _ := rig.drainEvents(); len(kinds) != 0 {
		t.Fatalf("events=%v, want none", kinds)
	}
	if log := warnings(); strings.Contains(log, "stale working") {
		t.Fatalf("slog=%q, want no stale warning for a pane that redraws", log)
	}
}

// Acceptance 3: the screen is frozen but the composer holds a draft.
func TestCmdStaleWorkingComposerNotEmptyNotTyped_t3517(t *testing.T) {
	rig, clock, pane, _ := t3517Rig(t, "idle")
	draft := strings.Replace(pane.fixture(), "\x1b[0m\x1b[7mA\x1b[0m\x1b[38;2;138;148;168msk your question...\x1b[0m", "half-typed draft", 1)
	if got := relayAnsiComposerState(draft); got != "composer_pending" {
		t.Fatalf("draft screen composer=%q, want composer_pending", got)
	}
	pane.setScreen(draft)
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)

	pane.assertNothingTyped()
	if rows := t3384HeldRows(t, rig); len(rows) != 1 {
		t.Fatalf("held rows=%+v, want the row still held", rows)
	}
}

// Acceptance 4: frozen for less than stale_after.
func TestCmdStaleWorkingFrozenTooShortNotTyped_t3517(t *testing.T) {
	rig, clock, pane, _ := t3517Rig(t, "idle")
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter-time.Second)

	pane.assertNothingTyped()
	if rows := t3384HeldRows(t, rig); len(rows) != 1 {
		t.Fatalf("held rows=%+v, want the row still held", rows)
	}
}

// The setting is honoured: with a shorter relay.cmd_stale_after the same two
// re-checks that were too close before are enough.
func TestCmdStaleWorkingStaleAfterSetting_t3517(t *testing.T) {
	t.Setenv("PANEWIRE_RELAY_CMD_STALE_AFTER", "2m")
	if got := relayCmdStaleAfterFromEnv(); got != 2*time.Minute {
		t.Fatalf("relayCmdStaleAfterFromEnv=%s, want 2m", got)
	}
	t.Setenv("PANEWIRE_RELAY_CMD_STALE_AFTER", "soon")
	if got := relayCmdStaleAfterFromEnv(); got != defaultRelayCmdStaleAfter {
		t.Fatalf("an invalid setting read as %s, want the default", got)
	}
	rig, clock, pane, _ := t3517Rig(t, "idle")
	rig.node.relayControl.cmdStaleAfter = 2 * time.Minute
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, 2*time.Minute)
	if got := pane.calls("pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times with stale_after=2m, want 1", got)
	}
}

// Acceptance 5: another occupant takes the pane between the two reads. The
// lease gate drops the row, and the new occupant's screen starts a window of its
// own.
func TestCmdStaleWorkingLeaseChangeNotTyped_t3517(t *testing.T) {
	rig, clock, pane, held := t3517Rig(t, "idle")
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	pane.write("terminal", "term-b\n")
	t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter+time.Minute)

	pane.assertNothingTyped()
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.dropped") != 1 || payloads["relay.dropped"].Reason != "pane_occupant_changed" {
		t.Fatalf("events=%v dropped=%+v, want one pane_occupant_changed drop", kinds, payloads["relay.dropped"])
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
		t.Fatalf("a dropped row was marked delivered: %+v", row)
	}
}

// The frozen window belongs to one occupant: the same screen read for another
// occupant starts over.
func TestCmdStaleWorkingSampleIsPerLease_t3517(t *testing.T) {
	rig, clock, pane, _ := t3517Rig(t, "idle")
	manager := rig.node.relayBusyManager()
	item := relayHeld{Pane: "w1:pW", Lane: t3276Worker, EventID: 1}
	steps := []struct {
		advance time.Duration
		lease   string
		stale   bool
	}{
		{0, "occupant-a", false},
		{defaultRelayCmdStaleAfter, "occupant-b", false},
		{defaultRelayCmdStaleAfter - time.Second, "occupant-b", false},
		{time.Second, "occupant-b", true},
	}
	for index, step := range steps {
		clock.advance(step.advance)
		if got := manager.cmdStaleWorkingDigest(t.Context(), item, step.lease) != ""; got != step.stale {
			t.Fatalf("step %d (%+v): stale=%v, want %v", index, step, got, step.stale)
		}
	}
	pane.assertNothingTyped()
}

// A pane that leaves working has no frozen window left.
func TestCmdStaleWorkingSampleForgottenWhenPaneLeavesWorking_t3517(t *testing.T) {
	rig, clock, pane, _ := t3517Rig(t, "idle")
	manager := rig.node.relayBusyManager()
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	manager.mu.Lock()
	sampled := len(manager.cmdScreens)
	manager.mu.Unlock()
	if sampled != 1 {
		t.Fatalf("samples=%d after the first re-check, want 1", sampled)
	}
	pane.write("status", "idle\n")
	manager.release(context.Background(), "w1:pW", false)
	manager.mu.Lock()
	sampled = len(manager.cmdScreens)
	manager.mu.Unlock()
	if sampled != 0 {
		t.Fatalf("samples=%d after the pane turned idle, want 0", sampled)
	}
}

// Acceptance 6: a failed ANSI read at either re-check is not evidence.
func TestCmdStaleWorkingReadErrorNotTyped_t3517(t *testing.T) {
	for _, failAt := range []string{"first", "second"} {
		t.Run(failAt, func(t *testing.T) {
			rig, clock, pane, _ := t3517Rig(t, "idle")
			pane.setAnsiFails(failAt == "first")
			t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
			pane.setAnsiFails(failAt == "second")
			t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter+time.Minute)

			pane.assertNothingTyped()
			if rows := t3384HeldRows(t, rig); len(rows) != 1 {
				t.Fatalf("held rows=%+v, want the row still held", rows)
			}
			if kinds, _ := rig.drainEvents(); len(kinds) != 0 {
				t.Fatalf("events=%v, want none", kinds)
			}
		})
	}
}

// Acceptance 7: a send into a stale pane is proven only by the nonce echo and a
// changed screen. Either alone, or neither, is maybe_in_pane, and the row is
// never typed a second time (a directive's retry-once does not apply).
func TestCmdStaleWorkingUnprovenSendNeverRetyped_t3517(t *testing.T) {
	for _, mode := range []string{t3517NoRedraw, t3517EchoPlainOnly, t3517ChangedNoEcho} {
		t.Run(mode, func(t *testing.T) {
			rig, clock, pane, held := t3517Rig(t, relayDeliverDirective)
			pane.write("mode", mode)
			t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
			t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter)
			// A later re-check meets a row that is no longer held.
			t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter)

			if got := pane.calls("pane send-text"); got != 1 {
				t.Fatalf("pane send-text called %d times, want the row typed once (typed=%q)", got, pane.typed())
			}
			kinds, payloads := rig.drainEvents()
			if t3276Count(kinds, "relay.delivered") != 0 || t3276Count(kinds, "relay.unconfirmed") != 1 {
				t.Fatalf("events=%v, want one relay.unconfirmed and no delivery", kinds)
			}
			reason := payloads["relay.unconfirmed"].Reason
			if !strings.Contains(reason, "maybe_in_pane") || !strings.Contains(reason, relayStaleWorkingEvidence) {
				t.Fatalf("relay.unconfirmed reason=%q, want maybe_in_pane with the stale_working: prefix", reason)
			}
			if row := rig.row(held[0].EventID); row.DeliveredAt != "" {
				t.Fatalf("an unproven row was marked delivered: %+v", row)
			}
			if rows := t3384HeldRows(t, rig); len(rows) != 0 {
				t.Fatalf("held rows=%+v, want the unproven row gone from the hold", rows)
			}
		})
	}
}

// A row typed behind a stale one finds the pane working and stays held, so it
// is never typed on the stale proof.
func TestCmdStaleWorkingRowsBehindStayHeldUntilNextRecheck_t3517(t *testing.T) {
	rig, clock, pane, held := t3517Rig(t, "idle", "idle")
	t3517Recheck(t, rig, clock, defaultRelayMaxWait+time.Minute)
	t3517Recheck(t, rig, clock, defaultRelayCmdStaleAfter)
	if got := pane.calls("pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want 1", got)
	}
	// The pane drew the first row and then froze again: a new window starts
	// from that screen, so the second row waits stale_after more.
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	if got := pane.calls("pane send-text"); got != 1 {
		t.Fatalf("the second row was typed %d times in a window shorter than stale_after", got)
	}
	t3517Recheck(t, rig, clock, relayCmdHoldRecheck)
	if got := pane.calls("pane send-text"); got != 2 {
		t.Fatalf("pane send-text called %d times after a second frozen window, want 2 (typed=%q)", got, pane.typed())
	}
	typed := pane.typed()
	for index, directive := range held {
		nonce := relayNonce(relayHeld{EventID: directive.EventID, Text: directive.Text})
		if !strings.Contains(typed[index], nonce) {
			t.Fatalf("inject %d=%q, want row %d (%s)", index+1, typed[index], directive.EventID, nonce)
		}
	}
}
