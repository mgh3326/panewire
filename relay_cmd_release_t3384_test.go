package panewire

// hk 3384: a release never types into a working cmd pane, whatever the held
// row's policy, and it types one row per inject into a cmd pane. Production
// v0.0.138 typed eight idle and max_wait rows as one batch into a working cmd
// composer; the return gate withheld the Enter and the batch sat unsent there.
// Everything runs on fakes: a real hub, a real node relay and a shell herdr.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// t3384Pane is a shell herdr for one pane that echoes whatever was typed into
// it. A cmd pane takes text with pane send-text, shows it in its composer, and
// on pane send-keys return moves it into the transcript and turns working. A
// claude pane takes text with agent prompt, which submits it at once. The
// status file is the pane's status; a test writes it to end a turn.
type t3384Pane struct {
	dir string
}

func t3384Install(t *testing.T, harness, status string) *t3384Pane {
	t.Helper()
	dir := t.TempDir()
	pane := &t3384Pane{dir: dir}
	pane.setStatus(t, status)
	script := `#!/bin/sh
d='` + dir + `'
div='` + t3330CmdDivider + `'
echo "$1 $2" >> "$d/calls"
screen() {
  if [ -f "$d/submitted" ]; then printf '❯ %s\ndone\n' "$(cat "$d/submitted")"; fi
  printf '%s\n' "$div"
  if [ -f "$d/composer" ]; then printf '❯ %s\n' "$(cat "$d/composer")"; else printf '❯\n'; fi
  printf '%s\n' "$div"
}
case "$1 $2" in
"agent get")
  printf '{"result":{"agent":{"agent":"` + harness + `","agent_status":"%s","name":"` + t3276Worker + `","pane_id":"w1:pW"}}}\n' "$(cat "$d/status")" ;;
"agent read"|"pane read") screen ;;
"pane send-text") printf '%s' "$4" > "$d/composer"; printf '%s\n' "$4" >> "$d/typed" ;;
"pane send-keys")
  if [ "$4" = return ] && [ -f "$d/composer" ]; then mv "$d/composer" "$d/submitted"; echo working > "$d/status"; fi ;;
"agent prompt")
  if [ "` + harness + `" = cmd ]; then
    echo '{"error":{"code":"agent_not_ready","message":"not an active named agent"}}' >&2; exit 1
  fi
  printf '%s' "$4" > "$d/submitted"; printf '%s\n' "$4" >> "$d/typed"; echo working > "$d/status" ;;
"agent send-keys")
  if [ "` + harness + `" = cmd ]; then exit 1; fi ;;
esac
`
	installFakeHerdr(t, dir, script)
	return pane
}

func (pane *t3384Pane) setStatus(t *testing.T, status string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(pane.dir, "status"), []byte(status+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (pane *t3384Pane) calls(t *testing.T, pair string) int {
	t.Helper()
	return (&t3330CmdPane{dir: pane.dir}).calls(t, pair)
}

// typed is every text the pane took, one per send.
func (pane *t3384Pane) typed(t *testing.T) []string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, "typed"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
}

// t3384Hold offers one event per policy to a pane that is working, so each is
// held, and returns the directives in offer order.
func t3384Hold(t *testing.T, rig *t3276Rig, policies ...string) []hubRelayInjectEvent {
	t.Helper()
	manager := rig.node.relayBusyManager()
	var held []hubRelayInjectEvent
	for index, policy := range policies {
		directive := rig.send("ev-t3384-"+string(rune('a'+index)), "report", "[worker] progress "+string(rune('a'+index)))
		message := t3276Message(directive)
		message.DeliverPolicy = policy
		manager.offer(t.Context(), message)
		held = append(held, directive)
	}
	kinds, _ := rig.drainEvents()
	if got := t3276Count(kinds, "relay.held"); got != len(policies) {
		t.Fatalf("events=%v, want %d rows held on the working pane", kinds, len(policies))
	}
	return held
}

func t3384HeldRows(t *testing.T, rig *t3276Rig) []relayHeld {
	t.Helper()
	rows, err := rig.node.relayStore().RelayHeldForPane(t.Context(), "w1:pW")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// Acceptance (a): the wait runs out on a working cmd pane whose rows are held
// under the idle and the max_wait=60 policy. Nothing is typed; each row is told
// as working_wait_expired and dropped. Both wakes an expired wait can take are
// covered: the wait reporting working (the incident) and a plain expiry. Since
// hk 3411 a working cmd hold ends only at the relay.cmd_hold_max ceiling, which
// this test sets already passed.
func TestCmdExpiredHoldOfAnyPolicyNotTypedIntoWorkingPane_t3384(t *testing.T) {
	for _, wake := range []string{"working", "expiry"} {
		t.Run(wake, func(t *testing.T) {
			rig := t3276Start(t)
			rig.node.relayControl.cmdHoldMax = time.Nanosecond
			pane := t3384Install(t, "cmd", "working")
			held := t3384Hold(t, rig, "idle", "max_wait=60")
			manager := rig.node.relayBusyManager()

			if wake == "working" {
				manager.releaseWorking(t.Context(), "w1:pW", true)
			} else {
				manager.release(t.Context(), "w1:pW", true)
			}

			for _, pair := range []string{"pane send-text", "pane send-keys", "agent prompt", "agent send-keys"} {
				if got := pane.calls(t, pair); got != 0 {
					t.Fatalf("an expired hold typed into a working cmd pane: %d %q calls, typed=%q", got, pair, pane.typed(t))
				}
			}
			reasons := map[string]map[int64]string{"relay.unconfirmed": {}, "relay.dropped": {}}
			for {
				select {
				case event := <-rig.events:
					var payload struct {
						Reason          string `json:"reason"`
						OriginalEventID int64  `json:"original_event_id"`
					}
					if _, tracked := reasons[event.Kind]; tracked && json.Unmarshal(event.Payload, &payload) == nil {
						reasons[event.Kind][payload.OriginalEventID] = payload.Reason
					}
					if event.Kind == "relay.delivered" || event.Kind == "relay.released" {
						t.Fatalf("an expired hold on a working cmd pane was delivered: %s", event.Payload)
					}
					continue
				default:
				}
				break
			}
			for _, directive := range held {
				for kind, byID := range reasons {
					if byID[directive.EventID] != relayDroppedWorkingExpired {
						t.Errorf("%s for row %d: reason=%q, want %s", kind, directive.EventID, byID[directive.EventID], relayDroppedWorkingExpired)
					}
				}
				if row := rig.row(directive.EventID); row.DeliveredAt != "" {
					t.Errorf("row %d was marked delivered, want it kept for a replay: %+v", directive.EventID, row)
				}
			}
			if rows := t3384HeldRows(t, rig); len(rows) != 0 {
				t.Fatalf("held rows left after the drop: %+v", rows)
			}
		})
	}
}

// Acceptance (b): three rows held for a cmd pane are typed as three injects,
// each with its own return and proof; no batch is typed. A proven row turns
// the pane working, so each idle wake types one row and the pane finishing its
// turn wakes the next.
func TestCmdReleaseTypesOneRowPerInject_t3384(t *testing.T) {
	rig := t3276Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, "idle", "idle", "max_wait=60")
	manager := rig.node.relayBusyManager()

	for turn := 1; turn <= len(held); turn++ {
		pane.setStatus(t, "idle")
		manager.release(t.Context(), "w1:pW", false)
		if got := pane.calls(t, "pane send-text"); got != turn {
			t.Fatalf("after idle wake %d: pane send-text called %d times, want %d (typed=%q)", turn, got, turn, pane.typed(t))
		}
		if got := pane.calls(t, "pane send-keys"); got != turn {
			t.Fatalf("after idle wake %d: pane send-keys return called %d times, want %d", turn, got, turn)
		}
	}
	typed := pane.typed(t)
	if len(typed) != len(held) {
		t.Fatalf("typed %d texts, want one per row: %q", len(typed), typed)
	}
	for index, directive := range held {
		text := typed[index]
		if strings.Contains(text, "[batch") {
			t.Fatalf("a batch was typed into a cmd pane: %q", text)
		}
		nonce := relayNonce(relayHeld{EventID: directive.EventID, Text: directive.Text})
		if !strings.Contains(text, nonce) || !strings.Contains(text, directive.Text) || len(relayNoncesIn(text)) != 1 {
			t.Fatalf("inject %d=%q, want only row %d with its nonce %s", index+1, text, directive.EventID, nonce)
		}
		if row := rig.row(directive.EventID); row.DeliveredAt == "" {
			t.Fatalf("row %d was not proven delivered: %+v", directive.EventID, row)
		}
	}
	kinds, _ := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != len(held) || t3276Count(kinds, "relay.unconfirmed") != 0 || t3276Count(kinds, "relay.batched") != 0 {
		t.Fatalf("events=%v, want %d proven relay.delivered, no relay.unconfirmed and no relay.batched", kinds, len(held))
	}
	if rows := t3384HeldRows(t, rig); len(rows) != 0 {
		t.Fatalf("held rows left after every row was delivered: %+v", rows)
	}
}

// Acceptance (c): a claude pane keeps the batch. Three held rows go out as one
// agent prompt and each member is proven on its own nonce.
func TestClaudeReleaseStillBatches_t3384(t *testing.T) {
	rig := t3276Start(t)
	pane := t3384Install(t, "claude", "working")
	held := t3384Hold(t, rig, "idle", "idle", "max_wait=60")
	pane.setStatus(t, "idle")

	rig.node.relayBusyManager().release(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "agent prompt"); got != 1 {
		t.Fatalf("agent prompt called %d times, want one batch (typed=%q)", got, pane.typed(t))
	}
	typed := pane.typed(t)
	if len(typed) != 1 || !strings.HasPrefix(typed[0], "[batch 3건]") || len(relayNoncesIn(typed[0])) != len(held) {
		t.Fatalf("typed=%q, want one [batch 3건] inject carrying all three nonces", typed)
	}
	kinds, _ := rig.drainEvents()
	if t3276Count(kinds, "relay.batched") != 1 || t3276Count(kinds, "relay.delivered") != len(held) {
		t.Fatalf("events=%v, want one relay.batched and %d relay.delivered", kinds, len(held))
	}
	for _, directive := range held {
		if row := rig.row(directive.EventID); row.DeliveredAt == "" {
			t.Fatalf("row %d was not proven delivered: %+v", directive.EventID, row)
		}
	}
}

// Acceptance (d): the cmd pane goes working after the first of three injects,
// so the second and third are not typed. They stay held, nothing drops them,
// and the release waits again for the pane.
func TestCmdReleaseHoldsTheRestOncePaneTurnsWorking_t3384(t *testing.T) {
	rig := t3276Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, "idle", "idle", "idle")
	manager := rig.node.relayBusyManager()
	rig.waitMu.Lock()
	waitsBefore := len(rig.waits)
	rig.waitMu.Unlock()
	pane.setStatus(t, "idle")

	manager.release(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want only the first row (typed=%q)", got, pane.typed(t))
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("the first row was not proven delivered: %+v", row)
	}
	kinds, _ := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want the first row delivered and nothing dropped", kinds)
	}
	rows := t3384HeldRows(t, rig)
	if len(rows) != 2 || rows[0].EventID != held[1].EventID || rows[1].EventID != held[2].EventID {
		t.Fatalf("held rows=%+v, want the second and third still held", rows)
	}
	manager.mu.Lock()
	inMemory := len(manager.held["w1:pW"])
	manager.mu.Unlock()
	if inMemory != 2 {
		t.Fatalf("%d rows held in memory, want 2 so the next wake finds them", inMemory)
	}
	eventually(t, 2*time.Second, "a wait armed for the rows still held", func() bool {
		rig.waitMu.Lock()
		defer rig.waitMu.Unlock()
		return len(rig.waits) > waitsBefore
	})
}

// A cmd pane that turned working again before a non-expired release read it
// gets nothing typed, and its rows stay held rather than dropped.
func TestCmdReleaseOnWorkingPaneBeforeExpiryKeepsRowsHeld_t3384(t *testing.T) {
	rig := t3276Start(t)
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, "idle", relayDeliverDirective)

	rig.node.relayBusyManager().releaseWorking(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "pane send-text"); got != 0 {
		t.Fatalf("a working cmd pane was typed into before expiry: %q", pane.typed(t))
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want the rows kept held", kinds)
	}
	if rows := t3384HeldRows(t, rig); len(rows) != len(held) {
		t.Fatalf("held rows=%+v, want all %d kept", rows, len(held))
	}
}
