package panewire

// hk 3330 review round 1 (PR #286): a cmd pane has two writes -- pane send-text
// types, pane send-keys return submits -- and herdr reports neither back. These
// tests pin the two ways a retry or a replay could paste twice:
//
//   - a send-text failure may have typed part of the text, so it is never
//     retried (finding 1);
//   - the relay reads cmd's composer like claude's, so a failed return followed
//     by a replay finds its own text there and submits it once, and a draft
//     already in the composer is left alone (finding 2).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// t3330Composer is a stateful cmd pane. send-text appends to the composer
// buffer, return submits the buffer into the transcript and starts a turn, and
// a flag file makes the next send-text type half the text and fail, or the next
// return fail without submitting.
type t3330Composer struct {
	dir string
}

func t3330InstallComposer(t *testing.T, draft string) *t3330Composer {
	t.Helper()
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("buf", draft)
	write("status", "idle")
	write("submitted", "")
	script := `#!/bin/sh
d='` + dir + `'
echo "$1 $2" >> "$d/calls"
case "$1 $2" in
"agent get")
  printf '{"result":{"agent":{"agent":"cmd","agent_status":"%s","name":"` + t3276Worker + `","pane_id":"w1:pW"}}}\n' "$(cat "$d/status")" ;;
"agent read"|"pane read")
  src=visible; prev=
  for a in "$@"; do [ "$prev" = "--source" ] && src=$a; prev=$a; done
  fmt=plain; [ "$1" = pane ] && fmt=ansi
  buf=$(cat "$d/buf")
  # echo lag: right after a return the first visible read still draws the text
  # the keypress just submitted.
  if [ "$src" = visible ] && [ -f "$d/stale" ]; then buf=$(cat "$d/stale"); rm "$d/stale"; fi
  { cat "$d/submitted"
    echo '` + t3330CmdDivider + `'
    if [ -z "$buf" ] && [ -f "$d/suggestion" ]; then
      # an idle composer holds claude's dim prompt suggestion; only the ANSI
      # read can tell it from a draft.
      if [ $fmt = ansi ]; then printf '❯ \033[2m%s\033[0m\n' "$(cat "$d/suggestion")"; else echo "❯ $(cat "$d/suggestion")"; fi
    else
      echo "❯ $buf"
    fi
    echo '` + t3330CmdDivider + `'; } ;;
"agent prompt"|"agent send-keys")
  echo '{"error":{"code":"agent_not_ready"}}' >&2
  exit 1 ;;
"pane send-text")
  if [ -f "$d/fail_text" ]; then
    rm "$d/fail_text"
    half=$(printf '%s' "$4" | cut -c1-12)
    printf '%s%s' "$(cat "$d/buf")" "$half" > "$d/buf"
    exit 1
  fi
  printf '%s%s' "$(cat "$d/buf")" "$4" > "$d/buf"
  # a draft the operator types between send-text and return
  if [ -f "$d/draft_after_text" ]; then printf '%s operator-draft' "$(cat "$d/buf")" > "$d/buf"; fi ;;
"pane send-keys")
  [ "$4" = "return" ] || exit 0
  if [ -f "$d/fail_return" ]; then rm "$d/fail_return"; exit 1; fi
  echo "❯ $(cat "$d/buf")" >> "$d/submitted"
  if [ -f "$d/lag_return" ]; then cp "$d/buf" "$d/stale"; fi
  : > "$d/buf"
  echo working > "$d/status" ;;
esac
`
	installFakeHerdr(t, dir, script)
	return &t3330Composer{dir: dir}
}

func (pane *t3330Composer) flag(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(pane.dir, name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (pane *t3330Composer) read(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, name))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(body)
}

func (pane *t3330Composer) calls(t *testing.T, pair string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(pane.read(t, "calls")), "\n") {
		if line == pair {
			n++
		}
	}
	return n
}

// submittedCopies counts how many submitted turns carry text: one copy is one
// delivery, two is the double-submit the review finding describes.
func (pane *t3330Composer) submittedCopies(t *testing.T, text string) int {
	t.Helper()
	return strings.Count(pane.read(t, "submitted"), text)
}

func t3330RelayText() string {
	held := relayHeld{EventID: 7, Text: "fix the failing check"}
	return relayNonce(held) + " " + held.Text
}

// Finding 1 (comment 4498389800): herdr may have typed part of the text before
// pane send-text failed, and cmd has no composer read that could see the
// residue before a re-type. The inject is maybe-in-pane, so the immediate
// retry never types again.
func TestCmdSendTextFailureIsNeverRetried_t3330(t *testing.T) {
	t.Run("verdict", func(t *testing.T) {
		pane := t3330InstallComposer(t, "")
		pane.flag(t, "fail_text")

		result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3330RelayText(), nil)

		if result.Outcome != relayInjectMaybeInPane || result.Evidence != "send_text_failed" {
			t.Fatalf("result=%+v, want maybe-in-pane with send_text_failed", result)
		}
		if relayRetriesOnce(result) {
			t.Fatal("a send-text failure is retried once: the re-type lands on top of the partial text")
		}
	})
	t.Run("relay", func(t *testing.T) {
		rig := t3276Start(t)
		directive := rig.send("ev-cmd-sendfail", "findings", "[mergewake] findings for the worker")
		pane := t3330InstallComposer(t, "")
		pane.flag(t, "fail_text")

		rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

		if got := pane.calls(t, "pane send-text"); got != 1 {
			t.Fatalf("pane send-text called %d times, want 1: a failed send is never typed again", got)
		}
		if got := pane.calls(t, "pane send-keys"); got != 0 {
			t.Fatalf("pane send-keys called %d times after a failed send-text, want 0", got)
		}
		kinds, payloads := rig.drainEvents()
		if t3276Count(kinds, "relay.unconfirmed") != 1 || !strings.Contains(payloads["relay.unconfirmed"].Reason, "maybe_in_pane send_text_failed") {
			t.Fatalf("events=%v reason=%q, want one relay.unconfirmed naming maybe_in_pane send_text_failed", kinds, payloads["relay.unconfirmed"].Reason)
		}
	})
}

// Finding 2 (comment 4498390147): the first return fails, so the text sits typed
// in the composer. The replay reads the composer, finds exactly its own text,
// and presses the one return -- it does not type a second copy.
func TestCmdFailedReturnThenReplaySubmitsOneCopy_t3330(t *testing.T) {
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "fail_return")
	text := t3330RelayText()

	first := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)
	if first.Outcome != relayInjectMaybeInPane || first.Evidence != "return_failed" {
		t.Fatalf("first inject=%+v, want maybe-in-pane with return_failed", first)
	}
	if got := pane.submittedCopies(t, text); got != 0 {
		t.Fatalf("a failed return submitted %d copies, want 0", got)
	}

	replay := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)

	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times across the replay, want 1: the replay must not type again", got)
	}
	if got := pane.submittedCopies(t, text); got != 1 {
		t.Fatalf("submitted copies=%d, want exactly 1 (replay=%+v)", got, replay)
	}
	if replay.Outcome != relayInjectDelivered {
		t.Fatalf("replay=%+v, want delivered on the echo once the one return went through", replay)
	}
}

// Finding 2, second half: a draft already in cmd's composer would be submitted
// with the relay text appended. The relay withholds and types nothing.
func TestCmdDraftInComposerIsWithheldNotTyped_t3330(t *testing.T) {
	const draft = "half-written draft the operator is still typing"
	t.Run("verdict", func(t *testing.T) {
		pane := t3330InstallComposer(t, draft)

		result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3330RelayText(), nil)

		if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:composer_pending" {
			t.Fatalf("result=%+v, want maybe-in-pane with presend:composer_pending", result)
		}
		if relayRetriesOnce(result) {
			t.Fatal("a withheld draft is retried: a second look would not change the composer")
		}
		if got := pane.calls(t, "pane send-text") + pane.calls(t, "pane send-keys"); got != 0 {
			t.Fatalf("the pane was written to %d times, want 0", got)
		}
		if got := pane.read(t, "buf"); got != draft {
			t.Fatalf("composer=%q, want the draft untouched", got)
		}
	})
	t.Run("relay", func(t *testing.T) {
		rig := t3276Start(t)
		directive := rig.send("ev-cmd-draft", "findings", "[mergewake] findings for the worker")
		pane := t3330InstallComposer(t, draft)

		rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

		if got := pane.calls(t, "pane send-text") + pane.calls(t, "pane send-keys"); got != 0 {
			t.Fatalf("a draft in the composer was written to %d times, want 0", got)
		}
		if got := pane.read(t, "submitted"); got != "" {
			t.Fatalf("submitted=%q, want nothing", got)
		}
		kinds, _ := rig.drainEvents()
		if t3276Count(kinds, "relay.unconfirmed") != 1 || t3276Count(kinds, "relay.delivered") != 0 {
			t.Fatalf("events=%v, want one relay.unconfirmed and no relay.delivered", kinds)
		}
	})
}

// Round 3, finding A (comment 4498495723): an idle cmd pane draws claude's dim
// prompt suggestion in its empty composer. The plain read cannot tell it from a
// draft, so the presend reads the ANSI screen and applies the reap's dim rule:
// the suggestion is not a draft, and the event is typed and submitted once.
func TestCmdDimSuggestionInEmptyComposerIsTypedOnce_t3330(t *testing.T) {
	pane := t3330InstallComposer(t, "")
	if err := os.WriteFile(filepath.Join(pane.dir, "suggestion"), []byte("Check PR 76 status"), 0o600); err != nil {
		t.Fatal(err)
	}
	text := t3330RelayText()

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)

	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered: a dim suggestion is not a pending draft", result)
	}
	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want 1", got)
	}
	if got := pane.calls(t, "pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys called %d times, want exactly 1 return", got)
	}
	if got := pane.submittedCopies(t, text); got != 1 {
		t.Fatalf("submitted copies=%d, want exactly 1", got)
	}
}

// Round 3, finding B (comment 4498495738): the return after send-text is gated
// by the composer read after the text landed. A draft that appears between
// send-text and return would be submitted with the relay text, so no return is
// pressed and the verdict is maybe-in-pane.
func TestCmdDraftBetweenSendTextAndReturnWithholdsReturn_t3330(t *testing.T) {
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "draft_after_text")

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3330RelayText(), nil)

	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "return_withheld:composer_foreign" {
		t.Fatalf("result=%+v, want maybe-in-pane with return_withheld:composer_foreign", result)
	}
	if relayRetriesOnce(result) {
		t.Fatal("a withheld return is retried: the retry would type over the text already in the composer")
	}
	if got := pane.calls(t, "pane send-keys"); got != 0 {
		t.Fatalf("pane send-keys called %d times, want 0: the draft would have been submitted", got)
	}
	if got := pane.read(t, "submitted"); got != "" {
		t.Fatalf("submitted=%q, want nothing", got)
	}
}

// Round 3, finding C (comment 4498495743): one return per inject. The composer
// still draws the text right after the return (echo lag); verify must not read
// that as residue to submit again. It observes until the echo lands.
func TestCmdEchoLagAfterReturnPressesOneReturn_t3330(t *testing.T) {
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "lag_return")
	text := t3330RelayText()

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)

	if got := pane.calls(t, "pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys called %d times, want exactly 1 return (result=%+v)", got, result)
	}
	if got := pane.submittedCopies(t, text); got != 1 {
		t.Fatalf("submitted copies=%d, want exactly 1", got)
	}
	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered once the lagging echo settled", result)
	}
}
