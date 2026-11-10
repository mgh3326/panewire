package panewire

// Contract and per-harness table: docs/relay-harness-delivery.md.

import (
	"strings"
	"time"
)

// relayProof names the on-screen evidence that earns relay.delivered for one
// harness. Every proof starts from the per-row nonce (#687); what differs is
// how the harness lets the relay tell a submitted prompt from text that is
// only sitting in the composer.
type relayProof int

const (
	// relayProofNone is a harness with no rule: nothing it shows can prove the
	// text was taken, so the relay reports unconfirmed (hk 3276).
	relayProofNone relayProof = iota
	// relayProofTranscriptEcho: the nonce is an exact token in the transcript
	// above the located composer, or the harness's queue banner newly appeared
	// beside it. Claude, codex and devin draw enough chrome for this.
	relayProofTranscriptEcho
	// relayProofEchoAndWorking is for a harness whose composer cannot be
	// located. A pending paste would also show the nonce, so the nonce alone
	// proves nothing; the proof is the nonce on screen plus herdr reporting the
	// pane went from idle/done to working after the send, which only a
	// submitted prompt causes.
	relayProofEchoAndWorking
)

// relayHarnessAdapter is one harness's entry in the relay: how its composer is
// found, how a message queued mid-turn shows, and which evidence proves
// delivery. A harness without an entry gets
// no injection proof at all.
type relayHarnessAdapter struct {
	// composerEdge and composerGlyph locate the live composer between two
	// divider lines; a nil edge means the harness draws no locatable composer.
	composerEdge  func(line string) bool
	composerGlyph string
	// queued reports the harness's "accepted into the submit-later queue"
	// evidence on a visible read; nil means the harness shows none.
	queued func(screen string) bool
	proof  relayProof
	// paneTransport types through the pane-level calls (pane send-text, then
	// pane send-keys return) instead of herdr's agent prompt. herdr refuses
	// the agent-level calls for a cmd pane with agent_not_ready, and the spawn
	// path already delivers a cmd brief this way. The relay and `panewire
	// prompt` both read this one field, so the two cannot disagree about how a
	// pane is reached.
	paneTransport bool
	// workingUnprovable marks a harness that cannot prove a directive typed
	// into a working pane: its proof needs the composer of an idle or done
	// pane, so a send into a working one stays unproven and the retry-once rule
	// would type the directive again (hk 3370). The directive policy holds such
	// a pane while it is working and types when it turns idle or done.
	workingUnprovable bool
	// observe: a read that shows no proof yet is re-read for the observation
	// window before the inject is called unconfirmed, because the echo renders
	// late on a busy pane. Observation never types or presses anything.
	observe bool
	// maxAttempts bounds re-injection after herdr rejected a send; zero means
	// relayMaxInjectAttempts.
	maxAttempts int
}

// relayObserveWindow is how long an adapter with observe set keeps reading a
// pane whose inject is not yet proven.
var relayObserveWindow = 8 * time.Second

func relayQueuedClaudeCodex(screen string) bool {
	return strings.Contains(screen, "Press up to edit queued messages") ||
		strings.Contains(screen, "Messages to be submitted after next tool call")
}

// relayHarnessAdapters is the closed table of harnesses the relay can prove a
// delivery on. The grok, omp and cmd entries have no captured queue banner, so
// they use the one rule that needs none (relayProofEchoAndWorking). That rule needs the pane idle or done before the
// send, so a message injected into a working grok, omp or cmd pane is typed but
// stays unconfirmed until a live capture gives a transcript rule.
//
// cmd is also the one pane-level harness: herdr answers agent get and agent
// read for it but refuses agent prompt and agent send-keys with
// agent_not_ready, so its text goes through pane send-text and its submit
// through pane send-keys (hk 3330). cmd draws claude's two-divider composer
// with the same ❯ prompt, so it takes claude's composer rule: a draft or its
// own typed text is found there, and the relay never types over a draft or
// submits a text it cannot see.
var relayHarnessAdapters = map[string]relayHarnessAdapter{
	"claude": {composerEdge: isClaudeDividerLine, composerGlyph: "❯", queued: relayQueuedClaudeCodex, proof: relayProofTranscriptEcho, observe: true},
	"codex":  {queued: relayQueuedClaudeCodex, proof: relayProofTranscriptEcho, observe: true},
	"devin":  {composerEdge: isDevinDividerLine, composerGlyph: "❭", queued: devinQueued, proof: relayProofTranscriptEcho, maxAttempts: relayDevinMaxInjectAttempts},
	"grok":   {proof: relayProofEchoAndWorking, observe: true},
	"omp":    {proof: relayProofEchoAndWorking, observe: true},
	"cmd":    {composerEdge: isClaudeDividerLine, composerGlyph: "❯", proof: relayProofEchoAndWorking, observe: true, paneTransport: true, workingUnprovable: true},
}

// harnessLoginScreens is the closed table of the screen each harness shows when
// it has no login: every line of a row must appear on the screen. The node
// reads it on the capture of a spawn that ended rc=76 (quota_login_probe.go).
// A harness has a row only from a recorded capture of its login screen:
// claude and codex have none yet, so a screen of theirs is never read as a
// login screen. agy: jobs/at-3816-retro-read-views/spawn-failure-*.txt on
// mac-dev, 2026-11-10.
var harnessLoginScreens = map[string][]string{
	"agy": {"Open the URL below in your browser:", "After authenticating, copy the code displayed in the browser and paste it below:"},
}

// harnessLoginScreen reports whether screen is kind's login screen.
func harnessLoginScreen(kind, screen string) bool {
	lines := harnessLoginScreens[kind]
	if len(lines) == 0 {
		return false
	}
	for _, line := range lines {
		if !strings.Contains(screen, line) {
			return false
		}
	}
	return true
}

// relayAdapterFor looks a harness name up case-insensitively. An empty or
// unknown name has no adapter.
func relayAdapterFor(harness string) (relayHarnessAdapter, bool) {
	adapter, ok := relayHarnessAdapters[strings.ToLower(strings.TrimSpace(harness))]
	return adapter, ok
}

func (adapter relayHarnessAdapter) injectAttempts() int {
	if adapter.maxAttempts > 0 {
		return adapter.maxAttempts
	}
	return relayMaxInjectAttempts
}

// relayHarnessWorkingUnprovable reports whether a directive for harness is held
// while its pane is working (hk 3370).
func relayHarnessWorkingUnprovable(harness string) bool {
	adapter, ok := relayAdapterFor(harness)
	return ok && adapter.workingUnprovable
}

// relayPaneWorkingUnprovable reports whether a pane in status, occupied by
// harness, must not be typed into: it is working and its harness cannot prove
// text typed into a working pane (hk 3370, hk 3384).
func relayPaneWorkingUnprovable(status, harness string) bool {
	return status == "working" && relayHarnessWorkingUnprovable(harness)
}

// harnessPaneTransport reports whether harness is typed at the pane level
// rather than through herdr's agent prompt. Both the relay and `panewire
// prompt` ask this, so the two can never disagree about how a pane is
// reached.
func harnessPaneTransport(harness string) bool {
	adapter, ok := relayAdapterFor(harness)
	return ok && adapter.paneTransport
}
