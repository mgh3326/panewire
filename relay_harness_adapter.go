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
// found, how a message queued mid-turn shows, whether it takes input while
// working, and which evidence proves delivery. A harness without an entry gets
// no injection proof at all.
type relayHarnessAdapter struct {
	// composerEdge and composerGlyph locate the live composer between two
	// divider lines; a nil edge means the harness draws no locatable composer.
	composerEdge  func(line string) bool
	composerGlyph string
	// queued reports the harness's "accepted into the submit-later queue"
	// evidence on a visible read; nil means the harness shows none.
	queued func(screen string) bool
	// midTurn: the harness accepts input while a turn runs (herdr agent prompt
	// reaches it mid-work), so a working pane is injected at once instead of
	// held until idle. A blocked pane is never injected mid-turn: its prompt
	// may be an approval question that the text would answer.
	midTurn bool
	proof   relayProof
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
// delivery on. The grok and omp entries have no captured screen to read a
// composer or queue banner from, so they use the one rule that needs neither
// (relayProofEchoAndWorking) and are never injected mid-turn; a live capture
// would replace that rule with a transcript one.
var relayHarnessAdapters = map[string]relayHarnessAdapter{
	"claude": {composerEdge: isClaudeDividerLine, composerGlyph: "❯", queued: relayQueuedClaudeCodex, proof: relayProofTranscriptEcho, observe: true},
	"codex":  {queued: relayQueuedClaudeCodex, midTurn: true, proof: relayProofTranscriptEcho, observe: true},
	"devin":  {composerEdge: isDevinDividerLine, composerGlyph: "❭", queued: devinQueued, proof: relayProofTranscriptEcho, maxAttempts: relayDevinMaxInjectAttempts},
	"grok":   {proof: relayProofEchoAndWorking, observe: true},
	"omp":    {proof: relayProofEchoAndWorking, observe: true},
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
