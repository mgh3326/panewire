package panewire

// Contract: docs/relay-harness-delivery.md (workingUnprovable, stale working).
//
// hk 3517: Command Code reports its own lifecycle to herdr, and sometimes never
// sends the report that ends a turn, so herdr keeps `working` for hours after
// the final answer. A held row for such a pane waited for idle until the
// relay.cmd_hold_max ceiling. The node proves the pane stuck from herdr's own
// reads and then takes the idle path. It only reads: nothing is written to
// herdr, and a real working turn redraws (spinner, timers), so a visible screen
// that stays byte-identical for relay.cmd_stale_after is the whole proof.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"time"
)

const (
	// defaultRelayCmdStaleAfter is the relay.cmd_stale_after default.
	defaultRelayCmdStaleAfter = 10 * time.Minute
	// relayStaleWorkingEvidence prefixes the delivery evidence of a send into a
	// pane proven stale-working, so the hub log shows which rule delivered it.
	relayStaleWorkingEvidence = "stale_working:"
)

// relayCmdStaleAfterFromEnv reads the relay.cmd_stale_after setting from
// PANEWIRE_RELAY_CMD_STALE_AFTER. A missing or invalid value is the default.
func relayCmdStaleAfterFromEnv() time.Duration {
	if value, err := time.ParseDuration(os.Getenv("PANEWIRE_RELAY_CMD_STALE_AFTER")); err == nil && value > 0 {
		return value
	}
	return defaultRelayCmdStaleAfter
}

func (client *HubClient) relayCmdStaleAfter() time.Duration {
	if client.relayControl.cmdStaleAfter > 0 {
		return client.relayControl.cmdStaleAfter
	}
	return defaultRelayCmdStaleAfter
}

// relayScreenSample is the screen a working cmd pane showed at the re-check
// that first saw it: its digest, the occupant it was read from, and when. It is
// replaced whenever a later read differs or the occupant changes, so the time
// since it is how long the screen has been frozen.
type relayScreenSample struct {
	lease  string
	digest string
	since  time.Time
	warned bool
}

func relayScreenDigest(ansi string) string {
	sum := sha256.Sum256([]byte(ansi))
	return hex.EncodeToString(sum[:])
}

// forgetCmdScreen drops the pane's sample. A pane that is no longer working has
// no frozen window to measure.
func (manager *relayBusyManager) forgetCmdScreen(pane string) {
	manager.mu.Lock()
	delete(manager.cmdScreens, pane)
	manager.mu.Unlock()
}

// cmdStaleWorkingDigest is the re-check of a held row whose cmd pane herdr
// reports working. It returns the digest of the screen when the pane is proven
// stale-working, and "" otherwise. All of these must hold: the visible ANSI
// screen is byte-identical to the one remembered for the same occupant at least
// relay.cmd_stale_after ago, and the composer reads empty. A failed read is not
// evidence: it changes nothing, and the row keeps its hold.
func (manager *relayBusyManager) cmdStaleWorkingDigest(parent context.Context, item relayHeld, lease string) string {
	session := manager.client.herdrSessionFor(item.JobID, item.Pane)
	readContext, cancel := context.WithTimeout(withRelaySession(parent, session), manager.client.relayInjectTimeout())
	ansi, err := relayReadPaneANSI(readContext, item.Pane)
	cancel()
	if err != nil || ansi == "" {
		return ""
	}
	digest := relayScreenDigest(ansi)
	now := manager.client.nowUTC()

	manager.mu.Lock()
	defer manager.mu.Unlock()
	sample, known := manager.cmdScreens[item.Pane]
	if !known || sample.lease != lease || sample.digest != digest {
		manager.cmdScreens[item.Pane] = relayScreenSample{lease: lease, digest: digest, since: now}
		return ""
	}
	frozen := now.Sub(sample.since)
	if frozen < manager.client.relayCmdStaleAfter() || relayAnsiComposerState(ansi) != "composer_empty" {
		return ""
	}
	if !sample.warned {
		sample.warned = true
		manager.cmdScreens[item.Pane] = sample
		slog.Warn("relay cmd pane stale working", "pane", item.Pane, "lane", item.Lane, "event_id", item.EventID, "frozen_seconds", int64(frozen.Seconds()))
	}
	return digest
}

// staleScreenKey carries the digest of the screen a stale-working send started
// from, down to the inject's proof, which cannot see the manager's memory.
type staleScreenKey struct{}

func withRelayStaleScreen(ctx context.Context, digest string) context.Context {
	return context.WithValue(ctx, staleScreenKey{}, digest)
}

func relayStaleScreenFrom(ctx context.Context) (string, bool) {
	digest, ok := ctx.Value(staleScreenKey{}).(string)
	return digest, ok && digest != ""
}

// withStaleScreen adds the stale-send proof to reads taken after the return. The
// pane never shows the idle-to-working transition relayProofEchoAndWorking
// needs, so a send into a stale-working pane counts as submitted only when the
// visible screen now differs from the stale one. An unreadable screen proves
// nothing and leaves screenMoved false.
func (reads relayPaneReads) withStaleScreen(ctx context.Context, pane string) relayPaneReads {
	digest, stale := relayStaleScreenFrom(ctx)
	if !stale {
		return reads
	}
	reads.staleSend = true
	if ansi, err := relayReadPaneANSI(ctx, pane); err == nil && ansi != "" && relayScreenDigest(ansi) != digest {
		reads.screenMoved = true
	}
	return reads
}
