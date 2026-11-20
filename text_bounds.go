package panewire

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/textbound"
)

// Every bounded-text rule lives here so the variants stay side by side. Each
// one is part of a wire, record, or display contract, so they deliberately
// differ in unit (bytes or runes) and in marker; pick the one whose contract
// the new caller shares instead of writing another, and never slice a string
// with s[:n] where n can land inside a UTF-8 sequence.

const hubRelayPayloadTextLimit = 240

// paneMetadataValueLimit bounds one pane.report_metadata token value in bytes.
// herdr's schema caps token keys but not values; this cut is panewire's own
// contract with herdr's pane display, so a value is never split inside a rune.
const paneMetadataValueLimit = 128

// truncatePaneMetadataValue is the only cut applied to a pane metadata value
// before it is compared or sent (herdr_pane_metadata.go).
func truncatePaneMetadataValue(value string) string {
	return utf8PrefixWithin(value, paneMetadataValueLimit)
}

// utf8PrefixWithin and utf8SuffixWithin are the root package's names for the
// shared cuts in internal/textbound.
func utf8PrefixWithin(value string, limit int) string { return textbound.PrefixWithin(value, limit) }

func utf8SuffixWithin(value string, limit int) string { return textbound.SuffixWithin(value, limit) }

// stallExaminationExcerpt bounds persisted evidence and retains its last lines.
func stallExaminationExcerpt(value string) string { return utf8SuffixWithin(value, 4096) }

// stallNotificationText keeps the job, pane, and end of the visible screen.
func stallNotificationText(job, pane, screen string) string {
	prefix := "[stall] job " + job + " pane " + pane + ": "
	screen = strings.Map(func(r rune) rune {
		if r <= 0x1f || (r >= 0x7f && r <= 0x9f) {
			return ' '
		}
		return r
	}, screen)
	return prefix + utf8SuffixWithin(screen, laneEventTextLimit-len(prefix))
}

// truncateRelayText is the plain byte bound for relay note text: no marker.
func truncateRelayText(value string, limit int) string {
	return utf8PrefixWithin(value, limit)
}

// laneEventTextCeiling is the hard bound on lane.event text in every record and
// frame (hk 4835). Text above it is refused with a message naming the bound.
// Text between laneEventTextLimit and this bound is kept whole in the lane inbox
// and typed as a head plus a pointer (lane_event_long.go,
// docs/lane-event-long.md). It is 16 KiB, not larger, because every record and
// frame that carries it must fit laneEventFrameMax.
const laneEventTextCeiling = 16 << 10

// laneEventFrameMax bounds one encoded lane.event record or websocket frame.
// encoding/json writes <, > and & as six-byte escapes, so the ceiling of text
// can encode to six times its size, plus the envelope. The hub's websocket read
// limit and the node's event-file cap both take this value.
const laneEventFrameMax = 6*laneEventTextCeiling + 8<<10

// truncateLaneEventText is the typed-line cut: the bound a hub without
// lane_event_long accepts for a non-sink event. A node applies it only to the
// wire form for such a hub (laneEventPayloadForOldHub); the event file keeps the
// whole text (docs/lane-event-long.md).
func truncateLaneEventText(value string) (string, bool) {
	return truncateLaneEventTextAtLimit(value, laneEventTextLimit)
}

// truncateLaneEventTextAtLimit cuts to limit bytes including the
// "[truncated]" marker and reports whether anything was cut.
func truncateLaneEventTextAtLimit(value string, limit int) (string, bool) {
	if len(value) <= limit {
		return value, false
	}
	const marker = "[truncated]"
	return utf8PrefixWithin(value, limit-len(marker)) + marker, true
}

// capLaneText is the hk assistantLaneText re-cap shape: text over the
// lane-event byte limit is cut at a UTF-8 boundary so the result plus the
// ellipsis marker fits, then the "…" is appended — and only ever appended
// when something was actually cut.
func capLaneText(text string) string {
	if len(text) <= laneEventTextLimit {
		return text
	}
	return utf8PrefixWithin(text, laneEventTextLimit-len("…")) + "…"
}

// truncateHubRelayPayloadText applies the character (rather than byte) bound
// used by the relay record contract. Newlines in a question are display text,
// not a record separator, so callers may opt into normalization for it.
func truncateHubRelayPayloadText(value string, normalizeNewlines bool) (string, bool) {
	if normalizeNewlines {
		value = strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(value)
	}
	if utf8.RuneCountInString(value) <= hubRelayPayloadTextLimit {
		return value, false
	}
	return textbound.RunePrefix(value, hubRelayPayloadTextLimit), true
}

// hubPlacementReasonLimit is the byte bound hk places on a placement record's
// reason and on each candidate reason: hk accepts only printable text of at
// most this many bytes and refuses anything else with 400 invalid_context.
const hubPlacementReasonLimit = 256

// hubPlacementReasonText is the reason contract for a placement record
// (hk placement record, hub_placement_record.go). A spawn outcome's reason can
// be a whole engine message with newlines and control bytes, so control runes
// become spaces, invalid UTF-8 is replaced, and the text is cut on a rune
// boundary. The leading code (the stage or spawn_failed prefix) survives the cut.
func hubPlacementReasonText(reason string) string {
	reason = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(reason, "?"))
	return utf8PrefixWithin(strings.TrimSpace(reason), hubPlacementReasonLimit)
}

const inboxSummaryTitleRunes = 60

// inboxSummaryTitle bounds one report title in the desk inbox summary line.
func inboxSummaryTitle(text string) string {
	return textbound.RunePrefix(text, inboxSummaryTitleRunes)
}

// capInboxSummaryContent fits the inbox summary content into budget runes,
// ending a cut line with "…" (which counts toward the budget).
func capInboxSummaryContent(content string, budget int) string {
	if utf8.RuneCountInString(content) <= budget {
		return content
	}
	return textbound.RunePrefix(content, max(0, budget-1)) + "…"
}

// hubPaneIDLimit is the byte bound the hub keeps a job's pane under
// (normalizeHubActiveJobMetadata) and that every relay pane field uses.
const hubPaneIDLimit = 128

// boundLaneEventPaneID is the lane.event wire's sender pane: the value whole,
// or "" when it is over hubPaneIDLimit or holds any control byte. A pane id is
// an identity, so a cut one would name some other pane; no pane is the
// fail-open answer the hub already gives an older node.
func boundLaneEventPaneID(pane string) string {
	if utf8PrefixWithin(pane, hubPaneIDLimit) != pane || hasControlByte(pane) {
		return ""
	}
	return pane
}

// hasControlByte reports any ASCII control byte (< 0x20, or DEL). Git's
// check-ref-format refuses every one, and none belongs in an origin or glob.
func hasControlByte(s string) bool {
	for i := range len(s) {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}
