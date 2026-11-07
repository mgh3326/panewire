package panewire

import (
	"strings"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/textbound"
)

// Every bounded-text rule lives here so the variants stay side by side. Each
// one is part of a wire, record, or display contract, so they deliberately
// differ in unit (bytes or runes) and in marker; pick the one whose contract
// the new caller shares instead of writing another, and never slice a string
// with s[:n] where n can land inside a UTF-8 sequence.

const hubRelayPayloadTextLimit = 240

// utf8PrefixWithin and utf8SuffixWithin are the root package's names for the
// shared cuts in internal/textbound.
func utf8PrefixWithin(value string, limit int) string { return textbound.PrefixWithin(value, limit) }

func utf8SuffixWithin(value string, limit int) string { return textbound.SuffixWithin(value, limit) }

// truncateRelayText is the plain byte bound for relay note text: no marker.
func truncateRelayText(value string, limit int) string {
	return utf8PrefixWithin(value, limit)
}

// truncateLaneEventText is the only truncation point for lane.event text.
// It runs on the producer node before the file, outbox key, and wire payload
// are made; the hub only validates this final form so acknowledgements cannot
// name a different record after a restart.
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
// or "" when it is over hubPaneIDLimit or holds a control byte. A pane id is
// an identity, so a cut one would name some other pane; no pane is the
// fail-open answer the hub already gives an older node.
func boundLaneEventPaneID(pane string) string {
	if utf8PrefixWithin(pane, hubPaneIDLimit) != pane || strings.ContainsAny(pane, "\x00\r\n\t") {
		return ""
	}
	return pane
}
