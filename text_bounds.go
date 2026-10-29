package panewire

import (
	"strings"
	"unicode/utf8"
)

// Every bounded-text rule lives here so the variants stay side by side. Each
// one is part of a wire, record, or display contract, so they deliberately
// differ in unit (bytes or runes) and in marker; pick the one whose contract
// the new caller shares instead of writing another, and never slice a string
// with s[:n] where n can land inside a UTF-8 sequence.

const hubRelayPayloadTextLimit = 240

// utf8PrefixWithin returns value when it fits in limit bytes, otherwise the
// longest prefix of at most limit bytes that is valid UTF-8.
func utf8PrefixWithin(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

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
	return string([]rune(value)[:hubRelayPayloadTextLimit]), true
}
