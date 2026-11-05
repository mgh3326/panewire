// Package textbound holds the UTF-8-safe byte cuts every bounded-text rule
// is built on, for the root package and every internal package. Never slice
// a string with s[:n] where n can land inside a UTF-8 sequence.
package textbound

import (
	"strings"
	"unicode/utf8"
)

// PrefixWithin returns value when it fits in limit bytes, otherwise the
// longest prefix of at most limit bytes that is valid UTF-8.
func PrefixWithin(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

// SuffixWithin returns the longest valid UTF-8 suffix of value within limit
// bytes: the tail of a process's output, where the error usually is. A
// negative limit yields "", and invalid bytes are dropped so the result is
// always valid UTF-8.
func SuffixWithin(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) > limit {
		value = value[runeStartFrom(value, len(value)-limit):]
	}
	return strings.ToValidUTF8(value, "")
}

// RunePrefix returns value when it has at most limit runes, otherwise its
// first limit runes (a negative limit yields "").
func RunePrefix(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	return string([]rune(value)[:max(0, limit)])
}

func runeStartFrom(value string, start int) int {
	for start < len(value) && !utf8.RuneStart(value[start]) {
		start++
	}
	return start
}
