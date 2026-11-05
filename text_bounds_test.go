package panewire

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestUTF8PrefixWithinNeverSplitsARune(t *testing.T) {
	for _, tc := range []struct {
		value string
		limit int
		want  string
	}{
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"가나다", 9, "가나다"},
		{"가나다", 8, "가나"},
		{"가나다", 7, "가나"},
		{"가나다", 6, "가나"},
		{"가", 2, ""},
		{"", 0, ""},
	} {
		if got := utf8PrefixWithin(tc.value, tc.limit); got != tc.want {
			t.Errorf("utf8PrefixWithin(%q, %d)=%q, want %q", tc.value, tc.limit, got, tc.want)
		}
	}
}

func TestTruncationVariantsKeepTheirMarkers(t *testing.T) {
	long := strings.Repeat("가", laneEventTextLimit)
	if got, cut := truncateLaneEventText(long); !cut || !strings.HasSuffix(got, "[truncated]") || len(got) > laneEventTextLimit || !utf8.ValidString(got) {
		t.Fatalf("lane event truncation len=%d cut=%v valid=%v", len(got), cut, utf8.ValidString(got))
	}
	if got := capLaneText(long); !strings.HasSuffix(got, "…") || len(got) > laneEventTextLimit || !utf8.ValidString(got) {
		t.Fatalf("capLaneText len=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if got, cut := truncateHubRelayPayloadText(long, false); !cut || utf8.RuneCountInString(got) != hubRelayPayloadTextLimit {
		t.Fatalf("relay payload runes=%d cut=%v", utf8.RuneCountInString(got), cut)
	}
	short := "짧은 줄"
	if got, cut := truncateLaneEventText(short); cut || got != short {
		t.Fatalf("short lane text changed: %q %v", got, cut)
	}
	if got := capLaneText(short); got != short {
		t.Fatalf("short capLaneText changed: %q", got)
	}
}

func TestCompletedRelayTextKeepsMultibyteLastLineValid(t *testing.T) {
	text := completedRelayText(hubJobEventPayload{Label: "job", Host: "host", ReportPath: "/r.md", ReportLastLine: "a" + strings.Repeat("가", 100)})
	if !utf8.ValidString(text) {
		t.Fatalf("relay text is not valid UTF-8: %q", text)
	}
}

// legacyInboxSummaryCut is the pre-text_bounds inline form; the shared
// helpers must keep its output byte for byte.
func legacyInboxSummaryCut(content string, budget int) string {
	if len([]rune(content)) > budget {
		return string([]rune(content)[:max(0, budget-1)]) + "…"
	}
	return content
}

func TestInboxSummaryBoundsMatchTheLegacyCut(t *testing.T) {
	long := strings.Repeat("보고서 제목 ", 20)
	if got, want := inboxSummaryTitle(long), string([]rune(long)[:60]); got != want {
		t.Fatalf("title=%q, want %q", got, want)
	}
	if got := inboxSummaryTitle("짧음"); got != "짧음" {
		t.Fatalf("short title changed: %q", got)
	}
	for _, budget := range []int{-3, 0, 1, 10, 119, 120, 121, 500} {
		if got, want := capInboxSummaryContent(long, budget), legacyInboxSummaryCut(long, budget); got != want {
			t.Fatalf("budget %d: got %q, want %q", budget, got, want)
		}
	}
	line := inboxSummaryLine([]laneInboxItem{{Kind: "job.completed", Event: hubJobEventPayload{ReportLastLine: long}}}, "doc-1", 80)
	if utf8.RuneCountInString(line) > 80 || !strings.HasSuffix(line, " | hk:doc doc-1") {
		t.Fatalf("summary line=%q runes=%d", line, utf8.RuneCountInString(line))
	}
}
