package panewire

import (
	"strings"
	"testing"
	"time"
)

// quotaMaxWaitDoc is a policy/quota-stop Values section; each extra line is
// appended as a `- ` bullet, so tests can add a max_wait line (hk 4503) or a
// malformed one. The tests reach max_wait only through parseQuotaPolicy and
// the decision output, so they compile on a tree without the field.
func quotaMaxWaitDoc(lines ...string) string {
	doc := "## Values\n- default_expected: 1d (each task may set its own)\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue (resume)\"\n"
	for _, line := range lines {
		doc += "- " + line + "\n"
	}
	return doc + "\n## Other\n"
}

// Reset 13 h away with a 24 h task waits on expected time alone; with
// max_wait: 1h it hands over before that comparison (hk 4503).
func TestQuotaMaxWaitDecisionJobHandsOverBeyondLimit(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	policy, ok := parseQuotaPolicy(quotaMaxWaitDoc("max_wait: 1h"))
	if !ok {
		t.Fatal("policy with max_wait: 1h rejected")
	}
	for _, tc := range []struct {
		name   string
		reset  time.Time
		want   string
		reason string
	}{
		{"13h reset beyond 1h", now.Add(13 * time.Hour), "handover", "reset_beyond_max_wait"},
		{"13min reset within 1h", now.Add(13 * time.Minute), "wait", "reset_within_expected"},
		{"reset exactly at the limit", now.Add(time.Hour), "wait", "reset_within_expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := quotaDecisionJob(policy, true, 3, 24*time.Hour, now, 0, quotaStop{Reset: tc.reset}, now)
			if got != tc.want || reason != tc.reason {
				t.Errorf("got %s %s, want %s %s", got, reason, tc.want, tc.reason)
			}
		})
	}
}

// The non-implementer path (quotaDecision) applies the same limit.
func TestQuotaMaxWaitDecisionHandsOverBeyondLimit(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	policy, ok := parseQuotaPolicy(quotaMaxWaitDoc("max_wait: 1h"))
	if !ok {
		t.Fatal("policy with max_wait: 1h rejected")
	}
	got, reason := quotaDecision(policy, true, 3, 24*time.Hour, 0, quotaStop{Reset: now.Add(13 * time.Hour)}, now)
	if got != "handover" || reason != "reset_beyond_max_wait" {
		t.Errorf("13h reset: got %s %s, want handover reset_beyond_max_wait", got, reason)
	}
	got, reason = quotaDecision(policy, true, 3, 24*time.Hour, 0, quotaStop{Reset: now.Add(13 * time.Minute)}, now)
	if got != "wait" || reason != "reset_within_expected" {
		t.Errorf("13min reset: got %s %s, want wait reset_within_expected", got, reason)
	}
}

// A day-valued limit is read with the same grammar as default_expected.
func TestQuotaMaxWaitDayUnit(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	policy, ok := parseQuotaPolicy(quotaMaxWaitDoc("max_wait: 1d"))
	if !ok {
		t.Fatal("policy with max_wait: 1d rejected")
	}
	if got, reason := quotaDecisionJob(policy, true, 3, 48*time.Hour, now, 0, quotaStop{Reset: now.Add(13 * time.Hour)}, now); got != "wait" || reason != "reset_within_expected" {
		t.Errorf("13h under 1d: got %s %s, want wait reset_within_expected", got, reason)
	}
	if got, reason := quotaDecisionJob(policy, true, 3, 48*time.Hour, now, 0, quotaStop{Reset: now.Add(25 * time.Hour)}, now); got != "handover" || reason != "reset_beyond_max_wait" {
		t.Errorf("25h over 1d: got %s %s, want handover reset_beyond_max_wait", got, reason)
	}
}

// A policy without max_wait keeps today's decision: a 13 h reset waits on a
// 24 h task, and a reset past the expected time still hands over as too late.
func TestQuotaMaxWaitAbsentKeepsToday(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	policy, ok := parseQuotaPolicy(quotaMaxWaitDoc())
	if !ok {
		t.Fatal("policy without max_wait rejected")
	}
	if got, reason := quotaDecisionJob(policy, true, 3, 24*time.Hour, now, 0, quotaStop{Reset: now.Add(13 * time.Hour)}, now); got != "wait" || reason != "reset_within_expected" {
		t.Errorf("13h, no max_wait: got %s %s, want wait reset_within_expected", got, reason)
	}
	if got, _ := quotaDecision(policy, true, 3, 24*time.Hour, 0, quotaStop{Reset: now.Add(13 * time.Hour)}, now); got != "wait" {
		t.Errorf("generic 13h, no max_wait: got %s, want wait", got)
	}
	if got, reason := quotaDecisionJob(policy, true, 3, 24*time.Hour, now, 0, quotaStop{Reset: now.Add(25 * time.Hour)}, now); got != "handover" || reason != "reset_too_late" {
		t.Errorf("25h, no max_wait: got %s %s, want handover reset_too_late", got, reason)
	}
}

// A present max_wait line that is not a positive duration, or appears twice,
// makes the whole policy unavailable: fail closed, like the other values.
func TestQuotaMaxWaitMalformedFailsClosed(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, lines := range [][]string{
		{"max_wait: soon"},
		{"max_wait: 0h"},
		{"max_wait:"},
		{"max_wait: 1h30x"},
		{"max_wait: 1h", "max_wait: 2h"},
	} {
		doc := quotaMaxWaitDoc(lines...)
		if _, ok := parseQuotaPolicy(doc); ok {
			t.Errorf("%q accepted", strings.Join(lines, "; "))
			continue
		}
		var zero quotaPolicy
		if got, reason := quotaDecisionJob(zero, false, 3, 24*time.Hour, now, 0, quotaStop{Reset: now.Add(13 * time.Minute)}, now); got != "handover" || reason != "policy_unavailable" {
			t.Errorf("%q: got %s %s, want handover policy_unavailable", strings.Join(lines, "; "), got, reason)
		}
	}
}
