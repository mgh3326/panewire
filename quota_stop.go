package panewire

import (
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// quotaStop is the shared quota-stop evidence; see internal/quotastop.
type quotaStop = quotastop.Stop

func classifyQuotaStop(screen string, now time.Time, local *time.Location) (quotaStop, bool) {
	return quotastop.Classify(screen, now, local)
}

// quotaPolicy is accepted only when all four values are present and valid.
// A missing or malformed policy never authorizes a wait or input.
type quotaPolicy struct {
	Expected   time.Duration
	Urgent     map[int]bool
	MaxWaits   int
	ResumeText string
}

func quotaDuration(v string) (time.Duration, bool) {
	if strings.HasSuffix(v, "d") {
		n, err := strconv.ParseUint(strings.TrimSuffix(v, "d"), 10, 31)
		if err != nil || n == 0 || n > uint64((1<<63-1)/int64(24*time.Hour)) {
			return 0, false
		}
		return time.Duration(n) * 24 * time.Hour, true
	}
	d, err := time.ParseDuration(v)
	return d, err == nil && d > 0
}

func parseQuotaPolicy(body string) (quotaPolicy, bool) {
	var p quotaPolicy
	values := map[string]string{}
	in := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "## ") {
			if in {
				break
			}
			in = line == "## Values"
			continue
		}
		if !in || !strings.HasPrefix(line, "- ") {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimPrefix(line, "- "), ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		switch key {
		case "default_expected", "urgent_priorities", "max_waits", "resume_text":
			if _, exists := values[key]; exists {
				return quotaPolicy{}, false
			}
			values[key] = strings.TrimSpace(value)
		}
	}
	fields := strings.Fields(values["default_expected"])
	if len(fields) == 0 {
		return p, false
	}
	d, ok := quotaDuration(fields[0])
	if !ok {
		return p, false
	}
	p.Expected = d
	raw := values["urgent_priorities"]
	if !strings.HasPrefix(raw, "[") || !strings.HasSuffix(raw, "]") {
		return p, false
	}
	p.Urgent = map[int]bool{}
	inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]"))
	if inner != "" {
		for _, item := range strings.Split(inner, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(item))
			if err != nil || n < 1 {
				return quotaPolicy{}, false
			}
			p.Urgent[n] = true
		}
	}
	n, err := strconv.Atoi(values["max_waits"])
	if err != nil || n < 0 || n > 1 {
		return quotaPolicy{}, false
	}
	p.MaxWaits = n
	text, err := strconv.Unquote(values["resume_text"])
	if err != nil || strings.TrimSpace(text) == "" || strings.ContainsAny(text, "\r\n") {
		return quotaPolicy{}, false
	}
	p.ResumeText = text
	return p, true
}

// quotaActsFor reports the roles whose quota stop the manager acts on even in
// shadow mode (hk 3334): an implementer either waits for the reset and is
// resumed once, or is handed over to another pool. Every other role stays
// observe-only unless --quota-stop-act is set.
func quotaActsFor(role string) bool { return role == "implementer" }

// quotaActsOn is quotaActsFor for a recorded decision: a decision with no
// proven quota screen (pool "unknown": a failed or timed-out screen read) is
// never acted on in shadow mode, because nothing showed the pane is stopped.
func quotaActsOn(role, pool string) bool { return quotaActsFor(role) && pool != "unknown" }

// quotaWaits reports whether a wait decision will actually be waited out,
// which is also whether the pane shows its wait token: tick skips the action
// of every other role in shadow mode.
func (q *quotaStopManager) quotaWaits(role, pool string) bool {
	return q.act || quotaActsOn(role, pool)
}

// quotaDecisionJob is the decision for an implementer (policy/quota-stop,
// hk 3334): wait only when the reset is at or before the job's start plus the
// task's expected time, which is the time left on the task. A missing expected
// time, an unknown start, an urgent task, a second stop after one resume, a
// dialog or an unknown reset all hand over; there is no default expected time.
func quotaDecisionJob(policy quotaPolicy, policyOK bool, priority int, expected time.Duration, started time.Time, waits int, stop quotaStop, now time.Time) (string, string) {
	if !policyOK {
		return "handover", "policy_unavailable"
	}
	if policy.Urgent[priority] {
		return "handover", "urgent"
	}
	if waits >= policy.MaxWaits {
		return "handover", "resume_limit"
	}
	if stop.Choice {
		return "handover", "choice_dialog"
	}
	if stop.Reset.IsZero() || !stop.Reset.After(now) {
		return "handover", "reset_unknown"
	}
	if expected <= 0 {
		return "handover", "expected_missing"
	}
	if started.IsZero() {
		return "handover", "start_unknown"
	}
	if stop.Reset.After(started.Add(expected)) {
		return "handover", "reset_too_late"
	}
	return "wait", "reset_within_expected"
}

func quotaDecision(policy quotaPolicy, policyOK bool, priority int, expected time.Duration, waits int, stop quotaStop, now time.Time) (string, string) {
	if !policyOK {
		return "handover", "policy_unavailable"
	}
	if policy.Urgent[priority] {
		return "handover", "urgent"
	}
	if waits >= policy.MaxWaits {
		return "handover", "resume_limit"
	}
	if stop.Choice {
		return "handover", "choice_dialog"
	}
	if stop.Reset.IsZero() || !stop.Reset.After(now) {
		return "handover", "reset_unknown"
	}
	if expected <= 0 {
		expected = policy.Expected
	}
	if stop.Reset.Sub(now) > expected {
		return "handover", "reset_too_late"
	}
	return "wait", "reset_within_expected"
}
