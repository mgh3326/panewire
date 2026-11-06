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
