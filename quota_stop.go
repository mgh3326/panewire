package panewire

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// quotaStop is evidence from a single visible screen, not proof that sending
// input to the pane is safe. An unknown reset is represented by a zero time.
type quotaStop struct {
	Pool   string
	Reset  time.Time
	Choice bool
}

var (
	quotaDevin       = regexp.MustCompile(`(?i)^reached free model rate limit(?:\s|[·.…:]|$)`)
	quotaClaude      = regexp.MustCompile(`(?i)^you've hit your (?:(?:weekly|session) )?limit(?:\s|[·.:]|$)`)
	quotaCodexSignal = regexp.MustCompile(`(?i)^you've hit your usage limit[.·:].*try again at`)
	quotaCodex       = regexp.MustCompile(`(?i)try again at\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	quotaAt          = regexp.MustCompile(`(?i)\bat\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	quotaRelative    = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)`)
	quotaDate        = regexp.MustCompile(`(?i)resets?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(UTC|GMT|Z)|\s*\(([A-Za-z_]+/[A-Za-z_]+)\))?`)
)

func classifyQuotaStop(screen string, now time.Time, local *time.Location) (quotaStop, bool) {
	if local == nil {
		local = time.Local
	}
	block, ok := quotaStatusBlock(screen)
	if !ok {
		return quotaStop{}, false
	}
	screen = block
	var out quotaStop
	switch {
	case quotaDevin.MatchString(screen):
		out.Pool = "devin-free"
	case quotaCodexSignal.MatchString(screen) && (strings.Contains(strings.ToLower(screen), "limit") || strings.Contains(strings.ToLower(screen), "quota")):
		out.Pool = "codex"
	case quotaClaude.MatchString(screen):
		out.Pool = "claude"
	default:
		return out, false
	}
	lower := strings.ToLower(screen)
	out.Choice = strings.Contains(lower, "upgrade") || strings.Contains(lower, "credits") || strings.Contains(lower, "choose an option") || strings.Contains(lower, "wait until reset") || strings.Contains(lower, "wait for limit") || strings.Contains(lower, "use extra usage")
	if m := quotaRelative.FindStringSubmatch(screen); m != nil {
		n, err := strconv.ParseInt(m[1], 10, 32)
		if err == nil && n > 0 {
			unit := time.Second
			if strings.HasPrefix(strings.ToLower(m[2]), "minute") {
				unit = time.Minute
			}
			if strings.HasPrefix(strings.ToLower(m[2]), "hour") {
				unit = time.Hour
			}
			if n <= int64((1<<63-1)/unit) {
				out.Reset = now.Add(time.Duration(n) * unit).UTC()
			}
			return out, true
		}
	}
	if m := quotaDate.FindStringSubmatch(screen); m != nil {
		clock := strings.ToUpper(strings.ReplaceAll(m[3], " ", ""))
		layout := "Jan 2 2006 3PM"
		if strings.Contains(clock, ":") {
			layout = "Jan 2 2006 3:04PM"
		}
		month := m[1]
		if len(month) > 3 {
			month = month[:3]
		}
		zone := local
		if m[4] != "" {
			zone = time.UTC
		}
		if m[5] != "" {
			var err error
			zone, err = time.LoadLocation(m[5])
			if err != nil {
				return out, true
			}
		}
		year := now.In(zone).Year()
		value := month + " " + m[2] + " " + strconv.Itoa(year) + " " + clock
		parsed, err := time.ParseInLocation(layout, value, zone)
		if err == nil {
			if parsed.Before(now) {
				parsed = parsed.AddDate(1, 0, 0)
			}
			out.Reset = parsed.UTC()
		}
		return out, true
	}
	clockMatch := quotaCodex.FindStringSubmatch(screen)
	if len(clockMatch) == 0 && out.Pool == "devin-free" {
		clockMatch = quotaAt.FindStringSubmatch(screen)
	}
	if m := clockMatch; m != nil {
		clock := strings.ToUpper(strings.ReplaceAll(m[1], " ", ""))
		layout := "15:04"
		if strings.Contains(clock, "M") {
			layout = "3:04PM"
		}
		zone := local
		if len(m) > 2 && m[2] != "" {
			zone = time.UTC
		}
		parsed, err := time.ParseInLocation(layout, clock, zone)
		if err == nil {
			t := now.In(zone)
			parsed = time.Date(t.Year(), t.Month(), t.Day(), parsed.Hour(), parsed.Minute(), 0, 0, zone)
			if parsed.Before(now) {
				parsed = parsed.AddDate(0, 0, 1)
			}
			out.Reset = parsed.UTC()
		}
	}
	return out, true
}

// quotaStatusBlock accepts only an exact CLI status line as the last
// activity; later ordinary output, source code, or a completed answer invalidates it.
func quotaStatusBlock(screen string) (string, bool) {
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		line = strings.TrimLeft(line, "│┃•⚠️ ")
		if quotaDevin.MatchString(line) || quotaClaude.MatchString(line) || quotaCodexSignal.MatchString(line) {
			if len(lines)-i > 16 {
				return "", false
			}
			for _, tail := range lines[i+1:] {
				if !quotaChromeLine(tail) {
					return "", false
				}
			}
			return line + "\n" + strings.Join(lines[i+1:], "\n"), true
		}
	}
	return "", false
}

func quotaChromeLine(line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return true
	}
	if strings.Trim(line, "─━═│┃┌┐└┘╭╮╰╯- ") == "" {
		return true
	}
	lower := strings.ToLower(line)
	if line == "❯" || line == "❭" || line == "›" {
		return true
	}
	for _, placeholder := range []string{"❯ ask claude", "❭ ask devin", "› ask codex"} {
		if strings.HasPrefix(lower, placeholder) {
			return true
		}
	}
	for _, prefix := range []string{"choose an option:", "1. wait", "2. upgrade", "1. stop", "2. use", "swe-2", "esc to", "press enter", "select an option"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
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
