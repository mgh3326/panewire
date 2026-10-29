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
	quotaDevin    = regexp.MustCompile(`(?i)reached free model rate limit`)
	quotaClaude   = regexp.MustCompile(`(?i)(?:you've hit your (?:weekly|session) limit|(?:weekly|session) limit reached)`)
	quotaCodex    = regexp.MustCompile(`(?i)try again at\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)`)
	quotaRelative = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)`)
	quotaDate     = regexp.MustCompile(`(?i)resets?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))`)
)

func classifyQuotaStop(screen string, now time.Time, local *time.Location) (quotaStop, bool) {
	if local == nil {
		local = time.Local
	}
	var out quotaStop
	switch {
	case quotaDevin.MatchString(screen):
		out.Pool = "devin-free"
	case quotaClaude.MatchString(screen):
		out.Pool = "claude"
	case quotaCodex.MatchString(screen) && (strings.Contains(strings.ToLower(screen), "limit") || strings.Contains(strings.ToLower(screen), "quota")):
		out.Pool = "codex"
	default:
		return out, false
	}
	lower := strings.ToLower(screen)
	out.Choice = strings.Contains(lower, "upgrade") || strings.Contains(lower, "credits") || strings.Contains(lower, "choose an option")
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
			out.Reset = now.Add(time.Duration(n) * unit).UTC()
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
		year := now.In(local).Year()
		value := month + " " + m[2] + " " + strconv.Itoa(year) + " " + clock
		parsed, err := time.ParseInLocation(layout, value, local)
		if err == nil {
			if parsed.Before(now) {
				parsed = parsed.AddDate(1, 0, 0)
			}
			out.Reset = parsed.UTC()
		}
		return out, true
	}
	if m := quotaCodex.FindStringSubmatch(screen); m != nil {
		clock := strings.ToUpper(strings.ReplaceAll(m[1], " ", ""))
		layout := "15:04"
		if strings.Contains(clock, "M") {
			layout = "3:04PM"
		}
		parsed, err := time.ParseInLocation(layout, clock, local)
		if err == nil {
			t := now.In(local)
			parsed = time.Date(t.Year(), t.Month(), t.Day(), parsed.Hour(), parsed.Minute(), 0, 0, local)
			if parsed.Before(now) {
				parsed = parsed.AddDate(0, 0, 1)
			}
			out.Reset = parsed.UTC()
		}
	}
	return out, true
}
