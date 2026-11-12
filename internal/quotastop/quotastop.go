// Package quotastop recognises a CLI quota or rate-limit stop on one visible
// pane screen. It is shared by the root package's quota-stop manager and the
// fleet job watcher, and it never sends input to a pane.
package quotastop

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Stop is evidence from a single visible screen, not proof that sending
// input to the pane is safe. An unknown reset is represented by a zero time.
type Stop struct {
	CreditExhaustedAt time.Time
	Pool              string
	Reset             time.Time
	Choice            bool
}

var (
	// devin matches the phrase anywhere in the status line: the live screen
	// prefixes it with "▲ Something went wrong — ".
	devin       = regexp.MustCompile(`(?i)reached free model rate limit(?:\s|[·.…:]|$)`)
	claude      = regexp.MustCompile(`(?i)^you've hit your (?:(?:weekly|session) )?limit(?:\s|[·.:]|$)`)
	codexSignal = regexp.MustCompile(`(?i)^you've hit your usage limit[.·:].*try again at`)
	codex       = regexp.MustCompile(`(?i)try again at\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	atClock     = regexp.MustCompile(`(?i)\bat\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	relative    = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)`)
	dateReset   = regexp.MustCompile(`(?i)resets?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(UTC|GMT|Z)|\s*\(([A-Za-z_]+/[A-Za-z_]+)\))?`)
)

// Classify reports the quota stop shown as the last activity of screen.
func Classify(screen string, now time.Time, local *time.Location) (Stop, bool) {
	if local == nil {
		local = time.Local
	}
	block, ok := statusBlock(screen)
	if !ok {
		return Stop{}, false
	}
	screen = block
	var out Stop
	switch {
	case devin.MatchString(screen):
		out.Pool = "devin-free"
	case codexSignal.MatchString(screen) && (strings.Contains(strings.ToLower(screen), "limit") || strings.Contains(strings.ToLower(screen), "quota")):
		out.Pool = "codex"
	case claude.MatchString(screen):
		out.Pool = "claude"
	default:
		return out, false
	}
	lower := strings.ToLower(screen)
	out.Choice = strings.Contains(lower, "upgrade") || strings.Contains(lower, "credits") || strings.Contains(lower, "choose an option") || strings.Contains(lower, "wait until reset") || strings.Contains(lower, "wait for limit") || strings.Contains(lower, "use extra usage")
	if m := relative.FindStringSubmatch(screen); m != nil {
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
	if m := dateReset.FindStringSubmatch(screen); m != nil {
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
	clockMatch := codex.FindStringSubmatch(screen)
	if len(clockMatch) == 0 && out.Pool == "devin-free" {
		clockMatch = atClock.FindStringSubmatch(screen)
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

// statusPrefixes are the TUI marks a limit line can carry before its text: the
// bullet and warning marks, the square (■) and the tool-output corner (⎿).
// The square and corner were seen in a probe of the status line, not in a
// captured stop screen; no captured footer is accepted by chromeLine yet.
const statusPrefixes = "│┃•⚠️■⎿ "

// statusBlock accepts only an exact CLI status line as the last
// activity; later ordinary output, source code, or a completed answer invalidates it.
func statusBlock(screen string) (string, bool) {
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		line = strings.TrimLeft(line, statusPrefixes)
		if devin.MatchString(line) || claude.MatchString(line) || codexSignal.MatchString(line) {
			if len(lines)-i > 16 {
				return "", false
			}
			for _, tail := range lines[i+1:] {
				if !chromeLine(tail) {
					return "", false
				}
			}
			return line + "\n" + strings.Join(lines[i+1:], "\n"), true
		}
	}
	return "", false
}

func chromeLine(line string) bool {
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
