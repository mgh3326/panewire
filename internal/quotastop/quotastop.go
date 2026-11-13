// Package quotastop recognises a CLI quota or rate-limit stop on one visible
// pane screen. It is shared by the root package's quota-stop manager and the
// fleet job watcher, and it never sends input to a pane.
package quotastop

import (
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
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
	relative    = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)(?:\s*(?:and\s+|,\s*)?(\d+)\s*(seconds?|minutes?))?`)
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
		d, ok := relativeSpan(m[1], m[2])
		if ok && m[3] != "" {
			// "13 hours 9 minutes": the second span adds to the first.
			if extra, extraOK := relativeSpan(m[3], m[4]); extraOK && d <= time.Duration(1<<63-1)-extra {
				d += extra
			}
		}
		if ok {
			if d < maxSpan {
				out.Reset = now.Add(d).UTC()
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

// maxSpan marks a relative span too long for a time.Duration: the stop is
// real, its reset unreadable.
const maxSpan = time.Duration(1<<63 - 1)

// relativeSpan reads "<n> <unit>" as a duration. ok is false for a zero or
// unparsable count; a span past time.Duration is maxSpan.
func relativeSpan(count, unit string) (d time.Duration, ok bool) {
	n, err := strconv.ParseInt(count, 10, 32)
	if err != nil || n <= 0 {
		return 0, false
	}
	step := time.Second
	switch strings.ToLower(unit)[0] {
	case 'm':
		step = time.Minute
	case 'h':
		step = time.Hour
	}
	if n > int64(maxSpan/step) {
		return maxSpan, true
	}
	return time.Duration(n) * step, true
}

// statusPrefixes are the TUI marks a limit line can carry before its text: the
// bullet and warning marks, the square (■) and the tool-output corner (⎿).
// The square and corner were seen in a probe of the status line, not in a
// captured stop screen; no captured footer is accepted by chromeLine yet.
const statusPrefixes = "│┃•⚠️■⎿ "

// statusBlock accepts a CLI limit line as the last agent activity, wherever it
// sits on the visible screen. Only the CLI's own chrome (rules, an empty
// prompt, the option footer) and typed or injected user text may follow it:
// the relay's directive lands below the limit line, and neither it nor an empty
// prompt is agent output. Any other later line (ordinary output, source code,
// a completed answer) invalidates the stop.
//
// A user turn is its marked line plus the lines that provably wrap under it: a
// line continues the turn only when the previous line of the turn is so full
// that the continuation's first word could not have fitted, measured against
// the screen width (the widest rule line); a row under two thirds full is
// never wrapped. Position and indentation prove
// nothing, since an answer can be indented and follow with no blank line, so
// anything else after the marked line is agent output. A screen with no rule
// line to measure, or a wrap this arithmetic cannot prove (wide characters),
// fails closed: it is not accepted as a stop. A wrongly accepted stop ends a
// live job.
//
// The returned block is the limit line and the chrome after it, never the
// injected text, so a reset time or an "upgrade" mentioned in a directive
// cannot change the stop.
func statusBlock(screen string) (string, bool) {
	var limit string
	var tail []string
	found, later, injected := false, false, false
	rows := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	width := ruleWidth(rows)
	turnWidth := 0 // display width of the user turn's last line
	for _, raw := range rows {
		line := strings.TrimSpace(raw)
		switch {
		case chromeLine(line):
			// A rule or blank line ends a user message.
			injected = false
			tail = append(tail, line)
		case userTurn(line) || injected && wraps(turnWidth, line, width):
			// Typed or injected user text: a marked line and the lines that
			// provably wrap under it.
			injected = true
			turnWidth = textWidth(raw)
		default:
			injected = false
			line = strings.TrimLeft(line, statusPrefixes)
			if devin.MatchString(line) || claude.MatchString(line) || codexSignal.MatchString(line) {
				limit, tail, found, later = line, nil, true, false
			} else {
				later = true
			}
		}
	}
	if !found || later {
		return "", false
	}
	return limit + "\n" + strings.Join(tail, "\n"), true
}

// ruleWidth is the screen width as the widest horizontal rule shows it, or 0
// when the screen has none.
func ruleWidth(rows []string) int {
	width := 0
	for _, row := range rows {
		row = strings.TrimSpace(row)
		if n := utf8.RuneCountInString(row); n >= 10 && strings.Trim(row, "─━═") == "" && n > width {
			width = n
		}
	}
	return width
}

// textWidth is the number of characters on a row, trailing space excluded.
func textWidth(row string) int {
	return utf8.RuneCountInString(strings.TrimRight(row, " \t"))
}

// wraps reports whether line, the next row of a user turn whose last row was
// prev characters wide, can only be a wrap of it: the turn's row is within the
// screen width, at least two thirds full (a short row is never wrapped, however
// long the next word), and the continuation's first word would not have fitted.
func wraps(prev int, line string, width int) bool {
	if width == 0 || prev > width || prev*3 < width*2 {
		return false
	}
	word, _, _ := strings.Cut(line, " ")
	return prev+1+utf8.RuneCountInString(word) > width
}

// userTurn reports whether line starts a user turn: the prompt mark of devin,
// claude or codex followed by text. An empty prompt and the placeholders are
// chrome (chromeLine) and never reach here.
func userTurn(line string) bool {
	for _, mark := range []string{"❭", "❯", "›"} {
		if strings.HasPrefix(line, mark) {
			return true
		}
	}
	return false
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
