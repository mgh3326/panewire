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
	// PoolFromProfile marks a stop whose text names the Devin account limit
	// but not the rung that drew it (Devin's request rate limit, hk 4644):
	// the caller names the pool from the running profile instead.
	PoolFromProfile bool
}

// SamePool reports whether pool names this stop's pool. A stop whose pool the
// running profile decides matches either Devin pool: the screen cannot tell a
// free rung's limit from a paid rung's, so a resume guard must not read that
// difference as a changed screen.
func (s Stop) SamePool(pool string) bool {
	if s.PoolFromProfile {
		return pool == "devin" || pool == "devin-free"
	}
	return s.Pool == pool
}

var (
	// devin matches the phrase anywhere in the status line: the live screen
	// prefixes it with "▲ Something went wrong — ".
	devin = regexp.MustCompile(`(?i)reached free model rate limit(?:\s|[·.…:]|$)`)
	// devinPaid is the paid Devin weekly window spent (hk 4545). It is the devin
	// pool, not devin-free: paid devin-ds41 spends the account's paid window.
	devinPaid = regexp.MustCompile(`(?i)quota exhausted\s*[—–-]\s*your weekly usage quota has been exhausted(?:\s|[·.…:]|$)`)
	// devinRequest is Devin's short request-rate-limit stop (hk 4644). Its
	// message wraps across rows, so only the first row is matched here;
	// statusBlock joins the continuation.
	devinRequest = regexp.MustCompile(`(?i)reached request rate limit(?:\s|[·.…:]|$)`)
	claude       = regexp.MustCompile(`(?i)^you've hit your (?:(?:weekly|session) )?limit(?:\s|[·.:]|$)`)
	// codexSignal is codex's usage-limit sentence. The CLI renders the provider's
	// own message, which spells the apostrophe U+2019 ("You’ve"; hk 4982, the
	// 2026-11-16 codex stop), so both apostrophes are accepted.
	codexSignal = regexp.MustCompile(`(?i)^you[’']ve hit your usage limit[.·:].*try again at`)
	codex       = regexp.MustCompile(`(?i)try again at\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	// codexDate is codex's calendar reset ("try again at Nov 17th, 2026 12:02 AM",
	// hk 4982), which the CLI shows in local time.
	codexDate = regexp.MustCompile(`(?i)try again at\s+([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th),?\s+(\d{4})\s+(\d{1,2}:\d{2})\s*(AM|PM)`)
	atClock   = regexp.MustCompile(`(?i)\bat\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	relative  = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)(?:\s*(?:and\s+|,\s*)?(\d+)\s*(seconds?|minutes?))?`)
	dateReset = regexp.MustCompile(`(?i)resets?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(UTC|GMT|Z)|\s*\(([A-Za-z_]+/[A-Za-z_]+)\))?`)
	// The provider's own credit-exhausted rejection: Anthropic's "Your credit
	// balance is too low to access the Anthropic API" and OpenAI's
	// insufficient_quota / "You exceeded your current quota" (the errors
	// credit_rejection.go reads from a session).
	creditAnthropic = regexp.MustCompile(`(?i)credit balance is too low`)
	creditOpenAI    = regexp.MustCompile(`(?i)insufficient_quota|exceeded your current quota`)
	// creditCommandcode is the Command Code provider's rejection as omp renders
	// it as the status row ("400 You have insufficient credits to make this
	// request. Please purchase more credits to continue using the service.
	// (type=invalid_request_error", hk 5138). It anchors on the rendered error
	// row — the status, omp's "API Error:" prefix and the "⏺" mark — never on
	// the sentence anywhere in ordinary output, and never on a bare "400".
	creditCommandcode = regexp.MustCompile(`(?i)^(?:⏺\s*)?(?:api error:\s*)?400\s+you have insufficient credits to make this request`)
	// limitCommandcode is the Command Code provider's window limit as omp
	// renders it as the error row ("Error: 429 You've reached your 5-hour usage
	// limit for your plan. Your limit resets at 2026-11-18T08:31:56.856Z. Please
	// wait for the window to reset or upgrade your plan to continue.
	// (type=rate_limit_error param=RATE_LIMITED)", hk 5183). Only the 5-hour
	// wording is captured; the weekly row is matched by the same sentence with
	// the window named in one word ("weekly", "7-day"), which is a guess until a
	// weekly screen is captured. Like creditCommandcode it anchors on the
	// rendered error row (the status, omp's "Error:" / "API Error:" prefix and
	// the "⏺" mark), never on the sentence anywhere in ordinary output.
	limitCommandcode = regexp.MustCompile(`(?i)^(?:⏺\s*)?(?:(?:api )?error:\s*)?429\s+you[’']ve reached your [a-z0-9-]+ usage limit`)
	// limitCommandcodeReset is the row's own reset: an RFC 3339 timestamp.
	limitCommandcodeReset = regexp.MustCompile(`(?i)\blimit resets at\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))`)
	// limitCommandcodeDone ends the row: omp's "(type=… param=…)" suffix.
	limitCommandcodeDone = regexp.MustCompile(`(?i)\(type=[^)]*\)`)
)

// Classify reports the quota stop shown as the last activity of screen when the
// pane's agent kind is not known. An indented Devin banner is read only by
// ClassifyAgent, for a devin pane (hk 4680).
func Classify(screen string, now time.Time, local *time.Location) (Stop, bool) {
	return ClassifyAgent(screen, "", now, local)
}

// ClassifyAgent is Classify for a pane whose herdr agent kind is agent. Only a
// devin pane may have an indented banner read as a new limit: a blank row, then
// Devin's banner carrying its limit text and a reset, in the screen's last block
// (indentedDevinBlock, hk 4680 and hk 4690; docs/quota-stop.md, "Last block").
// A user turn after it, or output after a blank row, is not a stop.
func ClassifyAgent(screen, agent string, now time.Time, local *time.Location) (Stop, bool) {
	if local == nil {
		local = time.Local
	}
	if block, ok := statusBlock(screen); ok {
		return classifyStatus(block, now, local)
	}
	if agent != "devin" {
		return Stop{}, false
	}
	block, ok := indentedDevinBlock(screen)
	if !ok {
		return Stop{}, false
	}
	out, ok := classifyStatus(block, now, local)
	if !ok || !strings.HasPrefix(out.Pool, "devin") || out.Reset.IsZero() {
		return Stop{}, false
	}
	return out, true
}

// classifyStatus reads the pool and reset of a status block already accepted by
// the activity rule. screen is that block, never the whole pane.
func classifyStatus(screen string, now time.Time, local *time.Location) (Stop, bool) {
	var out Stop
	switch {
	case devin.MatchString(screen):
		out.Pool = "devin-free"
	case devinPaid.MatchString(screen):
		out.Pool = "devin"
	case devinRequest.MatchString(screen):
		out.Pool = "devin"
		out.PoolFromProfile = true
	case codexSignal.MatchString(screen) && (strings.Contains(strings.ToLower(screen), "limit") || strings.Contains(strings.ToLower(screen), "quota")):
		out.Pool = "codex"
	case claude.MatchString(screen):
		out.Pool = "claude"
	case creditAnthropic.MatchString(screen):
		// The provider's own rejection is a stop on the pool it names (hk 4982).
		out.Pool = "anthropic-api"
		out.CreditExhaustedAt = now.UTC()
	case creditOpenAI.MatchString(screen):
		out.Pool = "openai-api"
		out.CreditExhaustedAt = now.UTC()
	case creditCommandcode.MatchString(screen):
		out.Pool = "commandcode"
		out.CreditExhaustedAt = now.UTC()
	case limitCommandcode.MatchString(screen):
		// A window limit, not a dialog: its "upgrade your plan" text is not a
		// choice, and its reset is the row's own timestamp (zero when it does
		// not parse, which the manager hands over as reset_unknown).
		out.Pool = "commandcode"
		if m := limitCommandcodeReset.FindStringSubmatch(screen); m != nil {
			if reset, err := time.Parse(time.RFC3339Nano, m[1]); err == nil {
				out.Reset = reset.UTC()
			}
		}
		return out, true
	default:
		return out, false
	}
	out.Choice = isChoiceScreen(out.Pool, strings.ToLower(screen))
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
	if m := codexDate.FindStringSubmatch(screen); m != nil {
		month := m[1]
		if len(month) > 3 {
			month = month[:3]
		}
		value := month + " " + m[2] + " " + m[3] + " " + strings.ToUpper(m[4]) + m[5]
		if parsed, err := time.ParseInLocation("Jan 2 2006 3:04PM", value, local); err == nil {
			out.Reset = parsed.UTC()
		}
		return out, true
	}
	clockMatch := codex.FindStringSubmatch(screen)
	if len(clockMatch) == 0 && (out.Pool == "devin-free" || out.Pool == "devin") {
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

// isChoiceScreen reports whether a lower-cased limit screen is a choice dialog
// that waits for someone to pick an option. Claude's options menu and the credit
// wording are such dialogs. Devin's limit banners only advertise an upgrade or
// credits in their text ("Upgrade to Max for higher limits", hk 4680), so for a
// devin pool only the dialog's own menu header makes a choice (hk 4683).
func isChoiceScreen(pool, lower string) bool {
	if strings.HasPrefix(pool, "devin") {
		return strings.Contains(lower, "choose an option")
	}
	return strings.Contains(lower, "upgrade") || strings.Contains(lower, "credits") || strings.Contains(lower, "choose an option") || strings.Contains(lower, "wait until reset") || strings.Contains(lower, "wait for limit") || strings.Contains(lower, "use extra usage")
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
// sits on the visible screen. After it may come the CLI's own chrome (rules, an
// empty prompt, the option footer) and typed or injected user turns: the relay's
// directive lands below the limit line, and neither it nor an empty prompt is
// agent output. Any other later row (ordinary output, source code, a completed
// answer) invalidates the stop.
//
// A user turn is exactly one prompt-marked row. Flat screen text cannot tell a
// wrapped or multi-line message from an answer typed under it, so the rows
// after a turn are trusted only once a rule starts the composer: until then
// any non-blank row is agent output (so it is also never taken for a status,
// which keeps a limit quoted in a multi-line message from becoming one). A
// directive that spans several rows therefore fails closed: no stop. The one
// residual risk is an answer whose own row starts with the prompt mark; the
// screen alone cannot rule it out (docs/quota-stop.md).
//
// The returned block is the limit line and the chrome after it, never the
// injected text, so a reset time or an "upgrade" mentioned in a directive
// cannot change the stop.
func statusBlock(screen string) (string, bool) {
	_, block, ok := statusBlockOf(screen, limitRow)
	return block, ok
}

// statusBlockOf is statusBlock for any status row isStatus accepts, and also
// returns the status row itself. CreditExhausted reads the same activity rule,
// so a credit rejection quoted by an answer is not a stop.
func statusBlockOf(screen string, isStatus func(string) bool) (string, string, bool) {
	var status string
	var tail []string
	found, later, open, gap := false, false, false, false
	// limitOpen marks a Devin rate-limit banner (free-model hk 4680, request
	// rate hk 4644), whose message wraps onto the next rows indented with the
	// banner body. Those rows extend the limit line instead of counting as
	// later agent output; the flag clears at the banner's closing sentence or
	// at any row that ends the block.
	limitOpen, limitIndent := false, 0
	for _, raw := range strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n") {
		line := strings.TrimSpace(raw)
		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		switch {
		case line == "" || ruleLine(line):
			open = open && line == ""
			gap = gap || line == ""
			limitOpen = false
			tail = append(tail, line)
		case limitOpen && indent > 0 && indent >= limitIndent && !promptChrome(line) && !userTurn(line) && !footerLine(line):
			status += " " + line
			tail = nil
			limitOpen = !requestLimitComplete(status)
		case open && !promptChrome(line) && !userTurn(line) && !(gap && !strings.HasPrefix(raw, " ") && !strings.HasPrefix(raw, "\t") && isStatus(strings.TrimLeft(line, statusPrefixes))):
			// Under a user turn with no rule yet: agent output or a message
			// row of unknown ownership. A flush-left status after a blank row
			// is the exception: it is how a new limit follows a user turn.
			later = true
		case promptChrome(line) || !open && footerLine(line):
			tail = append(tail, line)
		case !open && composerLabelRule(line):
			tail = append(tail, "────")
		case userTurn(line):
			open, gap = true, false
		default:
			line = strings.TrimLeft(line, statusPrefixes)
			if isStatus(line) {
				status, tail, found, later, open = line, nil, true, false, false
				limitOpen, limitIndent = (devin.MatchString(line) || devinRequest.MatchString(line)) && !requestLimitComplete(line), indent
			} else if found && limitCommandcode.MatchString(status) && !limitCommandcodeDone.MatchString(status) && limitContinuationRow(line) && len(status)+len(line) < 600 {
				// omp's window-limit row wraps; its next rows join it until
				// the "(type=…)" suffix closes it (hk 5183).
				status += " " + line
				tail = nil
			} else if found && limitCommandcode.MatchString(status) && (limitRetryRow(line) || creditDiagnosticRow(line)) {
				// omp's retry notice and diagnostic path under the limit row
				// are the harness's chrome; the limit stays the last activity.
				tail = append(tail, line)
			} else if found && creditRow(status) && (creditDiagnosticRow(line) || creditContinuationRow(line)) {
				// omp writes its own diagnostic path under the rejection
				// ("raw-http-request=<file>", hk 4982) and the rejection itself
				// may wrap onto a continuation row (hk 5138). Both are the
				// harness's chrome, not output: the rejection stays the last
				// activity.
				tail = append(tail, line)
			} else {
				later = true
				limitOpen = false
			}
		}
	}
	if !found || later {
		return "", "", false
	}
	return status, status + "\n" + strings.Join(tail, "\n"), true
}

// requestLimitComplete reports whether the joined Devin request-rate-limit
// banner has reached its closing sentence. Until then its message may wrap
// onto the next banner-body rows; after it no row can be a continuation.
func requestLimitComplete(text string) bool {
	return strings.Contains(strings.ToLower(text), "send a message to retry")
}

// limitRow reports a row that is a CLI limit line or a provider
// credit-exhausted rejection: both are a stop on the pool they name.
func limitRow(line string) bool {
	line = strings.TrimLeft(line, statusPrefixes)
	return devin.MatchString(line) || devinPaid.MatchString(line) || devinRequest.MatchString(line) || claude.MatchString(line) || codexSignal.MatchString(line) || creditRow(line) || limitCommandcode.MatchString(line)
}

// CreditExhausted reports the pool named by a provider credit-exhausted line
// shown as the last agent activity of screen. It applies the same activity rule
// as a quota stop (statusBlockOf): a rejection quoted by a later answer, or one
// followed by ordinary agent output, is not a stop. A screen that names no
// provider credit text reports false.
func CreditExhausted(screen string) (string, bool) {
	line, _, ok := statusBlockOf(screen, creditRow)
	if !ok {
		return "", false
	}
	for _, provider := range []struct {
		pool string
		line *regexp.Regexp
	}{{"anthropic-api", creditAnthropic}, {"openai-api", creditOpenAI}, {"commandcode", creditCommandcode}} {
		if provider.line.MatchString(line) {
			return provider.pool, true
		}
	}
	return "", false
}

// creditRow reports a row that is a provider credit-exhausted rejection.
func creditRow(line string) bool {
	line = strings.TrimLeft(line, statusPrefixes)
	return creditAnthropic.MatchString(line) || creditOpenAI.MatchString(line) || creditCommandcode.MatchString(line)
}

// creditDiagnosticRow reports omp's diagnostic line under a provider
// rejection: the exact prefix it writes for the request it saved. Only a row
// after a credit status row is read as chrome (hk 4982).
func creditDiagnosticRow(line string) bool {
	_, ok := strings.CutPrefix(line, "raw-http-request=")
	return ok
}

// creditContinuationRow reports a wrapped continuation of a credit rejection:
// omp renders the error's tail ("(type=invalid_request_error",
// "Please purchase more credits…") on the next row when the message wraps.
// Ordinary output does not start with these fragments, so it still cancels.
func creditContinuationRow(line string) bool {
	line = strings.TrimLeft(line, statusPrefixes)
	lower := strings.ToLower(line)
	return strings.HasPrefix(lower, "(type=") || strings.HasPrefix(lower, "please purchase more credits") || strings.HasPrefix(lower, "purchase more credits")
}

// limitRetryRow reports omp's retry notice under a window-limit row ("Retry
// failed after 1 attempts: Provider requested 5075005ms wait, exceeds
// retry.maxDelayMs", hk 5183). It is read only after a limit status row.
func limitRetryRow(line string) bool {
	return strings.HasPrefix(strings.ToLower(line), "retry failed after ")
}

// limitContinuationWords are the words of the window-limit sentence. A wrapped
// continuation row is made of these, a number or a timestamp, and the
// "(type=…)" suffix, so ordinary agent output under an unfinished row still
// cancels the stop.
var limitContinuationWords = map[string]bool{
	"you've": true, "you’ve": true, "reached": true, "your": true, "usage": true, "limit": true, "for": true, "plan": true,
	"resets": true, "at": true, "please": true, "wait": true, "the": true, "window": true, "to": true, "reset": true,
	"or": true, "upgrade": true, "continue": true, "weekly": true, "week": true, "hour": true, "day": true, "429": true,
}

var limitWindowWord = regexp.MustCompile(`^\d+-(?:hour|day|week)$`)

var limitTimestampWord = regexp.MustCompile(`(?i)^\d{4}-\d{2}-\d{2}T[0-9:.]+(?:z|[+-]\d{2}:\d{2})?$`)

// limitContinuationRow reports a wrapped continuation of a window-limit row.
func limitContinuationRow(line string) bool {
	words := strings.Fields(strings.ToLower(line))
	if len(words) == 0 {
		return false
	}
	for _, word := range words {
		word = strings.Trim(word, ".,;:()")
		switch {
		case limitContinuationWords[word], limitTimestampWord.MatchString(word), limitWindowWord.MatchString(word):
		case strings.HasPrefix(word, "type=") || strings.HasPrefix(word, "param="):
		default:
			return false
		}
	}
	return true
}

// userTurn reports whether line starts a user turn: the prompt mark of devin,
// claude or codex followed by text. An empty prompt and the placeholders are
// promptChrome and checked first.
func userTurn(line string) bool {
	for _, mark := range []string{"❭", "❯", "›"} {
		if strings.HasPrefix(line, mark) {
			return true
		}
	}
	return false
}

// ruleLine reports a horizontal rule or box edge.
func ruleLine(line string) bool {
	return line != "" && strings.Trim(line, "─━═│┃┌┐└┘╭╮╰╯- ") == ""
}

// promptChrome reports an empty prompt or its placeholder.
func promptChrome(line string) bool {
	if line == "❯" || line == "❭" || line == "›" {
		return true
	}
	lower := strings.ToLower(line)
	for _, placeholder := range []string{"❯ ask claude", "❭ ask devin", "› ask codex"} {
		if strings.HasPrefix(lower, placeholder) {
			return true
		}
	}
	return false
}

// footerLine reports the option menu and status footer a limit screen draws,
// and Devin's working-directory row above the composer (devinCwdRow).
func footerLine(line string) bool {
	lower := strings.ToLower(line)
	for _, prefix := range []string{"choose an option:", "1. wait", "2. upgrade", "1. stop", "2. use", "swe-2", "esc to", "press enter", "select an option"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return devinCwdRow(line)
}

// devinCwdGlyphs are the status marks Devin draws before its working directory:
// ⏺ (hk 4644) and ● (hk 4680, the screen after a free-model limit). Only these
// two are evidenced; another mark is added with a captured screen.
var devinCwdGlyphs = []string{"⏺", "●"}

// devinCwdRow reports Devin's working-directory row: one status mark, a space,
// then one path token (~/… or /…) and nothing else. Assistant output shares the
// marks, so prose on the same row is output, not chrome (hk 4680).
func devinCwdRow(line string) bool {
	for _, glyph := range devinCwdGlyphs {
		rest, ok := strings.CutPrefix(line, glyph+" ")
		if !ok {
			continue
		}
		if (strings.HasPrefix(rest, "~/") || strings.HasPrefix(rest, "/")) && !strings.ContainsAny(rest, " \t") {
			return true
		}
	}
	return false
}

// composerLabelRule reports the composer rule Devin draws with the first turn's
// text as its label ("──── Your brief is the file … ─", hk 4680). It is chrome
// only where footerLine is: after a status, never under a user turn. The label
// is user text, so the caller keeps the rule and never its label.
func composerLabelRule(line string) bool {
	return strings.HasPrefix(line, "────") && strings.HasSuffix(line, " ─")
}
