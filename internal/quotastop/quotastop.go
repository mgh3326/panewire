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
	Pool              string
	Reset             time.Time
	Choice            bool
	CreditExhausted   bool
	CreditExhaustedAt time.Time
}

var (
	// devin matches the phrase anywhere in the status line: the live screen
	// prefixes it with "▲ Something went wrong — ".
	devin           = regexp.MustCompile(`(?i)reached free model rate limit(?:\s|[·.…:]|$)`)
	apiErrorStart   = regexp.MustCompile(`(?i)^api error:\s*[0-9]{3}\b`)
	providerErrType = regexp.MustCompile(`(?i)^[a-z][a-z0-9_]*_error\b[\s:,-]*`)
	jsonScalar      = regexp.MustCompile(`^(?:-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)[,}\]]?$`)
	requestOpen     = regexp.MustCompile(`^\[\s*request\s*:\s*[a-z0-9_-]*$`)
	requestClosed   = regexp.MustCompile(`^\[\s*request\s*:\s*[a-z0-9_-]+\s*\][\s.,;:!?]*$`)
	anthropicCredit = regexp.MustCompile(`(?is)^api error:\s*[0-9]{3}\b.*credit balance is too low to access the anthropic api`)
	openAICredit    = regexp.MustCompile(`(?is)^api error:\s*[0-9]{3}\b.*(insufficient_\s*quota|exceeded\s*your\s*current\s*quota)`)
	claude          = regexp.MustCompile(`(?i)^you've hit your (?:(?:weekly|session) )?limit(?:\s|[·.:]|$)`)
	codexSignal     = regexp.MustCompile(`(?i)^you've hit your usage limit[.·:].*try again at`)
	codex           = regexp.MustCompile(`(?i)try again at\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	atClock         = regexp.MustCompile(`(?i)\bat\s+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM)?)(?:\s+(UTC|GMT|Z))?`)
	relative        = regexp.MustCompile(`(?i)reset(?:s)? in\s+(\d+)\s*(seconds?|minutes?|hours?)`)
	dateReset       = regexp.MustCompile(`(?i)resets?\s+([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm))(?:\s+(UTC|GMT|Z)|\s*\(([A-Za-z_]+/[A-Za-z_]+)\))?`)
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
	providerPool := providerCreditPool(screen)
	var out Stop
	switch {
	case providerPool != "":
		out.Pool, out.CreditExhausted, out.CreditExhaustedAt = providerPool, true, now.UTC()
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

// statusBlock accepts only an exact CLI status line as the last activity.
// Provider error continuations stay inside an open JSON body.
func statusBlock(screen string) (string, bool) {
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		line = trimScreenBorder(line)
		if apiErrorStart.MatchString(line) {
			return providerErrorBlock(lines, i)
		}
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

func providerErrorBlock(lines []string, start int) (string, bool) {
	border := screenBorderPrefix(lines[start])
	if len(lines)-start > 16 || border == '┌' {
		return "", false
	}
	blockLines := []string{trimScreenBorder(strings.TrimSpace(lines[start]))}
	jsonBody := hasProviderJSONBody(blockLines[0])
	end := len(lines)
	promptFound := false
	for i := start + 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		content := trimScreenBorder(line)
		if promptLine(line) {
			end, promptFound = i, true
			break
		}
		if apiErrorStart.MatchString(content) {
			return "", false
		}
		if chromeLine(line) {
			continue
		}
		currentBlock := strings.Join(blockLines, "\n")
		json := jsonProgress{}
		if jsonBody {
			json = scanJSON(currentBlock)
		}
		joined := currentBlock + "\n" + content
		if json.open && (json.inString && providerTextPrefix(currentJSONString(currentBlock)+content) || !json.inString && (jsonContinuation(content) || jsonScalarContinuation(content, json))) {
			blockLines = append(blockLines, content)
			continue
		}
		plainContinuation := providerTextPrefix(plainProviderText(joined)) || anthropicRequestBracketComplete(joined)
		if !jsonBody && plainContinuation && (border == 0 || screenBorderPrefix(line) == border || anthropicRequestBracketOpen(currentBlock)) {
			blockLines = append(blockLines, content)
			continue
		}
		return "", false
	}
	if !promptFound {
		return "", false
	}
	block := strings.Join(blockLines, " ")
	json := jsonProgress{}
	if jsonBody {
		json = scanJSON(strings.Join(blockLines, "\n"))
	}
	if json.started && json.open {
		return "", false
	}
	if !jsonBody && !providerTextPrefix(plainProviderText(block)) && !anthropicRequestBracketComplete(block) {
		return "", false
	}
	for _, tail := range lines[end:] {
		if !chromeLine(tail) {
			return "", false
		}
	}
	if providerCreditPool(block) == "" {
		return "", false
	}
	return block, true
}

func providerCreditPool(block string) string {
	if anthropicCredit.MatchString(block) {
		return "anthropic-api"
	}
	if openAICredit.MatchString(block) {
		return "openai-api"
	}
	compact := strings.Join(strings.Fields(block), "")
	compact = strings.ToLower(compact)
	if strings.HasPrefix(compact, "apierror:") {
		if strings.Contains(compact, "creditbalanceistoolowtoaccesstheanthropicapi") {
			return "anthropic-api"
		}
		if strings.Contains(compact, "insufficient_quota") || strings.Contains(compact, "exceededyourcurrentquota") {
			return "openai-api"
		}
	}
	return ""
}

func chromeLine(line string) bool {
	line = strings.TrimSpace(line)
	line = trimScreenBorder(line)
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

func promptLine(line string) bool {
	line = trimScreenBorder(strings.TrimSpace(line))
	lower := strings.ToLower(line)
	for _, glyph := range []string{"❯", "❭", "›"} {
		if line == glyph || strings.HasPrefix(lower, strings.ToLower(glyph+" ask ")) {
			return true
		}
	}
	return false
}

func jsonContinuation(line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return false
	}
	return strings.ContainsRune(`{}[],:"`, []rune(line)[0])
}

func jsonScalarContinuation(line string, state jsonProgress) bool {
	return state.expectValue && jsonScalar.MatchString(strings.TrimSpace(line))
}

type jsonProgress struct {
	started     bool
	open        bool
	inString    bool
	expectValue bool
}

func scanJSON(block string) jsonProgress {
	var state jsonProgress
	depth, inString, escaped := 0, false, false
	var containers []rune
	for _, r := range block {
		if !state.started {
			if r == '{' || r == '[' {
				state.started = true
				depth++
				containers = append(containers, r)
				state.expectValue = r == '['
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if inString && r == '\\' {
			escaped = true
			continue
		}
		if r == '"' {
			inString = !inString
			if inString {
				state.expectValue = false
			}
			continue
		}
		if !inString {
			switch r {
			case '{', '[':
				depth++
				containers = append(containers, r)
				state.expectValue = r == '['
			case '}':
				depth--
				if len(containers) > 0 {
					containers = containers[:len(containers)-1]
				}
				state.expectValue = false
			case ']':
				depth--
				if len(containers) > 0 {
					containers = containers[:len(containers)-1]
				}
				state.expectValue = false
			case ':':
				state.expectValue = true
			case ',':
				state.expectValue = len(containers) > 0 && containers[len(containers)-1] == '['
			case ' ', '\t', '\n', '\r':
			default:
				state.expectValue = false
			}
		}
	}
	state.open = depth != 0 || inString
	state.inString = inString
	return state
}

func plainProviderText(block string) string {
	body := strings.TrimSpace(apiErrorStart.ReplaceAllString(block, ""))
	return providerErrType.ReplaceAllString(body, "")
}

func hasProviderJSONBody(line string) bool {
	body := plainProviderText(line)
	return strings.HasPrefix(strings.TrimSpace(body), "{") || strings.HasPrefix(strings.TrimSpace(body), "[")
}

func providerTextPrefix(message string) bool {
	var compact strings.Builder
	for _, r := range strings.ToLower(message) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			compact.WriteRune(r)
		}
	}
	value := compact.String()
	if value == "" {
		return false
	}
	for _, expected := range []string{
		"yourcreditbalanceistoolowtoaccesstheanthropicapipleasegotoplansbillingtoupgradeorpurchasecredits",
		"insufficientquota",
		"insufficientquotapleasecheckyourplanandbillingdetails",
		"youexceededyourcurrentquota",
		"youexceededyourcurrentquotapleasecheckyourplanandbillingdetails",
		"exceededyourcurrentquotapleasecheckyourplanandbillingdetails",
	} {
		if strings.HasPrefix(expected, value) {
			return true
		}
	}
	return false
}

func anthropicRequestBracketOpen(block string) bool {
	tail, ok := anthropicRequestTail(block)
	return ok && requestOpen.MatchString(tail)
}

func anthropicRequestBracketComplete(block string) bool {
	tail, ok := anthropicRequestTail(block)
	return ok && requestClosed.MatchString(tail)
}

func anthropicRequestTail(block string) (string, bool) {
	body := strings.ToLower(strings.Join(strings.Fields(plainProviderText(block)), " "))
	message := "your credit balance is too low to access the anthropic api"
	start := strings.Index(body, message)
	if start < 0 {
		return "", false
	}
	tail := strings.TrimLeft(body[start+len(message):], " .,:;!?")
	return tail, true
}

func currentJSONString(block string) string {
	start := strings.IndexAny(block, "{")
	if start < 0 {
		return ""
	}
	inString, escaped := false, false
	var value strings.Builder
	for _, r := range block[start:] {
		if escaped {
			value.WriteRune(r)
			escaped = false
			continue
		}
		if inString && r == '\\' {
			escaped = true
			continue
		}
		if r == '"' {
			if inString {
				inString = false
				value.Reset()
			} else {
				inString = true
				value.Reset()
			}
			continue
		}
		if inString {
			value.WriteRune(r)
		}
	}
	if !inString {
		return ""
	}
	return value.String()
}

func screenBorderPrefix(line string) rune {
	line = strings.TrimLeft(line, " \t")
	if line == "" {
		return 0
	}
	r := []rune(line)[0]
	if strings.ContainsRune("│┃┌└|", r) {
		return r
	}
	return 0
}

func trimScreenBorder(line string) string {
	return strings.TrimLeft(line, "│┃┌┐└┘╭╮╰╯|•⚠️ ")
}
