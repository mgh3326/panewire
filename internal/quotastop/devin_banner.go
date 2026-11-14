package quotastop

import (
	"regexp"
	"strings"
)

// devinFooter is Devin's model footer under the composer ("SWE-2 Medium",
// "SWE-2 Flash High · bypass …"). Only capitalised model words match, so prose
// that begins "SWE-2 is …" stays output.
var devinFooter = regexp.MustCompile(`^SWE-2 [A-Z][a-z]+`)

// devinBannerHead reports a row that opens a Devin error banner. Devin indents
// the banner by one space, so the head is matched after any leading blanks.
func devinBannerHead(line string) bool {
	return strings.HasPrefix(strings.TrimLeft(line, " \t"), "▲ Something went wrong")
}

// devinLimitText reports text that carries one of Devin's limit messages.
func devinLimitText(text string) bool {
	return devin.MatchString(text) || devinPaid.MatchString(text) || devinRequest.MatchString(text)
}

// devinChromeRow reports a trimmed row that is Devin chrome: a rule, the
// labelled composer rule, the empty input row or its placeholder, Devin's
// working-directory row and its model footer. indentedDevinBlock accepts such a
// row only in a block of chrome after a blank row (docs/quota-stop.md, "last
// block"); a row after a blank that is not chrome is output, a quoted banner or
// another block.
func devinChromeRow(trimmed string) bool {
	return trimmed == "" || ruleLine(trimmed) || composerLabelRule(trimmed) ||
		promptChrome(trimmed) || devinCwdRow(trimmed) || devinFooter.MatchString(trimmed)
}

// indentedDevinBlock finds the last Devin limit banner in screen that is
// indented, follows a blank row, and reaches its closing sentence. It returns
// the banner's joined text only when the banner is in the screen's last block
// (hk 4680, operator decision B; hk 4690, operator decision 264): the rows
// directly under the banner, up to the next blank row, are its own block and
// whatever they are is not checked; after a blank row, only blocks of Devin
// chrome may follow, and no user turn may follow at all. The banner is one
// indented row, or a "▲ Something went wrong" row whose body is indented on the
// rows after it. Only the last banner is considered: an earlier banner has a
// later banner's rows after it, which are not chrome.
func indentedDevinBlock(screen string) (string, bool) {
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	start := -1
	for i := len(lines) - 1; i >= 0; i-- {
		if devinBannerHead(lines[i]) {
			start = i
			break
		}
	}
	if start < 1 || strings.TrimSpace(lines[start-1]) != "" || leadingSpace(lines[start]) < 1 {
		return "", false
	}
	text := strings.TrimSpace(lines[start])
	end, body := start, leadingSpace(lines[start])
	if !devinLimitText(text) {
		if start+1 >= len(lines) || leadingSpace(lines[start+1]) < 1 {
			return "", false
		}
		end, body = start+1, leadingSpace(lines[start+1])
		text += " " + strings.TrimSpace(lines[end])
	}
	for !requestLimitComplete(text) && end+1 < len(lines) {
		next := lines[end+1]
		if strings.TrimSpace(next) == "" || leadingSpace(next) < body {
			break
		}
		text += " " + strings.TrimSpace(next)
		end++
	}
	if !devinLimitText(text) || !requestLimitComplete(text) {
		return "", false
	}
	// gap is set by the first blank row after the banner's block. From then on
	// every row must be chrome, so a later block of output, a quoted banner or
	// a user turn means the banner is not in the last block.
	gap := false
	for _, raw := range lines[end+1:] {
		row := strings.TrimSpace(raw)
		switch {
		case row == "":
			gap = true
		case userTurn(row) && !promptChrome(row):
			return "", false
		case gap && !devinChromeRow(row):
			return "", false
		}
	}
	return text, true
}

// leadingSpace counts the spaces and tabs before a row's first character.
func leadingSpace(raw string) int {
	return len(raw) - len(strings.TrimLeft(raw, " \t"))
}
