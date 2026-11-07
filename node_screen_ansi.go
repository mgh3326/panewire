package panewire

import (
	"strconv"
	"strings"
	"unicode"
)

// nodeScreenRow is one screen row read with `herdr pane read --format ansi`:
// its visible characters, and for each one the SGR state in effect when it was
// drawn: dim (SGR 2), reverse video (SGR 7) and the foreground colour.
type nodeScreenRow struct {
	text    []rune
	dim     []bool
	reverse []bool
	fg      []string
}

// nodeSGR is the SGR state the parser carries across characters. fg is the
// foreground colour as the sequence that selected it ("38;2;138;148;168",
// "38;5;244", "31"), or "" for the default foreground.
type nodeSGR struct {
	dim     bool
	reverse bool
	fg      string
}

// parseNodeScreen splits an ANSI screen into rows. SGR state carries across
// rows, because a renderer need not reset at a line break. Every escape
// sequence is dropped from the text, so a plain-text screen parses to itself
// with no dim character, and so does a screen from a herdr that ignores the
// format. A sequence cut off at the end of the screen is dropped.
func parseNodeScreen(raw string) []nodeScreenRow {
	rows := []nodeScreenRow{{}}
	var sgr nodeSGR
	runes := []rune(raw)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case r == '\r':
		case r == '\n':
			rows = append(rows, nodeScreenRow{})
		case r == 0x1b:
			if i+1 >= len(runes) {
				return rows
			}
			i++
			switch runes[i] {
			case '[':
				start := i + 1
				i = start
				for i < len(runes) && (runes[i] < 0x40 || runes[i] > 0x7e) {
					i++
				}
				if i < len(runes) && runes[i] == 'm' {
					sgr = sgr.apply(string(runes[start:i]))
				}
			case ']':
				for i++; i < len(runes) && runes[i] != 0x07 && !(runes[i] == 0x1b && i+1 < len(runes) && runes[i+1] == '\\'); i++ {
				}
				if i < len(runes) && runes[i] == 0x1b {
					i++
				}
			default:
				// ESC, intermediates, one final byte: character set selection
				// and the like.
				for i < len(runes) && runes[i] >= 0x20 && runes[i] <= 0x2f {
					i++
				}
			}
		default:
			row := &rows[len(rows)-1]
			row.text = append(row.text, r)
			row.dim = append(row.dim, sgr.dim)
			row.reverse = append(row.reverse, sgr.reverse)
			row.fg = append(row.fg, sgr.fg)
		}
	}
	return rows
}

// apply returns the state after one SGR parameter list. Only 2 turns dim on
// and only 0 and 22 turn it off; 7 and 27 switch reverse video; 0, 39 and the
// 30-37, 90-97 and 38 forms set the foreground. The extended colour forms
// (38/48/58 with ;5;n or ;2;r;g;b) carry numbers that are not SGR codes, so a
// "2" inside one is not dim.
func (state nodeSGR) apply(params string) nodeSGR {
	fields := strings.Split(params, ";")
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if head, _, colon := strings.Cut(field, ":"); colon {
			if head == "38" || head == "48" || head == "58" {
				continue
			}
			field = head
		}
		code := 0
		if field != "" {
			var err error
			if code, err = strconv.Atoi(field); err != nil {
				continue
			}
		}
		switch {
		case code == 0:
			state = nodeSGR{}
		case code == 22:
			state.dim = false
		case code == 2:
			state.dim = true
		case code == 7:
			state.reverse = true
		case code == 27:
			state.reverse = false
		case code == 39, code >= 30 && code <= 37, code >= 90 && code <= 97:
			state.fg = ""
			if code != 39 {
				state.fg = strconv.Itoa(code)
			}
		case code == 38, code == 48, code == 58:
			width := 0
			if i+1 < len(fields) {
				switch fields[i+1] {
				case "5":
					width = 2
				case "2":
					width = 4
				}
			}
			if code == 38 && width > 0 && i+width < len(fields) {
				state.fg = strings.Join(fields[i:i+width+1], ";")
			}
			i += width
		}
	}
	return state
}

func (row nodeScreenRow) String() string { return string(row.text) }

// nodeScreenPlain is the screen's text with every escape sequence removed.
func nodeScreenPlain(raw string) string {
	rows := parseNodeScreen(raw)
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = row.String()
	}
	return strings.Join(lines, "\n")
}

// solid reports whether the row, from character index from on, draws any
// non-blank character that is not dim.
func (row nodeScreenRow) solid(from int) bool {
	for i := from; i < len(row.text); i++ {
		if !unicode.IsSpace(row.text[i]) && !row.dim[i] {
			return true
		}
	}
	return false
}

// idlePlaceholder reports whether the row, from character index from on, draws
// exactly placeholder the way an idle composer does: the first cell may be
// reverse video (the cursor sits on it), and every other cell is dim or in the
// foreground colour muted. muted is the colour of the composer's divider rows
// and "" when they have none, in which case only dim qualifies; a cell in the
// default foreground is never muted, so a typed draft never matches.
func (row nodeScreenRow) idlePlaceholder(from int, placeholder, muted string) bool {
	for from < len(row.text) && unicode.IsSpace(row.text[from]) {
		from++
	}
	end := len(row.text)
	for end > from && unicode.IsSpace(row.text[end-1]) {
		end--
	}
	if string(row.text[from:end]) != placeholder {
		return false
	}
	for i := from; i < end; i++ {
		if i == from && row.reverse[i] {
			continue
		}
		if row.reverse[i] || !(row.dim[i] || (muted != "" && row.fg[i] == muted)) {
			return false
		}
	}
	return true
}

// ruleForeground is the foreground colour of the row's first rule character
// ("─"), or "" when the row has none or draws it in the default foreground.
func (row nodeScreenRow) ruleForeground() string {
	for i, r := range row.text {
		if r == '─' {
			return row.fg[i]
		}
	}
	return ""
}
