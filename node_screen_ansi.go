package panewire

import (
	"strconv"
	"strings"
	"unicode"
)

// nodeScreenRow is one screen row read with `herdr pane read --format ansi`:
// its visible characters, and for each one whether SGR 2 (dim) was in effect
// when it was drawn.
type nodeScreenRow struct {
	text []rune
	dim  []bool
}

// parseNodeScreen splits an ANSI screen into rows. SGR state carries across
// rows, because a renderer need not reset at a line break. Every escape
// sequence is dropped from the text, so a plain-text screen parses to itself
// with no dim character, and so does a screen from a herdr that ignores the
// format. A sequence cut off at the end of the screen is dropped.
func parseNodeScreen(raw string) []nodeScreenRow {
	rows := []nodeScreenRow{{}}
	dim := false
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
					dim = nodeApplySGR(string(runes[start:i]), dim)
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
			row.dim = append(row.dim, dim)
		}
	}
	return rows
}

// nodeApplySGR returns the dim state after one SGR parameter list. Only 2
// turns dim on and only 0 and 22 turn it off. The extended colour forms
// (38/48/58 with ;5;n or ;2;r;g;b) carry numbers that are not SGR codes, so a
// "2" inside one is not dim.
func nodeApplySGR(params string, dim bool) bool {
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
		switch code {
		case 0, 22:
			dim = false
		case 2:
			dim = true
		case 38, 48, 58:
			if i+1 < len(fields) {
				switch fields[i+1] {
				case "5":
					i += 2
				case "2":
					i += 4
				}
			}
		}
	}
	return dim
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
