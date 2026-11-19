package panewire

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// MySQL assigns a SET list left to right, so an expression sees the columns an
// earlier assignment of the same statement already set; Postgres and SQLite
// read the old row for every expression. A store UPDATE whose later assignment
// reads an earlier-assigned column would run on the dialects with different
// results. Every such pair is either ordered so the answers agree, and listed
// here with the reason, or not written.
func TestHubUpdatesReadNoEarlierAssignment(t *testing.T) {
	// allowed pairs are "file:later<-earlier". Each has a proof in the code it
	// names.
	allowed := map[string]bool{
		// hub_spawn_park.go parkSpawn: next_check_ms is set first from the old
		// row; park_reason then reads it and gets the same answer as the old
		// row would (a kept wake is unchanged, a replaced one equals next, which
		// next_check_ms>next rejects).
		"hub_spawn_park.go:park_reason<-next_check_ms": true,
	}
	literal := regexp.MustCompile("(?s)`(UPDATE [^`]*)`((?:\\s*\\+\\s*[^`\\n]*?\\s*\\+\\s*`[^`]*`)*)")
	splice := regexp.MustCompile("(?s)\\+\\s*[^`]*?\\s*\\+\\s*`([^`]*)`")
	update := regexp.MustCompile(`(?is)^UPDATE\s+(\w+)\s+SET\s+(.*?)(\s+WHERE\s+.*)?$`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	scanned, used := 0, map[string]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range literal.FindAllStringSubmatch(string(raw), -1) {
			// A spliced-in expression (a dialect call) stands for one
			// placeholder: it reads no column.
			statement := m[1]
			for _, part := range splice.FindAllStringSubmatch(m[2], -1) {
				statement += "?" + part[1]
			}
			parsed := update.FindStringSubmatch(statement)
			if parsed == nil {
				continue
			}
			scanned++
			var assigned []string
			for _, item := range splitTopLevel(parsed[2]) {
				column, expression, ok := strings.Cut(item, "=")
				if !ok {
					continue
				}
				column = strings.TrimSpace(column)
				for _, earlier := range assigned {
					if !regexp.MustCompile(`\b` + regexp.QuoteMeta(earlier) + `\b`).MatchString(expression) {
						continue
					}
					key := file + ":" + column + "<-" + earlier
					if allowed[key] {
						used[key] = true
						continue
					}
					t.Errorf("%s: %s reads %s, which an earlier assignment of the same UPDATE sets; MySQL would read the new value. Order the list so each column is read before it is set:\n\t%s", file, column, earlier, statement)
				}
				assigned = append(assigned, column)
			}
		}
	}
	if scanned < 30 {
		t.Fatalf("scanned %d UPDATE statements; the scan no longer finds the store's", scanned)
	}
	for key := range allowed {
		if !used[key] {
			t.Errorf("%s is allowed but no UPDATE has it any more: delete the entry", key)
		}
	}
}
