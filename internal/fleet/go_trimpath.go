package fleet

import (
	"os"
	"path/filepath"
	"strings"
)

// trimpathFlag is the go flag that keeps absolute source paths out of a
// package's action id and its built binary (hk 5160).
const trimpathFlag = "-trimpath"

// goFlagsForPane returns the GOFLAGS a spawned pane takes: base with
// trimpathFlag appended unless base already turns it on, so an existing flag is
// kept, never replaced.
//
// The flag belongs in every fleet pane because each session works in its own
// worktree directory: without it the main module's directory is part of the
// action id, so two worktrees of one commit share no cache entry.
func goFlagsForPane(base string) string {
	if trimpathSet(base) {
		return base
	}
	return strings.TrimSpace(base + " " + trimpathFlag)
}

// trimpathSet reports whether the last trimpath occurrence in flags turns it
// on. The go command applies the flags in order, so "-trimpath -trimpath=false"
// leaves it off and needs the appended flag.
func trimpathSet(flags string) bool {
	set := false
	for _, f := range goflagsFields(flags) {
		switch f {
		case trimpathFlag, trimpathFlag + "=true":
			set = true
		case trimpathFlag + "=false":
			set = false
		}
	}
	return set
}

// goflagsFields splits a GOFLAGS value into flags. Text inside single or double
// quotes is one word, as the go command parses it, so a "-trimpath" inside a
// quoted -ldflags value is not a flag of its own. A backslash escapes the next
// byte inside double quotes.
func goflagsFields(s string) []string {
	var fields []string
	for i := 0; i < len(s); {
		for i < len(s) && isGOFLAGSSpace(s[i]) {
			i++
		}
		if i >= len(s) {
			break
		}
		var word strings.Builder
		quote := byte(0)
		for ; i < len(s); i++ {
			c := s[i]
			if quote == 0 && isGOFLAGSSpace(c) {
				break
			}
			if quote == 0 && (c == '\'' || c == '"') {
				quote = c
				continue
			}
			if quote != 0 && c == quote {
				quote = 0
				continue
			}
			if quote == '"' && c == '\\' && i+1 < len(s) {
				i++
				word.WriteByte(s[i])
				continue
			}
			word.WriteByte(c)
		}
		fields = append(fields, word.String())
	}
	return fields
}

func isGOFLAGSSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// effectiveGOFLAGS is the GOFLAGS the spawner's own go command resolves: an
// exported value wins, otherwise the one saved by `go env -w`. Passing only the
// exported value would make a nonempty pane value hide the saved one
// (cmd/go/internal/cfg.Getenv), so a host's saved flags would stop applying.
func effectiveGOFLAGS() string {
	if v := os.Getenv("GOFLAGS"); v != "" {
		return v
	}
	return goenvGOFLAGS()
}

// goenvGOFLAGS reads GOFLAGS from the go environment file, whose location and
// format are the go command's: GOENV, else os.UserConfigDir()/go/env; one
// KEY=VALUE per line, the last assignment winning.
func goenvGOFLAGS() string {
	file := os.Getenv("GOENV")
	if file == "off" {
		return ""
	}
	if file == "" {
		dir, err := os.UserConfigDir()
		if err != nil {
			return ""
		}
		file = filepath.Join(dir, "go", "env")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return ""
	}
	value := ""
	for _, line := range strings.Split(string(data), "\n") {
		if key, v, ok := strings.Cut(line, "="); ok && key == "GOFLAGS" {
			value = v
		}
	}
	return value
}
