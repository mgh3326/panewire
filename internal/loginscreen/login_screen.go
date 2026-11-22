// Package loginscreen is the closed table of the screens each harness shows
// when it has no login, and the one matcher every reader of it goes through.
// The root package reads a spawn's saved capture with it (quota_login_probe.go),
// and internal/fleet reads each brief-submit poll with it (herdr.go confirmBrief),
// so both agree on what a login screen is. See docs/fleet-state.md.
package loginscreen

import "strings"

// screens is the closed table of the screens each harness shows when it has no
// login. A screen is one row: every line of a row must appear on the capture,
// and a capture is a login screen when any one row matches in full. A harness
// has a row only from a recorded capture of its login screen: claude and codex
// have none yet, so a screen of theirs is never read as a login screen. agy:
// jobs/at-3816-retro-read-views/spawn-failure-*.txt on mac-dev, 2026-11-10 (the
// first row); the 2026-11-20 mac-dev capture of the newer sign-in screen (the
// second row). See docs/fleet-state.md.
var screens = map[string][][]string{
	"agy": {
		{"Open the URL below in your browser:", "After authenticating, copy the code displayed in the browser and paste it below:"},
		{"Copy and paste the URL or click on the link below:", "paste the authorization code below:"},
	},
}

// Matches reports whether screen is kind's login screen: some row of the table
// for kind has every one of its lines on the screen. Every reader of the table
// goes through this function.
func Matches(kind, screen string) bool {
	for _, lines := range screens[kind] {
		if len(lines) == 0 {
			continue
		}
		if hasAllLines(screen, lines) {
			return true
		}
	}
	return false
}

// hasAllLines reports whether every line appears on screen.
func hasAllLines(screen string, lines []string) bool {
	for _, line := range lines {
		if !strings.Contains(screen, line) {
			return false
		}
	}
	return true
}
