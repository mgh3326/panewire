package claude

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

var (
	ansiEscape         = regexp.MustCompile("\x1b(?:\\[[0-?]*[ -/]*[@-~]|\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|\\([0-2])")
	usagePercent       = regexp.MustCompile(`(?i)([0-9]{1,3}(?:\.[0-9]+)?)\s*%\s*(used|left)`)
	usageOnly          = regexp.MustCompile(`(?i)\s+only$`)
	usageReset         = regexp.MustCompile(`(?i)\bresets?\s+([A-Za-z]{3,9})\.?\s+([0-9]{1,2})\s+at\s+([0-9]{1,2})(?::([0-9]{2}))?\s*([ap])\.?m\.?\s*\(([A-Za-z0-9_+\-/]+)\)`)
	usageMonths        = map[string]time.Month{"jan": time.January, "feb": time.February, "mar": time.March, "apr": time.April, "may": time.May, "jun": time.June, "jul": time.July, "aug": time.August, "sep": time.September, "oct": time.October, "nov": time.November, "dec": time.December}
	errPanelIncomplete = quota.Fail(quota.ErrKindParse, "usage panel incomplete or unrecognized")
)

// ParseUsageScreen parses Claude Code's print-mode /usage output: "current
// session" is the 5h bucket, "current week (all models)" the 7d bucket and any
// other "current week (NAME)" a model bucket. A percentage is "% used" or
// "% left". Both account windows and their reset times must be present.
//
// The label-proximity and "% left" rules are ported from pacelever, itself
// ported from ClaudeBar's claude-usage-screen.js (MIT).
func ParseUsageScreen(text string, now time.Time) (quota.Observation, error) {
	cleaned := strings.ReplaceAll(ansiEscape.ReplaceAllString(text, ""), "\r", "")
	lines := strings.Split(cleaned, "\n")
	session, sessionReset, ok := usageValue(lines, "current session", now)
	if !ok {
		return quota.Observation{}, errPanelIncomplete
	}
	weekly, weeklyReset, ok := usageValue(lines, "current week (all models)", now)
	if !ok {
		return quota.Observation{}, errPanelIncomplete
	}
	observed := quota.Observation{Source: quota.SourceCLI, Buckets: []quota.Bucket{
		accountBucket("5h", quota.Pct(session), sessionReset),
		accountBucket("7d", quota.Pct(weekly), weeklyReset),
	}}
	seen := map[string]bool{}
	for i, line := range lines {
		lower := strings.ToLower(line)
		start := strings.Index(lower, "current week (")
		if start < 0 {
			continue
		}
		name := line[start+len("current week ("):]
		end := strings.Index(name, ")")
		if end < 0 {
			continue
		}
		name = strings.TrimSpace(name[:end])
		if name == "" || strings.EqualFold(name, "all models") {
			continue
		}
		// Claude reports "Sonnet only" as the Sonnet model limit, not a new model.
		name = usageOnly.ReplaceAllString(name, "")
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		used, ok := percentNear(lines, i)
		if !ok {
			continue
		}
		reset, ok := resetNear(lines, i, now)
		if !ok {
			reset = weeklyReset
		}
		seen[key] = true
		observed.Buckets = append(observed.Buckets, modelBucket(name, "7d", quota.Pct(used), reset))
	}
	return observed, nil
}

func usageValue(lines []string, label string, now time.Time) (float64, string, bool) {
	for i, line := range lines {
		if !strings.Contains(strings.ToLower(line), label) {
			continue
		}
		used, ok := percentNear(lines, i)
		if !ok {
			return 0, "", false
		}
		reset, ok := resetNear(lines, i, now)
		if !ok {
			return 0, "", false
		}
		return used, reset, true
	}
	return 0, "", false
}

func percentNear(lines []string, start int) (float64, bool) {
	for i := start; i < len(lines) && i < start+12; i++ {
		m := usagePercent.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		v, err := strconv.ParseFloat(m[1], 64)
		if err != nil || v < 0 || v > 100 {
			return 0, false
		}
		if strings.EqualFold(m[2], "left") {
			v = 100 - v
		}
		return v, true
	}
	return 0, false
}

func resetNear(lines []string, start int, now time.Time) (string, bool) {
	for i := start; i < len(lines) && i < start+14; i++ {
		m := usageReset.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		return parseReset(m, now)
	}
	return "", false
}

// parseReset turns "Nov 1 at 2am (Asia/Seoul)" into the next such instant
// after now, in UTC.
func parseReset(m []string, now time.Time) (string, bool) {
	// m[1] is 3-9 ASCII letters (usageReset), so the three-byte cut is safe.
	month, ok := usageMonths[strings.ToLower(m[1])[:3]]
	if !ok {
		return "", false
	}
	day, dayErr := strconv.Atoi(m[2])
	hour, hourErr := strconv.Atoi(m[3])
	minute := 0
	if m[4] != "" {
		var err error
		if minute, err = strconv.Atoi(m[4]); err != nil {
			return "", false
		}
	}
	if dayErr != nil || hourErr != nil || day < 1 || day > 31 || hour < 1 || hour > 12 || minute > 59 {
		return "", false
	}
	if strings.EqualFold(m[5], "p") && hour != 12 {
		hour += 12
	}
	if strings.EqualFold(m[5], "a") && hour == 12 {
		hour = 0
	}
	loc, err := time.LoadLocation(m[6])
	if err != nil {
		return "", false
	}
	localNow := now.In(loc)
	reset := time.Date(localNow.Year(), month, day, hour, minute, 0, 0, loc)
	if !reset.After(now) {
		reset = reset.AddDate(1, 0, 0)
	}
	return reset.UTC().Format(time.RFC3339Nano), true
}
