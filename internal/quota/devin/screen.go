package devin

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

var (
	ansiEscape     = regexp.MustCompile("\x1b(?:\\[[0-?]*[ -/]*[@-~]|\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|\\([0-2])")
	percentPattern = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*%\s*(left|used|remaining)?`)
	resetsIn       = regexp.MustCompile(`(?i)resets?\s+in\s+((?:[0-9]+(?:\.[0-9]+)?\s*(?:days?|hours?|minutes?|seconds?|weeks?|[dhmsw])\s*)+)`)
	remainingPanel = regexp.MustCompile(`(?i)(?:Pro|Ultra|Free)[^\n]*?([0-9]+(?:\.[0-9]+)?)%\s+remaining`)
)

// cleanScreen strips terminal escapes and carriage returns from probe output.
func cleanScreen(s string) string {
	return strings.ReplaceAll(ansiEscape.ReplaceAllString(s, ""), "\r", "")
}

// humanDuration reads "6d 2h", "2 hours 5 minutes" and the like.
func humanDuration(s string) (time.Duration, bool) {
	s = strings.ToLower(s)
	for _, p := range [][2]string{{"weeks", "w"}, {"week", "w"}, {"days", "d"}, {"day", "d"}, {"hours", "h"}, {"hour", "h"}, {"minutes", "m"}, {"minute", "m"}, {"seconds", "s"}, {"second", "s"}} {
		s = strings.ReplaceAll(s, p[0], p[1])
	}
	s = strings.Join(strings.Fields(s), "")
	var total time.Duration
	for s != "" {
		i := 0
		for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
			i++
		}
		if i == 0 || i == len(s) {
			return 0, false
		}
		n, err := strconv.ParseFloat(s[:i], 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1e6 {
			return 0, false
		}
		unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour, 'w': 168 * time.Hour}[s[i]]
		if unit == 0 {
			return 0, false
		}
		total += time.Duration(n * float64(unit))
		s = strings.TrimSpace(s[i+1:])
	}
	return total, true
}

// resetIn is the reset time of a "resets in 6d 2h" phrase, or "".
func resetIn(s string, now time.Time) string {
	m := resetsIn.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	d, ok := humanDuration(m[1])
	if !ok || d <= 0 {
		return ""
	}
	return now.Add(d).UTC().Format(time.RFC3339Nano)
}

func number(s string) float64 {
	n, _ := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
	return n
}

var errPanel = quota.Fail(quota.ErrKindParse, "usage panel incomplete or unrecognized")

// ParseScreen reads devin's `/usage` panel: each line naming a window (weekly
// or 7-day, 5h or 5-hour, daily, monthly) with a percentage is a bucket; a
// percentage is "% used", "% left" or "% remaining", and a bare one is used. A
// panel with no such line falls back to the plan line's "NN% remaining". Any
// bucket outside 0..100 fails the whole panel.
func ParseScreen(text string, now time.Time) (quota.Observation, error) {
	clean := cleanScreen(text)
	if strings.Contains(strings.ToLower(clean), "trust this folder") {
		return quota.Observation{}, errPanel
	}
	observed := quota.Observation{Source: quota.SourceCLI}
	for _, line := range strings.Split(clean, "\n") {
		lower := strings.ToLower(line)
		window := ""
		switch {
		case strings.Contains(lower, "weekly") || strings.Contains(lower, "7-day"):
			window = "7d"
		case strings.Contains(lower, "5h") || strings.Contains(lower, "5-hour"):
			window = "5h"
		case strings.Contains(lower, "daily"):
			window = "1d"
		case strings.Contains(lower, "monthly"):
			window = "30d"
		}
		if window == "" {
			continue
		}
		m := percentPattern.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		used := number(m[1])
		if strings.EqualFold(m[2], "left") || strings.EqualFold(m[2], "remaining") {
			used = 100 - used
		}
		observed.Buckets = append(observed.Buckets, accountBucket(window, quota.Pct(used), resetIn(line, now)))
	}
	if len(observed.Buckets) == 0 {
		if m := remainingPanel.FindStringSubmatch(clean); m != nil {
			observed.Buckets = []quota.Bucket{accountBucket("7d", quota.Pct(100-number(m[1])), resetIn(clean, now))}
		}
	}
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, errPanel
	}
	for _, b := range observed.Buckets {
		if !quota.ValidPct(b.UsedPct) {
			return quota.Observation{}, errPanel
		}
	}
	return observed, nil
}
