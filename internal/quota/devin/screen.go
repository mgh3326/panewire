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
	resetsDate     = regexp.MustCompile(`(?i)\bresets?\s+([A-Za-z]{3,9})\.?\s+([0-9]{1,2})(?:,?\s+([0-9]{4}))?(?:,?\s+at|,)?\s+([0-9]{1,2})(?::([0-9]{2}))?(?::([0-9]{2}))?\s*([ap]\.?m\.?)?(?:\s*(?:\(([A-Za-z0-9_+\-/]+)\)|([A-Za-z0-9_+\-/]+)))?`)
	remainingPanel = regexp.MustCompile(`(?i)(?:Pro|Ultra|Free)[^\n]*?([0-9]+(?:\.[0-9]+)?)%\s+remaining`)
	windowWeekly   = regexp.MustCompile(`(?i)\b(?:weekly|7-day)\b`)
	windowFiveHour = regexp.MustCompile(`(?i)\b(?:5h|5-hour)\b`)
	windowDaily    = regexp.MustCompile(`(?i)\bdaily\b`)
	windowMonthly  = regexp.MustCompile(`(?i)\bmonthly\b`)

	months = map[string]time.Month{
		"jan": time.January, "feb": time.February, "mar": time.March, "apr": time.April,
		"may": time.May, "jun": time.June, "jul": time.July, "aug": time.August,
		"sep": time.September, "oct": time.October, "nov": time.November, "dec": time.December,
	}
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

// resetDate parses an absolute reset date phrase like "resets Nov 15, 8:00 AM (UTC)"
// or "resets Nov 15 at 8am (Asia/Seoul)", returning RFC3339Nano UTC or "".
func resetDate(s string, now time.Time) string {
	m := resetsDate.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	month, ok := months[strings.ToLower(m[1])[:3]]
	if !ok {
		return ""
	}
	day, err := strconv.Atoi(m[2])
	if err != nil || day < 1 || day > 31 {
		return ""
	}
	hour, err := strconv.Atoi(m[4])
	if err != nil {
		return ""
	}
	minute := 0
	if m[5] != "" {
		if minute, err = strconv.Atoi(m[5]); err != nil || minute < 0 || minute > 59 {
			return ""
		}
	}
	sec := 0
	if m[6] != "" {
		if sec, err = strconv.Atoi(m[6]); err != nil || sec < 0 || sec > 59 {
			return ""
		}
	}
	ampm := strings.ToLower(strings.ReplaceAll(m[7], ".", ""))
	if ampm == "pm" {
		if hour < 12 {
			hour += 12
		}
	} else if ampm == "am" {
		if hour == 12 {
			hour = 0
		}
	}
	if hour < 0 || hour > 23 {
		return ""
	}
	zoneStr := m[8]
	if zoneStr == "" {
		zoneStr = m[9]
	}
	var loc *time.Location
	switch strings.ToUpper(strings.TrimSpace(zoneStr)) {
	case "", "UTC", "GMT", "Z":
		loc = time.UTC
	default:
		var lErr error
		loc, lErr = time.LoadLocation(zoneStr)
		if lErr != nil {
			return ""
		}
	}
	var reset time.Time
	if m[3] != "" {
		year, yErr := strconv.Atoi(m[3])
		if yErr != nil || year < 2000 || year > 2100 {
			return ""
		}
		reset = time.Date(year, month, day, hour, minute, sec, 0, loc)
	} else {
		localNow := now.In(loc)
		reset = time.Date(localNow.Year(), month, day, hour, minute, sec, 0, loc)
		if !reset.After(now) {
			reset = reset.AddDate(1, 0, 0)
		}
	}
	return reset.UTC().Format(time.RFC3339Nano)
}

func parseReset(s string, now time.Time) string {
	if r := resetDate(s, now); r != "" {
		return r
	}
	return resetIn(s, now)
}

func number(s string) float64 {
	n, _ := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
	return n
}

var errPanel = quota.Fail(quota.ErrKindParse, "usage panel incomplete or unrecognized")

type screenBucket struct {
	window string
	used   float64
	reset  string
}

type screenPlan struct {
	used  float64
	reset string
}

// screenWindow names the window a line reports. The "resets in …" and "resets <date>"
// phrases are removed first so tokens like "5h" inside "3d 5h" never count as a window.
func screenWindow(line string) string {
	probe := resetsIn.ReplaceAllString(line, " ")
	probe = resetsDate.ReplaceAllString(probe, " ")
	switch {
	case windowWeekly.MatchString(probe):
		return "7d"
	case windowFiveHour.MatchString(probe):
		return "5h"
	case windowDaily.MatchString(probe):
		return "1d"
	case windowMonthly.MatchString(probe):
		return "30d"
	}
	return ""
}

func sameShare(a, b float64) bool { return math.Abs(a-b) <= 0.000001 }

// ParseScreen reads devin's `/usage` panel: each line naming a window (weekly
// or 7-day, 5h or 5-hour, daily, monthly) with a percentage is a bucket; a
// percentage is "% used", "% left" or "% remaining", and a bare one is used. A
// panel with no such line falls back to the plan line's "NN% remaining", which
// is the weekly account window. A Weekly line with no reset of its own takes
// the first plan line's reset when both report the same used share; identical
// buckets from redraws are published once. Any bucket outside 0..100 fails the
// whole panel.
func ParseScreen(text string, now time.Time) (quota.Observation, error) {
	clean := cleanScreen(text)
	if strings.Contains(strings.ToLower(clean), "trust this folder") {
		return quota.Observation{}, errPanel
	}
	observed := quota.Observation{Source: quota.SourceCLI}
	var buckets []screenBucket
	var plans []screenPlan
	for _, line := range strings.Split(clean, "\n") {
		if m := remainingPanel.FindStringSubmatch(line); m != nil {
			plans = append(plans, screenPlan{used: 100 - number(m[1]), reset: parseReset(line, now)})
		}
		window := screenWindow(line)
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
		buckets = append(buckets, screenBucket{window: window, used: used, reset: parseReset(line, now)})
	}
	if len(buckets) == 0 {
		if len(plans) > 0 {
			buckets = []screenBucket{{window: "7d", used: plans[0].used, reset: plans[0].reset}}
		}
	} else {
		for i := range buckets {
			if buckets[i].window != "7d" || buckets[i].reset != "" {
				continue
			}
			for _, plan := range plans {
				if plan.reset != "" && sameShare(plan.used, buckets[i].used) {
					buckets[i].reset = plan.reset
					break
				}
			}
		}
	}
	seen := map[screenBucket]struct{}{}
	for _, b := range buckets {
		if _, ok := seen[b]; ok {
			continue
		}
		seen[b] = struct{}{}
		bucket := accountBucket(b.window, quota.Pct(b.used), b.reset)
		if !quota.ValidPct(bucket.UsedPct) {
			return quota.Observation{}, errPanel
		}
		observed.Buckets = append(observed.Buckets, bucket)
	}
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, errPanel
	}
	return observed, nil
}
