package devin

import (
	"fmt"
	"testing"
	"time"
)

func resetAt(d time.Duration) string {
	return testNow.Add(d).UTC().Format(time.RFC3339Nano)
}

func windowPanel(planReset, dailyReset, weekly string) string {
	return "Pro · 45% remaining · resets in " + planReset + "\n" +
		"Daily: 10% used · resets in " + dailyReset + "\n" + weekly + "\n"
}

// The live 2026-11-12 panel: the Weekly line names its reset as a date,
// so the weekly bucket keeps its stable absolute reset date (hk 4330).
func TestScreenUsageLiveLabelsWindowsByWindowWords(t *testing.T) {
	o, err := ParseScreen(fixture(t, "screen_usage_live.txt"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{
		{"1d", 10, resetAt(4*time.Hour + 42*time.Minute)},
		{"7d", 55, "2026-11-15T08:00:00Z"},
	})
}

// A reset phrase is a duration, never a window name: "5h" inside "3d 5h",
// "15h" or "5h 26m" must not turn the Daily or plan line into a 5h bucket.
func TestScreenResetPhraseNeverNamesTheWindow(t *testing.T) {
	for _, test := range []struct {
		phrase string
		after  time.Duration
	}{
		{"3d 5h", 3*24*time.Hour + 5*time.Hour},
		{"1d 15h", 24*time.Hour + 15*time.Hour},
		{"2d 5h 4m", 2*24*time.Hour + 5*time.Hour + 4*time.Minute},
		{"5h 26m", 5*time.Hour + 26*time.Minute},
		{"15h", 15 * time.Hour},
		{"5d", 5 * 24 * time.Hour},
		{"6d 2h", 6*24*time.Hour + 2*time.Hour},
	} {
		t.Run(test.phrase, func(t *testing.T) {
			panel := windowPanel(test.phrase, test.phrase, "Weekly: 55% used · resets Nov 15, 8:00 AM (UTC)")
			o, err := ParseScreen(panel, testNow)
			if err != nil {
				t.Fatal(err)
			}
			checkBuckets(t, o.Buckets, []wantBucket{
				{"1d", 10, resetAt(test.after)},
				{"7d", 55, "2026-11-15T08:00:00Z"},
			})
		})
	}
	for hour := 0; hour < 24; hour++ {
		t.Run(fmt.Sprintf("1d %dh", hour), func(t *testing.T) {
			phrase := fmt.Sprintf("1d %dh", hour)
			panel := windowPanel(phrase, phrase, "Weekly: 55% used")
			o, err := ParseScreen(panel, testNow)
			if err != nil {
				t.Fatal(err)
			}
			checkBuckets(t, o.Buckets, []wantBucket{
				{"1d", 10, resetAt(24*time.Hour + time.Duration(hour)*time.Hour)},
				{"7d", 55, resetAt(24*time.Hour + time.Duration(hour)*time.Hour)},
			})
		})
	}
}

// A window the panel names is still a window: an explicit 5h line is a 5h
// bucket even when its own reset phrase also contains "5h".
func TestScreenNamedFiveHourWindowSurvives(t *testing.T) {
	panel := "5h: 20% used · resets in 5h 26m\n" +
		"Weekly: 55% used · resets in 6d 2h\n" +
		"Daily: 10% used · resets in 1d 15h\n"
	o, err := ParseScreen(panel, testNow)
	if err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{
		{"5h", 20, resetAt(5*time.Hour + 26*time.Minute)},
		{"7d", 55, resetAt(6*24*time.Hour + 2*time.Hour)},
		{"1d", 10, resetAt(24*time.Hour + 15*time.Hour)},
	})
}

// The plan line lends its reset to a resetless Weekly line only when the used
// shares agree; a Weekly line that names its own reset keeps it.
func TestScreenWeeklyResetRules(t *testing.T) {
	t.Run("own reset wins", func(t *testing.T) {
		panel := "Pro · 45% remaining · resets in 3d 5h\n" +
			"Weekly: 55% used · resets in 6d 2h\n"
		o, err := ParseScreen(panel, testNow)
		if err != nil {
			t.Fatal(err)
		}
		checkBuckets(t, o.Buckets, []wantBucket{{"7d", 55, resetAt(6*24*time.Hour + 2*time.Hour)}})
	})
	t.Run("own absolute reset date wins", func(t *testing.T) {
		panel := "Pro · 45% remaining · resets in 3d 5h\n" +
			"Weekly: 55% used · resets Nov 15, 8:00 AM (UTC)\n"
		o, err := ParseScreen(panel, testNow)
		if err != nil {
			t.Fatal(err)
		}
		checkBuckets(t, o.Buckets, []wantBucket{{"7d", 55, "2026-11-15T08:00:00Z"}})
	})
	t.Run("disagreeing plan leaves weekly resetless", func(t *testing.T) {
		panel := "Pro · 40% remaining · resets in 3d 5h\n" +
			"Weekly: 55% used\n"
		o, err := ParseScreen(panel, testNow)
		if err != nil {
			t.Fatal(err)
		}
		checkBuckets(t, o.Buckets, []wantBucket{{"7d", 55, ""}})
	})
	t.Run("identical redraws publish once", func(t *testing.T) {
		panel := "Pro · 45% remaining · resets in 3d 5h\n" +
			"Weekly: 55% used\n" +
			"Weekly: 55% used\n"
		o, err := ParseScreen(panel, testNow)
		if err != nil {
			t.Fatal(err)
		}
		checkBuckets(t, o.Buckets, []wantBucket{{"7d", 55, resetAt(3*24*time.Hour + 5*time.Hour)}})
	})
}
