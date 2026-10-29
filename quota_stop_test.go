package panewire

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestClassifyQuotaStop(t *testing.T) {
	local := time.FixedZone("local", 9*60*60)
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		screen, pool string
		reset        time.Time
		choice       bool
	}{
		{"Reached free model rate limit … reset in 29 seconds (at 00:04 UTC)", "devin-free", now.Add(29 * time.Second), false},
		{"You've hit your weekly limit · resets Nov 1, 2am\nChoose an option: wait or upgrade", "claude", time.Date(2026, 10, 31, 17, 0, 0, 0, time.UTC), true},
		{"quota exceeded; try again at 12:39 PM", "codex", time.Date(2026, 10, 29, 3, 39, 0, 0, time.UTC), false},
		{"You've hit your session limit · resets unknown", "claude", time.Time{}, false},
		{"You've hit your usage limit. Please try again at unknown", "codex", time.Time{}, false},
	}
	for i, path := range []string{"testdata/quota-stop/devin.txt", "testdata/quota-stop/claude.txt", "testdata/quota-stop/codex.txt"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		cases[i].screen = string(b)
	}
	for _, tc := range cases {
		got, ok := classifyQuotaStop(tc.screen, now, local)
		if !ok || got.Pool != tc.pool || !got.Reset.Equal(tc.reset) || got.Choice != tc.choice {
			t.Errorf("%q: got %+v %t", tc.screen, got, ok)
		}
	}
	if _, ok := classifyQuotaStop("try running again tomorrow", now, local); ok {
		t.Fatal("unexpected classification")
	}
}

func TestQuotaPolicyAndDecision(t *testing.T) {
	doc := "# Policy\n\n## Values\n- default_expected: 1d (each task may set its own)\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue (resume)\"\n- Never choose paid options.\n\n## Other\n"
	p, ok := parseQuotaPolicy(doc)
	if !ok || p.Expected != 24*time.Hour || p.MaxWaits != 1 || !p.Urgent[1] || p.ResumeText != "continue (resume)" {
		t.Fatalf("policy=%+v ok=%v", p, ok)
	}
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	q := quotaStop{Reset: now.Add(time.Hour)}
	for _, tc := range []struct {
		priority, waits int
		stop            quotaStop
		want            string
	}{{1, 0, q, "handover"}, {3, 0, q, "wait"}, {3, 1, q, "handover"}, {3, 0, quotaStop{}, "handover"}, {3, 0, quotaStop{Reset: now.Add(25 * time.Hour)}, "handover"}, {3, 0, quotaStop{Reset: now.Add(time.Hour), Choice: true}, "handover"}} {
		got, _ := quotaDecision(p, true, tc.priority, 0, tc.waits, tc.stop, now)
		if got != tc.want {
			t.Errorf("case %+v: %s", tc, got)
		}
	}
	if _, ok := parseQuotaPolicy(strings.Replace(doc, "max_waits: 1", "max_waits: 2", 1)); ok {
		t.Fatal("unsafe policy accepted")
	}
}

func TestQuotaStopOverflowAndEmptyUrgency(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	stop, ok := classifyQuotaStop("Reached free model rate limit reset in 2000000000 hours", now, time.UTC)
	if !ok || !stop.Reset.IsZero() {
		t.Fatalf("overflow stop=%+v ok=%v", stop, ok)
	}
	doc := `## Values
- default_expected: 1d
- urgent_priorities: []
- max_waits: 1
- resume_text: "continue"
`
	p, ok := parseQuotaPolicy(doc)
	if !ok || len(p.Urgent) != 0 {
		t.Fatalf("policy=%+v ok=%v", p, ok)
	}
}

func TestQuotaStopLastActivityAndExplicitZone(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	for _, path := range []string{"testdata/quota-stop/code-negative.txt", "testdata/quota-stop/old-negative.txt"} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := classifyQuotaStop(string(b), now, time.UTC); ok {
			t.Errorf("%s classified %+v", path, got)
		}
	}
	text := "You've hit your weekly limit · resets Nov 1, 2am (Asia/Seoul)"
	got, ok := classifyQuotaStop(text, now, time.UTC)
	if !ok || !got.Reset.Equal(time.Date(2026, 10, 31, 17, 0, 0, 0, time.UTC)) {
		t.Fatalf("zone=%+v ok=%v", got, ok)
	}
	unknown, _ := classifyQuotaStop("You've hit your weekly limit · resets Nov 1, 2am (Mars/Olympus)", now, time.UTC)
	if !unknown.Reset.IsZero() {
		t.Fatalf("unknown zone=%v", unknown.Reset)
	}
}
