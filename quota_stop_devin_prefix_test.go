package panewire

import (
	"os"
	"testing"
	"time"
)

// The live devin stop line is prefixed with "▲ Something went wrong — ", which
// the old anchored pattern never matched (hk 3302). The fixture is today's
// screen: the reported status line above devin's idle composer.
func TestClassifyQuotaStopDevinSomethingWentWrong(t *testing.T) {
	now := time.Date(2026, 11, 6, 10, 27, 0, 0, time.UTC)
	screen, err := os.ReadFile("testdata/quota-stop/devin-something-went-wrong.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, ok := classifyQuotaStop(string(screen), now, time.UTC)
	if !ok || got.Pool != "devin-free" || !got.Reset.Equal(now.Add(5*time.Minute)) || got.Choice {
		t.Fatalf("today's devin screen: got %+v %t", got, ok)
	}
	// The old bare form still matches.
	bare, err := os.ReadFile("testdata/quota-stop/devin.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := classifyQuotaStop(string(bare), now, time.UTC); !ok || got.Pool != "devin-free" {
		t.Fatalf("bare devin form: got %+v %t", got, ok)
	}
}

func TestClassifyQuotaStopDevinPrefixNeedsLastActivity(t *testing.T) {
	now := time.Date(2026, 11, 6, 10, 27, 0, 0, time.UTC)
	for name, screen := range map[string]string{
		"later output":    "▲ Something went wrong — Reached free model rate limit … reset in 5 minutes\nI retried and finished the task.\n",
		"other error":     "▲ Something went wrong — connection reset by peer\n❭ Ask Devin to build features, fix bugs, or work on your code\n",
		"no marker":       "▲ Something went wrong — Reached free model rate limits are documented in README\n",
		"quoted in prose": "see the docs: the model said Reached free model rate limit earlier\nand then I kept working\n",
	} {
		if got, ok := classifyQuotaStop(screen, now, time.UTC); ok {
			t.Errorf("%s classified %+v", name, got)
		}
	}
}
