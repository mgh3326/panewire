package panewire

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// hk 4982: the node's single-screen observation recognises both spent-pool
// stops. The credit stop is recognised and left alone: the job watcher
// publishes it and the hub's credit trigger sweeps the pool.
func TestQuotaStopObservationReplaysT4982(t *testing.T) {
	kst := time.FixedZone("KST", 9*60*60)
	for _, tc := range []struct {
		name, fixture string
		decided       bool
	}{
		{"codex usage limit", "codex-usage-limit-2026-11-16.txt", true},
		{"omp anthropic credit", "omp-anthropic-credit-2026-11-17.txt", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, store, logs := quotaShadowFixture(t)
			screen, err := os.ReadFile("testdata/quota-stop/" + tc.fixture)
			if err != nil {
				t.Fatal(err)
			}
			q.read = func(context.Context, string) (string, error) { return string(screen), nil }
			now := time.Date(2026, 11, 16, 20, 25, 0, 0, kst)
			q.observeBounded(context.Background(), quotaShadowWake("wake-4982"), now)

			line := ""
			for _, l := range strings.Split(logs.String(), "\n") {
				if strings.Contains(l, `msg="quota-stop observation"`) {
					line = l
				}
			}
			if !strings.Contains(line, "outcome=quota") || !strings.Contains(line, "basis=classifier") {
				t.Fatalf("observation log line: %q\nall logs:\n%s", line, logs)
			}
			var decision string
			if err := store.db.QueryRow(`SELECT decision FROM quota_stop_decisions WHERE event_id='wake-4982'`).Scan(&decision); err != nil {
				t.Fatal(err)
			}
			if (decision != "") != tc.decided {
				t.Fatalf("decision = %q, want decided=%t", decision, tc.decided)
			}
		})
	}
}
