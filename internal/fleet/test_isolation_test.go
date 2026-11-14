package fleet

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// TestLedgerStaysUnderTestHome proves TestMain's isolation end to end (hk
// 4685): an App built by New resolves the job ledger under the temp home,
// never the launch home, and opening it writes only there. The arbiter
// guard panics if any Store opens under the launch home, so dropping the
// override turns this red.
func TestLedgerStaysUnderTestHome(t *testing.T) {
	a, err := New(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s := a.ledger()
	for name, path := range map[string]string{"state.db": s.DB, "jobs inbox": s.Inbox} {
		if testLaunchHome != "" && (path == testLaunchHome || strings.HasPrefix(path, testLaunchHome+string(os.PathSeparator))) {
			t.Fatalf("%s resolved under the launch home: %s", name, path)
		}
	}
	if _, err := s.Claim(context.Background(), arbiter.Claim{Job: "j", Lane: "l", T: "T1", AgentLabel: "j"}); err != nil {
		t.Fatal(err)
	}
}
