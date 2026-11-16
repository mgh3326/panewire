package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// An implementer's delivered brief tells it to ask for a consult by ESC with
// an hk packet, never through the hub or a model API (operator decision 313).
func TestSpawnBriefPreambleRoutesConsultByESCT4837(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(a.Jobs, s.Job, "brief-delivered.md"))
	if e != nil {
		t.Fatal(e)
	}
	brief := string(b)
	start := strings.Index(brief, "Host safety (fleet-v1):\n")
	end := strings.Index(brief, "\nLaunch identity:")
	if start < 0 || end < start {
		t.Fatalf("delivered brief has no fleet-v1 block: %q", brief)
	}
	block := brief[start:end]
	for _, want := range []string{
		"Need a consult (e.g. a review round's ladder step)? Send ESC --type decision to your owner lane with the packet as an hk doc key.",
		"Do not run `panewire consult request` and do not call any model API yourself.",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("fleet-v1 block lacks %q: %q", want, block)
		}
	}
}
