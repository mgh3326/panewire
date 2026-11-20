package fleet

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// hk 5454: a node's owner info lane event names its job, so the hub decides it
// by that job's --owner-notify mode. Every such emit is built by ownerInfoEmitArgs.

func TestOwnerInfoEmitArgsCarryJobNotice_5454(t *testing.T) {
	got := ownerInfoEmitArgs("mergewake-design", "replaced-close-pw-x", "/inbox", "[panewire] closed pw-x", "pw-x", "replaced_close")
	want := []string{"emit", "--kind", "lane.event", "--lane", "mergewake-design", "--type", "info", "--event-id", "replaced-close-pw-x", "--inbox-root", "/inbox", "--text", "[panewire] closed pw-x", "--notice-job", "pw-x", "--notice-cause", "replaced_close"}
	if !slices.Equal(got, want) {
		t.Fatalf("emit args = %q, want %q", got, want)
	}
}

// An owner info emit that does not come from ownerInfoEmitArgs names no job, so
// the hub cannot decide it: the guard fails it.
func TestOwnerInfoEmitOnlyThroughHelper_5454(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		if bypassesOwnerInfoEmit(name, string(src)) {
			t.Fatalf("%s emits an info lane event outside ownerInfoEmitArgs", name)
		}
	}
}

// bypassesOwnerInfoEmit reports whether a source file other than owner_notify.go
// spells out an info emit: the mutant shape a new producer would take.
func bypassesOwnerInfoEmit(name, src string) bool {
	return name != "owner_notify.go" && strings.Contains(src, `"--type", "info"`)
}

func TestOwnerInfoEmitGuardFlagsMutant_5454(t *testing.T) {
	mutant := `r := a.call(ctx, "panewire", "emit", "--kind", "lane.event", "--lane", owner, "--type", "info")`
	if !bypassesOwnerInfoEmit("new_producer.go", mutant) {
		t.Fatal("a new producer that spells out --type info was not flagged")
	}
	if bypassesOwnerInfoEmit("owner_notify.go", mutant) {
		t.Fatal("the helper itself was flagged")
	}
}
