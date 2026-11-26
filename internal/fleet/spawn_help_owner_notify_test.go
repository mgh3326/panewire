package fleet

import (
	"bytes"
	"strings"
	"testing"
)

func TestSpawnHelpListsOwnerNotifyFlag(t *testing.T) {
	var out bytes.Buffer
	a := &App{Out: &out}
	if err := a.help("spawn"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "[--owner-notify final|never]") {
		t.Fatalf("spawn help omits owner notify syntax: %q", out.String())
	}
}
