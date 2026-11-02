package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkspaceTrustMergesWithoutClobber(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(a.Home, ".claude.json")
	if e := os.WriteFile(path, []byte(`{"unrelated":42,"projects":{"other":{"hasTrustDialogAccepted":true}}}`), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "claude"}, nil); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var data map[string]any
	if e = json.Unmarshal(b, &data); e != nil {
		t.Fatal(e)
	}
	projects := data["projects"].(map[string]any)
	if data["unrelated"] != float64(42) || projects["other"] == nil || projects[cwd] == nil {
		t.Fatal("trust store clobbered")
	}
}
func TestCodexTrustPreservesCommentsAndExistingProject(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(a.Home, ".codex", "config.toml")
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	original := "# keep operator comment\nmodel = 'operator-model'\n"
	if e := os.WriteFile(path, []byte(original), 0600); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "codex"}, nil); e != nil {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(string(b), original) || strings.Count(string(b), "trust_level") != 1 {
		t.Fatal("Codex config lost content or duplicate project")
	}
}
func TestMalformedTrustStoreIsNotOverwritten(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(a.Home, ".claude.json")
	if e := os.WriteFile(path, []byte("broken"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "claude"}, nil); e == nil {
		t.Fatal("invalid store overwritten")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "broken" {
		t.Fatal("store clobbered")
	}
}
