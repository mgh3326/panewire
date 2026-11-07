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
func agyTrustPath(a *App) string {
	return filepath.Join(a.Home, ".gemini", "antigravity-cli", "settings.json")
}
func TestAgyTrustSeedsCWDAndKeepsOtherEntries(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := agyTrustPath(a)
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte(`{"theme":"dark","trustedWorkspaces":["/srv/other","/srv/parent"]}`), 0600); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "agy"}, nil); e != nil {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var data struct {
		Theme   string   `json:"theme"`
		Trusted []string `json:"trustedWorkspaces"`
	}
	if e = json.Unmarshal(b, &data); e != nil {
		t.Fatal(e)
	}
	want := []string{"/srv/other", "/srv/parent", cwd}
	if data.Theme != "dark" || strings.Join(data.Trusted, "\n") != strings.Join(want, "\n") {
		t.Fatalf("agy store = %s, want theme kept and trustedWorkspaces %v", b, want)
	}
}
func TestAgyTrustCreatesMissingStoreAndIgnoresTrustedParent(t *testing.T) {
	a := testApp(t)
	parent := filepath.Join(a.Home, "work")
	cwd := filepath.Join(parent, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := agyTrustPath(a)
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, []byte(`{"trustedWorkspaces":[`+strconvQuote(parent)+`]}`), 0600); e != nil {
		t.Fatal(e)
	}
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "agy"}, nil); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), strconvQuote(cwd)) {
		t.Fatalf("a trusted parent must not stand in for the cwd: %s", b)
	}
	if e := os.Remove(path); e != nil {
		t.Fatal(e)
	}
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "agy"}, nil); e != nil {
		t.Fatal(e)
	}
	if b, _ = os.ReadFile(path); !strings.Contains(string(b), strconvQuote(cwd)) {
		t.Fatalf("missing store not created: %s", b)
	}
}
func TestAgyTrustMalformedStoreIsRefused(t *testing.T) {
	a := testApp(t)
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	path := agyTrustPath(a)
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	for _, body := range []string{"broken", `{"trustedWorkspaces":"/srv/x"}`, `{"trustedWorkspaces":[7]}`} {
		if e := os.WriteFile(path, []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
		e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "agy"}, nil)
		if e == nil {
			t.Fatalf("%q: invalid store accepted", body)
		}
		if b, _ := os.ReadFile(path); string(b) != body {
			t.Fatalf("%q: store clobbered: %s", body, b)
		}
		if got := SpawnRefusalCode("workspace trust: "+e.Error(), exitConfig); got != "trust_store_malformed" {
			t.Fatalf("%q: refusal code %q, want trust_store_malformed", body, got)
		}
	}
}
func TestAgyTrustRefusesHomeAndSymlinkedStore(t *testing.T) {
	a := testApp(t)
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: a.Home}, Seat{Kind: "agy"}, nil); e == nil {
		t.Fatal("home trusted")
	}
	if _, e := os.Stat(agyTrustPath(a)); !os.IsNotExist(e) {
		t.Fatalf("store written for refused cwd: %v", e)
	}
	cwd := filepath.Join(a.Home, "tree")
	if e := os.MkdirAll(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(a.Home, "real.json")
	if e := os.WriteFile(target, []byte(`{}`), 0600); e != nil {
		t.Fatal(e)
	}
	path := agyTrustPath(a)
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(target, path); e != nil {
		t.Fatal(e)
	}
	if e := a.workspaceTrust(context.Background(), Spawn{CWD: cwd}, Seat{Kind: "agy"}, nil); e == nil {
		t.Fatal("symlinked store accepted")
	}
	if b, _ := os.ReadFile(target); string(b) != `{}` {
		t.Fatalf("symlink target written: %s", b)
	}
}
func strconvQuote(s string) string { b, _ := json.Marshal(s); return string(b) }
