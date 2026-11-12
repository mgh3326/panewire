package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPanewireConfigPrefersNewPathAndFallsBackToPacelever(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, ".config")
	newPath := filepath.Join(configDir, "panewire", "config.toml")
	oldPath := filepath.Join(configDir, "pacelever", "config.toml")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldPath, []byte("[pools.claude]\nclass='preserve'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := func(string) string { return "" }
	data, found, err := readPanewireConfig(getenv, root)
	if err != nil || !found || string(data) != "[pools.claude]\nclass='preserve'\n" {
		t.Fatalf("legacy fallback = %q, found=%v err=%v", data, found, err)
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("[pools.claude]\nclass='spend'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, found, err = readPanewireConfig(getenv, root)
	if err != nil || !found || string(data) != "[pools.claude]\nclass='spend'\n" {
		t.Fatalf("new path precedence = %q, found=%v err=%v", data, found, err)
	}
}

func TestReadPanewireConfigUsesNewExplicitOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "explicit.toml")
	if err := os.WriteFile(path, []byte("[pools.devin]\nclass='spend'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := func(name string) string {
		if name == "PANEWIRE_CONFIG" {
			return path
		}
		return ""
	}
	data, found, err := readPanewireConfig(getenv, t.TempDir())
	if err != nil || !found || string(data) != "[pools.devin]\nclass='spend'\n" {
		t.Fatalf("explicit config = %q, found=%v err=%v", data, found, err)
	}
}
