package fleet

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestReadPanewireConfigMissingExplicitDoesNotFallThrough(t *testing.T) {
	home := t.TempDir()
	legacy := filepath.Join(home, ".config", "pacelever", "config.toml")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("[pools.claude]\nclass='spend'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing.toml")
	getenv := func(name string) string {
		if name == "PACELEVER_CONFIG" {
			return missing
		}
		return ""
	}
	data, found, err := readPanewireConfig(getenv, home)
	if data != nil || found || err == nil || !strings.Contains(err.Error(), "PACELEVER_CONFIG") || !strings.Contains(err.Error(), missing) {
		t.Fatalf("missing explicit path = %q, found=%v, err=%v", data, found, err)
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("missing explicit error is unclear: %v", err)
	}
}
