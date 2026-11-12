package configpath

import (
	"reflect"
	"testing"
)

func TestCandidatesPreferPanewireAndRetainLegacyPaths(t *testing.T) {
	got := Candidates(func(string) string { return "" }, "/home/u", "config.toml")
	want := []string{
		"/home/u/.config/panewire/config.toml",
		"/home/u/.config/pacelever/config.toml",
		"/home/u/.config/scopefuel/config.toml",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Candidates = %v, want %v", got, want)
	}
}

func TestCandidatesHonorNewOverrideBeforeLegacyOverride(t *testing.T) {
	env := map[string]string{
		"PANEWIRE_CONFIG":  "/new/config.toml",
		"PACELEVER_CONFIG": "/old/config.toml",
		"XDG_CONFIG_HOME":  "/xdg",
	}
	got := Candidates(func(key string) string { return env[key] }, "/home/u", "config.toml")
	want := []string{"/new/config.toml", "/xdg/panewire/config.toml", "/old/config.toml", "/xdg/pacelever/config.toml", "/xdg/scopefuel/config.toml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Candidates = %v, want %v", got, want)
	}
}

func TestCacheCandidatesRenameDirectoryAndKeepExplicitLegacyFallback(t *testing.T) {
	env := map[string]string{
		"PANEWIRE_CACHE":  "/new/cache/records.json",
		"PACELEVER_CACHE": "/old/cache/records.json",
	}
	got := CacheCandidates(func(key string) string { return env[key] }, "/home/u", "devin-free-limit.json")
	want := []string{"/new/cache/devin-free-limit.json", "/old/cache/devin-free-limit.json"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CacheCandidates = %v, want %v", got, want)
	}
}
