// Package configpath resolves panewire paths while accepting the retired
// pacelever location during the migration.
package configpath

import (
	"os"
	"path/filepath"
)

// Candidates returns the explicit panewire override, the panewire XDG path,
// then legacy explicit and XDG paths. Empty and duplicate paths are omitted.
func Candidates(getenv func(string) string, home, name string) []string {
	var out []string
	add := func(path string) {
		if path == "" {
			return
		}
		for _, existing := range out {
			if existing == path {
				return
			}
		}
		out = append(out, path)
	}
	add(getenv("PANEWIRE_CONFIG"))
	add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "panewire", name))
	add(getenv("PACELEVER_CONFIG"))
	add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "pacelever", name))
	add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "scopefuel", name))
	return out
}

// CacheCandidates returns the new cache path followed by the pacelever path.
func CacheCandidates(getenv func(string) string, home, name string) []string {
	var out []string
	add := func(path string) {
		if path == "" {
			return
		}
		for _, existing := range out {
			if existing == path {
				return
			}
		}
		out = append(out, path)
	}
	base := func() string {
		if value := getenv("XDG_CACHE_HOME"); value != "" {
			return value
		}
		return filepath.Join(home, ".cache")
	}
	newPath := getenv("PANEWIRE_CACHE")
	if newPath == "" {
		newPath = filepath.Join(base(), "panewire", name)
	} else {
		newPath = filepath.Join(filepath.Dir(newPath), name)
	}
	add(newPath)
	legacy := getenv("PACELEVER_CACHE")
	if legacy != "" {
		add(filepath.Join(filepath.Dir(legacy), name))
	} else {
		add(filepath.Join(base(), "pacelever", name))
	}
	return out
}

func base(getenv func(string) string, variable, home, fallback string) string {
	if value := getenv(variable); value != "" {
		return value
	}
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, fallback)
}
