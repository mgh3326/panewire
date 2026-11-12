// Package configpath resolves panewire paths while accepting the retired
// pacelever location during the migration.
package configpath

import (
	"os"
	"path/filepath"
)

// Candidates returns an explicit config override, or the implicit panewire
// and legacy pacelever XDG paths. Explicit overrides never fall through.
func Candidates(getenv func(string) string, home, name string) []string {
	if path, _, ok := Explicit(getenv); ok {
		return []string{path}
	}
	return implicitCandidates(getenv, home, name, false)
}

// AdmissionCandidates retains admission's historical implicit scopefuel path.
// Explicit overrides remain exclusive, as they are for every config reader.
func AdmissionCandidates(getenv func(string) string, home, name string) []string {
	if path, _, ok := Explicit(getenv); ok {
		return []string{path}
	}
	return implicitCandidates(getenv, home, name, true)
}

// Explicit returns the highest priority explicit config path, its variable,
// and whether either supported override was set.
func Explicit(getenv func(string) string) (path, variable string, ok bool) {
	if path = getenv("PANEWIRE_CONFIG"); path != "" {
		return path, "PANEWIRE_CONFIG", true
	}
	if path = getenv("PACELEVER_CONFIG"); path != "" {
		return path, "PACELEVER_CONFIG", true
	}
	return "", "", false
}

func implicitCandidates(getenv func(string) string, home, name string, scopefuel bool) []string {
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
	add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "panewire", name))
	add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "pacelever", name))
	if scopefuel {
		add(filepath.Join(base(getenv, "XDG_CONFIG_HOME", home, ".config"), "scopefuel", name))
	}
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
	if legacy == "" {
		add(filepath.Join(base(), "pacelever", name))
	} else {
		add(filepath.Join(filepath.Dir(legacy), name))
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
