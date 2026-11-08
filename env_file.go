package panewire

import (
	"regexp"

	"github.com/mgh3326/panewire/internal/envfile"
)

// envFileMaxBytes bounds a mode-0600 env file; see internal/envfile.
const envFileMaxBytes = envfile.MaxBytes

// loadMode0600Env answers the file's keys and values; a key given twice
// keeps its last value. A file where a repeated key must be refused (the hub
// auth file) reads loadMode0600EnvEntries instead. The one implementation is
// internal/envfile, shared with internal packages.
func loadMode0600Env(path string) (map[string]string, error) { return envfile.Load(path) }

type envEntry struct {
	key, value string
}

// loadMode0600EnvEntries answers every assignment in file order, repeats
// included, after the same mode, size and syntax checks as loadMode0600Env.
func loadMode0600EnvEntries(path string) ([]envEntry, error) {
	entries, err := envfile.Entries(path)
	if err != nil {
		return nil, err
	}
	out := make([]envEntry, len(entries))
	for i, entry := range entries {
		out[i] = envEntry{key: entry.Key, value: entry.Value}
	}
	return out, nil
}

var machineIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
