package panewire

import (
	"bufio"
	"errors"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// envFileMaxBytes bounds a mode-0600 env file. A larger file is rejected
// rather than read up to the bound: a silently dropped tail can change what
// the file grants (a hub auth file losing a HUB_SCOPES_ line, for one).
const envFileMaxBytes = 1 << 20

// loadMode0600Env answers the file's keys and values; a key given twice
// keeps its last value. A file where a repeated key must be refused (the hub
// auth file) reads loadMode0600EnvEntries instead.
func loadMode0600Env(path string) (map[string]string, error) {
	entries, err := loadMode0600EnvEntries(path)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		values[entry.key] = entry.value
	}
	return values, nil
}

type envEntry struct {
	key, value string
}

// loadMode0600EnvEntries answers every assignment in file order, repeats
// included, after the same mode, size and syntax checks as loadMode0600Env.
func loadMode0600EnvEntries(path string) ([]envEntry, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return nil, errors.New("env file must be regular mode 0600")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open env file")
	}
	defer file.Close()
	var entries []envEntry
	limited := &io.LimitedReader{R: file, N: envFileMaxBytes + 1}
	scanner := bufio.NewScanner(limited)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return nil, errors.New("malformed env file")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			decoded, err := strconv.Unquote(value)
			if err != nil {
				return nil, errors.New("malformed quoted env value")
			}
			value = decoded
		}
		entries = append(entries, envEntry{key: key, value: value})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("read env file")
	}
	if limited.N == 0 {
		return nil, errors.New("env file is too large")
	}
	return entries, nil
}

var machineIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
