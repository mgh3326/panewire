package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

var hubCheckNamePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)

// HubCheckStatus is the closed result vocabulary that may leave a node.  The
// command, its arguments, and its output always remain local.
type HubCheckStatus string

const (
	HubCheckOK   HubCheckStatus = "ok"
	HubCheckFail HubCheckStatus = "fail"
)

func (status HubCheckStatus) valid() bool {
	return status == HubCheckOK || status == HubCheckFail
}

// HubCheck describes one local command.  Its argv is deliberately never
// serialized into a hub event or a notification.
type HubCheck struct {
	Name    string
	Argv    []string
	Timeout time.Duration
}

type hubChecksConfig struct {
	Checks []hubRawCheck `json:"checks"`
}

type hubRawCheck struct {
	Name    string   `json:"name"`
	Argv    []string `json:"argv"`
	Timeout string   `json:"timeout"`
}

// LoadHubChecksConfig accepts one explicit regular JSON file.  Check commands
// can contain local details, so the path is never inferred and symlinks are
// rejected before opening it.
func LoadHubChecksConfig(path string) ([]HubCheck, error) {
	if path == "" {
		return nil, errors.New("checks config path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("checks config must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("read checks config")
	}
	return ParseHubChecksConfig(data)
}

// ParseHubChecksConfig validates the intentionally small local-only schema:
// {"checks":[{"name":"...","argv":["..."],"timeout":"10s"}]}.
func ParseHubChecksConfig(data []byte) ([]HubCheck, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("checks config JSON is invalid")
	}
	var raw hubChecksConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return nil, errors.New("checks config JSON is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("checks config JSON has trailing data")
	}
	checks, err := parseHubChecks(raw.Checks)
	if err != nil {
		return nil, err
	}
	return checks, nil
}

func parseHubChecks(raw []hubRawCheck) ([]HubCheck, error) {
	checks := make([]HubCheck, 0, len(raw))
	names := make(map[string]struct{}, len(raw))
	for _, check := range raw {
		if !hubCheckNamePattern.MatchString(check.Name) {
			return nil, errors.New("check name is invalid")
		}
		if _, exists := names[check.Name]; exists {
			return nil, errors.New("check names must be unique")
		}
		names[check.Name] = struct{}{}
		if len(check.Argv) == 0 || len(check.Argv) > 32 {
			return nil, errors.New("check argv is invalid")
		}
		argv := make([]string, len(check.Argv))
		for index, argument := range check.Argv {
			if argument == "" || len(argument) > 1024 || strings.IndexByte(argument, 0) >= 0 {
				return nil, errors.New("check argv is invalid")
			}
			argv[index] = argument
		}
		timeout := 10 * time.Second
		if check.Timeout != "" {
			parsed, err := time.ParseDuration(check.Timeout)
			if err != nil || parsed < time.Millisecond || parsed > 5*time.Minute {
				return nil, errors.New("check timeout is invalid")
			}
			timeout = parsed
		}
		checks = append(checks, HubCheck{Name: check.Name, Argv: argv, Timeout: timeout})
	}
	return checks, nil
}

func validHubChecks(checks []HubCheck) bool {
	seen := make(map[string]struct{}, len(checks))
	for _, check := range checks {
		if !hubCheckNamePattern.MatchString(check.Name) || len(check.Argv) == 0 || len(check.Argv) > 32 || check.Timeout <= 0 || check.Timeout > 5*time.Minute {
			return false
		}
		if _, exists := seen[check.Name]; exists {
			return false
		}
		seen[check.Name] = struct{}{}
		for _, argument := range check.Argv {
			if argument == "" || len(argument) > 1024 || strings.IndexByte(argument, 0) >= 0 {
				return false
			}
		}
	}
	return true
}

func cloneHubChecks(checks []HubCheck) []HubCheck {
	copy := make([]HubCheck, len(checks))
	for index, check := range checks {
		copy[index] = HubCheck{Name: check.Name, Argv: append([]string(nil), check.Argv...), Timeout: check.Timeout}
	}
	return copy
}

// HubCheckExecutor is injectable for fixture tests.  Production uses
// executeHubCheck, which discards stdout and stderr.
type HubCheckExecutor func(context.Context, []string) error

func runHubChecks(ctx context.Context, checks []HubCheck, execute HubCheckExecutor) map[string]HubCheckStatus {
	results := make(map[string]HubCheckStatus, len(checks))
	for _, check := range checks {
		checkContext, cancel := context.WithTimeout(ctx, check.Timeout)
		err := execute(checkContext, append([]string(nil), check.Argv...))
		cancel()
		if err == nil {
			results[check.Name] = HubCheckOK
		} else {
			results[check.Name] = HubCheckFail
		}
	}
	return results
}

func executeHubCheck(ctx context.Context, argv []string) error {
	if len(argv) == 0 {
		return errors.New("empty argv")
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	return command.Run()
}
