package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// HubHostLoad is the bounded machine telemetry used only by the burst policy.
// It intentionally contains measurements, never command output.
type HubHostLoad struct {
	Load1       float64 `json:"load1"`
	Load5       float64 `json:"load5"`
	SwapUsedGB  float64 `json:"swap_used_gb"`
	WorkerProcs int     `json:"worker_procs"`
}

func (load HubHostLoad) valid() bool {
	return load.Load1 >= 0 && load.Load5 >= 0 && load.SwapUsedGB >= 0 && load.WorkerProcs >= 0 &&
		!math.IsNaN(load.Load1) && !math.IsNaN(load.Load5) && !math.IsNaN(load.SwapUsedGB) &&
		!math.IsInf(load.Load1, 0) && !math.IsInf(load.Load5, 0) && !math.IsInf(load.SwapUsedGB, 0)
}

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

// collectHubHostLoad reads the two platform load sources and a local worker
// count. Command text is parsed and discarded before the value leaves here.
func collectHubHostLoad(ctx context.Context) (HubHostLoad, error) {
	var load HubHostLoad
	var err error
	if runtime.GOOS == "darwin" {
		load, err = collectDarwinHostLoad(ctx, runHubMeasurement)
	} else if runtime.GOOS == "linux" {
		load, err = collectLinuxHostLoad(os.ReadFile)
	} else {
		return HubHostLoad{}, errors.New("host load unsupported")
	}
	if err != nil {
		return HubHostLoad{}, err
	}
	workers, err := countHubWorkerProcesses(ctx, runHubMeasurement)
	if err != nil {
		return HubHostLoad{}, err
	}
	load.WorkerProcs = workers
	return load, nil
}

func runHubMeasurement(ctx context.Context, argv ...string) ([]byte, error) {
	if len(argv) == 0 {
		return nil, errors.New("measurement unavailable")
	}
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	return command.Output()
}

func collectDarwinHostLoad(ctx context.Context, run func(context.Context, ...string) ([]byte, error)) (HubHostLoad, error) {
	loads, err := run(ctx, "sysctl", "-n", "vm.loadavg")
	if err != nil {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	swap, err := run(ctx, "sysctl", "-n", "vm.swapusage")
	if err != nil {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	return parseDarwinHostLoad(string(loads), string(swap))
}

func parseDarwinHostLoad(loads, swap string) (HubHostLoad, error) {
	fields := strings.Fields(strings.Trim(strings.TrimSpace(loads), "{}"))
	if len(fields) < 3 {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	parse := func(value string) (float64, error) { return strconv.ParseFloat(strings.Trim(value, "{}"), 64) }
	load1, err1 := parse(fields[0])
	load5, err5 := parse(fields[1])
	match := regexp.MustCompile(`(?i)used\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG])`).FindStringSubmatch(swap)
	if err1 != nil || err5 != nil || len(match) != 3 {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	used, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	switch strings.ToUpper(match[2]) {
	case "K":
		used /= 1024 * 1024
	case "M":
		used /= 1024
	case "G":
	}
	load := HubHostLoad{Load1: load1, Load5: load5, SwapUsedGB: used}
	if !load.valid() {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	return load, nil
}

func collectLinuxHostLoad(read func(string) ([]byte, error)) (HubHostLoad, error) {
	loads, err := read("/proc/loadavg")
	if err != nil {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	meminfo, err := read("/proc/meminfo")
	if err != nil {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	return parseLinuxHostLoad(string(loads), string(meminfo))
}

func parseLinuxHostLoad(loads, meminfo string) (HubHostLoad, error) {
	fields := strings.Fields(loads)
	if len(fields) < 2 {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	load1, err1 := strconv.ParseFloat(fields[0], 64)
	load5, err5 := strconv.ParseFloat(fields[1], 64)
	values := make(map[string]float64)
	for _, line := range strings.Split(meminfo, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if value, err := strconv.ParseFloat(fields[1], 64); err == nil {
				values[strings.TrimSuffix(fields[0], ":")] = value
			}
		}
	}
	total, totalOK := values["SwapTotal"]
	free, freeOK := values["SwapFree"]
	if err1 != nil || err5 != nil || !totalOK || !freeOK || free > total {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	load := HubHostLoad{Load1: load1, Load5: load5, SwapUsedGB: (total - free) / (1024 * 1024)}
	if !load.valid() {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	return load, nil
}

func countHubWorkerProcesses(ctx context.Context, run func(context.Context, ...string) ([]byte, error)) (int, error) {
	// `pgrep -c` is Linux-only; macOS pgrep rejects it with exit 2. Listing
	// matching PIDs and counting lines is portable across both platforms.
	output, runErr := run(ctx, "pgrep", "-f", "codex|claude")
	if runErr != nil {
		// pgrep exits 1 when nothing matches. That is the normal idle signal,
		// not a collection failure; every other command failure suppresses telemetry.
		var exitErr *exec.ExitError
		if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 1 || strings.TrimSpace(string(output)) != "" {
			return 0, errors.New("worker count unavailable")
		}
		return 0, nil
	}
	value := 0
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if _, err := strconv.Atoi(line); err != nil {
			return 0, errors.New("worker count unavailable")
		}
		value++
	}
	return value, nil
}
