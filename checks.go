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

// HubHostMemory is bounded memory telemetry used by placement admission. It
// intentionally contains measurements, never command output. A nil numeric
// value means that individual signal could not be measured.
type HubHostMemory struct {
	FreePct      *float64 `json:"free_pct"`
	CompressedMB *float64 `json:"compressed_mb"`
	SwapUsedMB   *float64 `json:"swap_used_mb"`
	PSISomeAvg10 *float64 `json:"psi_some_avg10"`
	Source       string   `json:"source"`
}

func (memory HubHostMemory) valid() bool {
	if memory.Source != "memory_pressure" && memory.Source != "vm_stat" && memory.Source != "proc_meminfo" {
		return false
	}
	return validOptionalMemoryFloat(memory.FreePct, 0, 100) &&
		validOptionalMemoryFloat(memory.CompressedMB, 0, math.Inf(1)) &&
		validOptionalMemoryFloat(memory.SwapUsedMB, 0, math.Inf(1)) &&
		validOptionalMemoryFloat(memory.PSISomeAvg10, 0, math.Inf(1))
}

func validOptionalMemoryFloat(value *float64, minimum, maximum float64) bool {
	return value == nil || (!math.IsNaN(*value) && !math.IsInf(*value, 0) && *value >= minimum && *value <= maximum)
}

func cloneHubHostMemory(memory *HubHostMemory) *HubHostMemory {
	if memory == nil {
		return nil
	}
	copy := *memory
	copy.FreePct = cloneMemoryFloat(memory.FreePct)
	copy.CompressedMB = cloneMemoryFloat(memory.CompressedMB)
	copy.SwapUsedMB = cloneMemoryFloat(memory.SwapUsedMB)
	copy.PSISomeAvg10 = cloneMemoryFloat(memory.PSISomeAvg10)
	return &copy
}

func cloneMemoryFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func memoryFloat(value float64) *float64 { return &value }

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

// collectHubHostMemory reads platform memory telemetry. Command and file text
// is parsed and discarded before the values leave here.
func collectHubHostMemory(ctx context.Context) (*HubHostMemory, error) {
	if runtime.GOOS == "darwin" {
		return collectDarwinHostMemory(ctx, runHubMeasurement)
	}
	if runtime.GOOS == "linux" {
		return collectLinuxHostMemory(os.ReadFile)
	}
	return nil, errors.New("host memory unsupported")
}

func collectDarwinHostMemory(ctx context.Context, run func(context.Context, ...string) ([]byte, error)) (*HubHostMemory, error) {
	pressure, pressureErr := run(ctx, "memory_pressure")
	swap, _ := run(ctx, "sysctl", "-n", "vm.swapusage")
	var primary *HubHostMemory
	if pressureErr == nil {
		memory, freeOK := parseDarwinMemoryPressure(string(pressure))
		primary = &memory
		if freeOK {
			setDarwinMemorySwap(&memory, swap)
			return &memory, nil
		}
	}

	vmstat, vmstatErr := run(ctx, "vm_stat")
	memsize, memsizeErr := run(ctx, "sysctl", "-n", "hw.memsize")
	if vmstatErr == nil && memsizeErr == nil {
		memory, freeOK := parseDarwinVMStat(string(vmstat), string(memsize))
		if freeOK {
			setDarwinMemorySwap(&memory, swap)
			return &memory, nil
		}
		if primary == nil {
			primary = &memory
		}
	}
	if primary != nil {
		setDarwinMemorySwap(primary, swap)
		return primary, nil
	}
	return nil, errors.New("host memory unavailable")
}

func setDarwinMemorySwap(memory *HubHostMemory, swap []byte) {
	if used, ok := parseDarwinSwapUsedMB(string(swap)); ok {
		memory.SwapUsedMB = memoryFloat(used)
	}
}

func collectLinuxHostMemory(read func(string) ([]byte, error)) (*HubHostMemory, error) {
	meminfo, meminfoErr := read("/proc/meminfo")
	psi, psiErr := read("/proc/pressure/memory") // PSI is optional on Linux.
	if meminfoErr != nil && psiErr != nil {
		return nil, errors.New("host memory unavailable")
	}
	memory, ok := parseLinuxHostMemory(string(meminfo), string(psi))
	if !ok {
		return nil, errors.New("host memory unavailable")
	}
	return &memory, nil
}

func parseDarwinMemoryPressure(output string) (HubHostMemory, bool) {
	memory := HubHostMemory{Source: "memory_pressure"}
	free, freeOK := parseMemoryPressureFreePct(output)
	if freeOK {
		memory.FreePct = memoryFloat(free)
	}
	if pages, ok := parseDarwinPageCount(output, "Pages used by compressor"); ok {
		if pageSize, ok := parseDarwinPageSize(output); ok {
			memory.CompressedMB = memoryFloat(pages * pageSize / (1024 * 1024))
		}
	}
	return memory, freeOK && memory.valid()
}

func parseDarwinVMStat(output, memsize string) (HubHostMemory, bool) {
	memory := HubHostMemory{Source: "vm_stat"}
	pageSize, pageSizeOK := parseDarwinPageSize(output)
	if pages, ok := parseDarwinPageCount(output, "Pages occupied by compressor"); ok && pageSizeOK {
		memory.CompressedMB = memoryFloat(pages * pageSize / (1024 * 1024))
	}
	freePages, freeOK := parseDarwinPageCount(output, "Pages free")
	inactivePages, inactiveOK := parseDarwinPageCount(output, "Pages inactive")
	totalBytes, totalOK := parsePositiveFloat(memsize)
	if !pageSizeOK || !freeOK || !inactiveOK || !totalOK || pageSize <= 0 {
		return memory, false
	}
	totalPages := totalBytes / pageSize
	if totalPages <= 0 || freePages+inactivePages > totalPages {
		return memory, false
	}
	memory.FreePct = memoryFloat((freePages + inactivePages) / totalPages * 100)
	return memory, memory.valid()
}

func parseLinuxHostMemory(meminfo, psi string) (HubHostMemory, bool) {
	values := parseLinuxMeminfo(meminfo)
	memory := HubHostMemory{Source: "proc_meminfo"}
	if total, totalOK := values["MemTotal"]; totalOK {
		if available, availableOK := values["MemAvailable"]; availableOK && total > 0 && available >= 0 && available <= total {
			memory.FreePct = memoryFloat(available / total * 100)
		}
	}
	if swapTotal, totalOK := values["SwapTotal"]; totalOK {
		if swapFree, freeOK := values["SwapFree"]; freeOK && swapFree >= 0 && swapFree <= swapTotal {
			memory.SwapUsedMB = memoryFloat((swapTotal - swapFree) / 1024)
		}
	}
	if psiSome, ok := parseLinuxPSISomeAvg10(psi); ok {
		memory.PSISomeAvg10 = memoryFloat(psiSome)
	}
	return memory, memory.valid()
}

func parseMemoryPressureFreePct(output string) (float64, bool) {
	match := regexp.MustCompile(`(?m)^System-wide memory free percentage:\s*([0-9]+(?:\.[0-9]+)?)%\s*$`).FindStringSubmatch(output)
	if len(match) != 2 {
		return 0, false
	}
	value, err := strconv.ParseFloat(match[1], 64)
	return value, err == nil && value >= 0 && value <= 100
}

func parseDarwinPageSize(output string) (float64, bool) {
	match := regexp.MustCompile(`(?i)page size of\s+([0-9]+)\s*(?:bytes)?`).FindStringSubmatch(output)
	if len(match) != 2 {
		return 0, false
	}
	return parsePositiveFloat(match[1])
}

func parseDarwinPageCount(output, label string) (float64, bool) {
	match := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(label) + `:\s*([0-9]+)\.?\s*$`).FindStringSubmatch(output)
	if len(match) != 2 {
		return 0, false
	}
	return parsePositiveOrZeroFloat(match[1])
}

func parsePositiveFloat(value string) (float64, bool) {
	parsed, ok := parsePositiveOrZeroFloat(value)
	return parsed, ok && parsed > 0
}

func parsePositiveOrZeroFloat(value string) (float64, bool) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0) && parsed >= 0
}

var darwinSwapUsageRE = regexp.MustCompile(`(?i)used\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*([KMG])`)

func parseDarwinSwapUsedMB(swap string) (float64, bool) {
	match := darwinSwapUsageRE.FindStringSubmatch(swap)
	if len(match) != 3 {
		return 0, false
	}
	used, err := strconv.ParseFloat(match[1], 64)
	if err != nil || math.IsNaN(used) || math.IsInf(used, 0) || used < 0 {
		return 0, false
	}
	switch strings.ToUpper(match[2]) {
	case "K":
		used /= 1024
	case "M":
	case "G":
		used *= 1024
	default:
		return 0, false
	}
	return used, true
}

func parseLinuxMeminfo(meminfo string) map[string]float64 {
	values := make(map[string]float64)
	for _, line := range strings.Split(meminfo, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if value, ok := parsePositiveOrZeroFloat(fields[1]); ok {
				values[strings.TrimSuffix(fields[0], ":")] = value
			}
		}
	}
	return values
}

func parseLinuxPSISomeAvg10(psi string) (float64, bool) {
	match := regexp.MustCompile(`(?m)^some\s+.*\bavg10=([0-9]+(?:\.[0-9]+)?)\b`).FindStringSubmatch(psi)
	if len(match) != 2 {
		return 0, false
	}
	return parsePositiveOrZeroFloat(match[1])
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
	usedMB, swapOK := parseDarwinSwapUsedMB(swap)
	if err1 != nil || err5 != nil || !swapOK {
		return HubHostLoad{}, errors.New("host load unavailable")
	}
	load := HubHostLoad{Load1: load1, Load5: load5, SwapUsedGB: usedMB / 1024}
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
