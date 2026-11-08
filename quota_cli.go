package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"
)

type quotaBucket struct {
	Window   string   `json:"window"`
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt string   `json:"resets_at,omitempty"`
	Scope    struct {
		Kind string `json:"kind"`
		Name string `json:"name,omitempty"`
	} `json:"scope"`
}
type quotaReading struct {
	Pool       string        `json:"pool"`
	Account    string        `json:"account"`
	Source     string        `json:"source"`
	AgeSeconds *float64      `json:"age_seconds"`
	Stale      bool          `json:"stale"`
	Buckets    []quotaBucket `json:"buckets"`
}
type quotaProvider struct {
	ID          string        `json:"id"`
	Source      string        `json:"source"`
	Age         *float64      `json:"age_s"`
	Stale       bool          `json:"stale"`
	Account     string        `json:"account_fp"`
	AccountKind string        `json:"account_fp_kind"`
	Publish     string        `json:"publish_fp"`
	PublishKind string        `json:"publish_fp_kind"`
	Buckets     []quotaBucket `json:"buckets"`
}

func (p quotaProvider) reading() quotaReading {
	account := ""
	if p.AccountKind == "account" {
		account = p.Account
	}
	if p.PublishKind == "account" {
		account = p.Publish
	}
	return quotaReading{p.ID, account, p.Source, p.Age, p.Stale, p.Buckets}
}
func quotaRun(ctx context.Context, what string, timeout time.Duration, jsonOutput bool, args ...string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pacelever", args...)
	cmd.WaitDelay = 100 * time.Millisecond
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
		err = nil
	}
	for _, line := range strings.Split(stderr.String(), "\n") {
		if line != "" && !strings.HasPrefix(line, "warning:") {
			slog.Debug("pacelever diagnostic", "line", line)
		}
	}
	if ctx.Err() == context.DeadlineExceeded {
		return nil, ExitTimeout, fmt.Errorf("%s: timed out after %s", what, timeout)
	}
	if err != nil {
		code := ExitInternal
		if e, ok := err.(*exec.ExitError); ok && e.ExitCode() >= 0 {
			code = e.ExitCode()
		}
		if !jsonOutput {
			summary := ""
			for _, line := range strings.Split(stderr.String(), "\n") {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "warning:") {
					summary = line
					break
				}
			}
			return out.Bytes(), code, fmt.Errorf("%s: exit %d: %s", what, code, summary)
		}
		var result struct {
			Reason string `json:"reason"`
			Error  string `json:"error"`
		}
		_ = json.Unmarshal(out.Bytes(), &result)
		reason := result.Reason
		if reason == "" {
			reason = result.Error
		}
		if reason == "" {
			reason = "dependency failed"
		}
		return out.Bytes(), code, fmt.Errorf("%s: %s", what, reason)
	}
	return out.Bytes(), 0, nil
}

// quotaStatusHost is one host of the status document (docs/quota.md §Status
// JSON v1). Pacelever is "absent" when this host has no pacelever binary and
// the readings are panewire's own.
type quotaStatusHost struct {
	Host             string         `json:"host"`
	PaceleverVersion string         `json:"pacelever_version"`
	Pacelever        string         `json:"pacelever,omitempty"`
	Readings         []quotaReading `json:"readings"`
}

func writeQuotaStatus(out io.Writer, jsonOutput bool, generatedAt string, host quotaStatusHost) {
	if jsonOutput {
		_ = json.NewEncoder(out).Encode(struct {
			Schema      string            `json:"schema"`
			GeneratedAt string            `json:"generated_at"`
			Hosts       []quotaStatusHost `json:"hosts"`
		}{"panewire.quota.status.v1", generatedAt, []quotaStatusHost{host}})
		return
	}
	for _, v := range host.Readings {
		age := "unknown"
		if v.AgeSeconds != nil {
			age = fmt.Sprint(*v.AgeSeconds)
		}
		buckets, _ := json.Marshal(v.Buckets)
		fmt.Fprintf(out, "%s %s account=%s source=%s age_seconds=%s stale=%t buckets=%s\n", host.Host, v.Pool, v.Account, v.Source, age, v.Stale, buckets)
	}
}

// runNativeQuotaStatus is `panewire quota status` on a host without pacelever:
// the shared store's reading of each pool this host measures itself, read
// without measuring, and a note on stderr that pacelever is absent. A pool
// whose reading cannot be read is listed as unavailable.
func runNativeQuotaStatus(ctx context.Context, jsonOutput bool, out, errOut io.Writer) int {
	host, err := os.Hostname()
	if err != nil {
		fmt.Fprintln(errOut, "quota status: hostname unavailable")
		return ExitInternal
	}
	fmt.Fprintln(errOut, "quota status: pacelever absent; native readings only")
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	readings := []quotaReading{}
	var generatedAt time.Time
	for _, pool := range nativeStatusPools {
		if !isNativeQuotaPool(pool) {
			continue
		}
		source := quotaCLISource(pool)
		generatedAt = source.now()
		reading, err := source.stored(ctx)
		if err != nil {
			fmt.Fprintf(errOut, "quota status: %s: %s\n", pool, quotaErrorKind(err))
			reading = quotaReading{Pool: pool, Source: "unavailable", Stale: true, Buckets: []quotaBucket{}}
		}
		readings = append(readings, reading)
	}
	writeQuotaStatus(out, jsonOutput, generatedAt.Format(time.RFC3339Nano), quotaStatusHost{Host: host, Pacelever: "absent", Readings: readings})
	return 0
}

func runQuotaCLI(args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, "usage: panewire quota status [--json] | refresh POOL | observe KIND ...")
		return ExitUsage
	}
	ctx := context.Background()
	switch args[0] {
	case "status":
		if len(args) > 2 || len(args) == 2 && args[1] != "--json" {
			return ExitUsage
		}
		if _, err := exec.LookPath("pacelever"); err != nil {
			return runNativeQuotaStatus(ctx, len(args) == 2, out, errOut)
		}
		data, code, err := quotaRun(ctx, "quota status", 30*time.Second, true, "--json")
		if err != nil {
			fmt.Fprintln(errOut, err)
			return code
		}
		var doc struct {
			Schema      string          `json:"schema"`
			GeneratedAt string          `json:"generated_at"`
			Providers   []quotaProvider `json:"providers"`
		}
		if json.Unmarshal(data, &doc) != nil || doc.Schema == "" || doc.Providers == nil {
			fmt.Fprintln(errOut, "quota status: invalid JSON")
			return ExitInternal
		}
		version, code, err := quotaRun(ctx, "pacelever version", 30*time.Second, false, "--version")
		if err != nil {
			fmt.Fprintln(errOut, err)
			return code
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Fprintln(errOut, "quota status: hostname unavailable")
			return ExitInternal
		}
		readings := make([]quotaReading, 0, len(doc.Providers))
		for _, p := range doc.Providers {
			readings = append(readings, p.reading())
		}
		writeQuotaStatus(out, len(args) == 2, doc.GeneratedAt, quotaStatusHost{Host: host, PaceleverVersion: strings.TrimSpace(string(version)), Readings: readings})
		return 0
	case "refresh":
		if len(args) != 2 || !fleetQuotaPoolPattern.MatchString(args[1]) {
			return ExitUsage
		}
		if isNativeQuotaPool(args[1]) {
			return runNativeQuotaRefresh(ctx, args[1], out, errOut)
		}
		data, code, err := quotaRun(ctx, "quota refresh", 60*time.Second, false, "refresh", args[1])
		if err != nil {
			fmt.Fprintln(errOut, err)
			return code
		}
		var p quotaProvider
		if json.Unmarshal(data, &p) == nil && p.ID == args[1] {
			_ = json.NewEncoder(out).Encode(p.reading())
		} else {
			_, _ = out.Write(data)
		}
		return 0
	case "observe":
		if len(args) < 2 {
			return ExitUsage
		}
		data, code, err := quotaRun(ctx, "quota observe", 30*time.Second, false, args...)
		if err != nil {
			fmt.Fprintln(errOut, err)
		}
		if err == nil {
			_, _ = out.Write(data)
		}
		return code
	}
	return ExitUsage
}
