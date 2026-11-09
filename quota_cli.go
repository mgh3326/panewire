package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
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

// quotaStatusHost is one host of the status document (docs/quota.md §Status
// JSON v1). Pacelever is "absent" when this host has no pacelever binary and
// the readings are panewire's own.
type quotaStatusHost struct {
	Host     string         `json:"host"`
	Readings []quotaReading `json:"readings"`
}

func writeQuotaStatus(out io.Writer, jsonOutput bool, generatedAt string, host quotaStatusHost) {
	if jsonOutput {
		_ = json.NewEncoder(out).Encode(struct {
			Schema      string            `json:"schema"`
			GeneratedAt string            `json:"generated_at"`
			Hosts       []quotaStatusHost `json:"hosts"`
		}{"panewire.quota.status.v2", generatedAt, []quotaStatusHost{host}})
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

// runNativeQuotaStatus reads this host's stored native measurements.
func runNativeQuotaStatus(ctx context.Context, jsonOutput bool, out, errOut io.Writer) int {
	host, err := os.Hostname()
	if err != nil {
		fmt.Fprintln(errOut, "quota status: hostname unavailable")
		return ExitInternal
	}
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
	writeQuotaStatus(out, jsonOutput, generatedAt.Format(time.RFC3339Nano), quotaStatusHost{Host: host, Readings: readings})
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
		return runNativeQuotaStatus(ctx, len(args) == 2, out, errOut)
	case "refresh":
		if len(args) != 2 || !fleetQuotaPoolPattern.MatchString(args[1]) {
			return ExitUsage
		}
		if !isNativeQuotaPool(args[1]) {
			fmt.Fprintf(errOut, "quota refresh: %s 풀에는 native 측정 소스가 없습니다\n", args[1])
			return 4
		}
		return runNativeQuotaRefresh(ctx, args[1], out, errOut)
	case "observe":
		if len(args) < 2 {
			return ExitUsage
		}
		fmt.Fprintln(errOut, "quota observe: no native observation command is available")
		return ExitUsage
	}
	return ExitUsage
}
