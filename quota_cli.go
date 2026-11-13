package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota/devin"
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
	// Class, Credit and Presence are set only for a credit pool's line: the
	// operator's pool class, the manual credit entry's state (live, expired,
	// absent, or unknown when the store could not be read) and whether this
	// host's presence record for the account is current (present, absent or
	// unknown).
	Class    string `json:"class,omitempty"`
	Credit   string `json:"credit,omitempty"`
	Presence string `json:"presence,omitempty"`
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
	return quotaReading{Pool: p.ID, Account: account, Source: p.Source, AgeSeconds: p.Age, Stale: p.Stale, Buckets: p.Buckets}
}

// quotaStatusHost is one host of the status document (docs/quota.md §Status
// JSON v1). Pacelever is "absent" when this host has no pacelever binary and
// the readings are panewire's own.
type quotaStatusHost struct {
	Host     string         `json:"host"`
	Readings []quotaReading `json:"readings"`
}

// writeQuotaStatusJSON prints the status document (docs/quota.md §Status JSON
// v1). It is the machine contract `--json` callers read; its bytes do not
// change with the text table.
func writeQuotaStatusJSON(out io.Writer, generatedAt string, host quotaStatusHost) {
	_ = json.NewEncoder(out).Encode(struct {
		Schema      string            `json:"schema"`
		GeneratedAt string            `json:"generated_at"`
		Hosts       []quotaStatusHost `json:"hosts"`
	}{"panewire.quota.status.v2", generatedAt, []quotaStatusHost{host}})
}

// runNativeQuotaStatus reads this host's stored native measurements.
func runNativeQuotaStatus(ctx context.Context, jsonOutput bool, out, errOut io.Writer) int {
	host, err := os.Hostname()
	if err != nil {
		fmt.Fprintln(errOut, "quota status: hostname unavailable")
		return ExitInternal
	}
	// The band reads the pool policy the same way selection does: the default
	// order, or PANEWIRE_POOL_POLICY. A policy file that does not load is a
	// refusal, not a table with guessed bands.
	policy, err := fleet.LoadPoolPolicy("")
	if err != nil {
		fmt.Fprintf(errOut, "quota status: pool policy: %v\n", err)
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
	// A credit pool has no reading to measure; its line shows the operator's
	// class, the manual credit entry and this host's presence, on a host that
	// publishes it (quota_credit_presence.go).
	credit := quotaCLICreditSource()
	creditWindows := map[string]fleet.CreditWindow{}
	for _, pool := range fleet.CreditAccountPools() {
		reading, window, ok := credit.statusWindow(ctx, pool)
		if !ok {
			continue
		}
		if generatedAt.IsZero() {
			generatedAt = credit.now()
		}
		readings = append(readings, reading)
		creditWindows[pool] = window
	}
	statusHost := quotaStatusHost{Host: host, Readings: readings}
	if jsonOutput {
		writeQuotaStatusJSON(out, generatedAt.Format(time.RFC3339Nano), statusHost)
		return ExitOK
	}
	writeQuotaStatusTable(out, generatedAt, statusHost, creditWindows, policy)
	return ExitOK
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
			return ExitConditionInvalid
		}
		return runNativeQuotaRefresh(ctx, args[1], out, errOut)
	case "observe":
		return runNativeQuotaObserve(args[1:], out, errOut)
	}
	return ExitUsage
}

func runNativeQuotaObserve(args []string, out, errOut io.Writer) int {
	if len(args) == 0 || args[0] != "devin-free-limit" {
		fmt.Fprintln(errOut, "quota observe: devin-free-limit만 지원합니다")
		return ExitUsage
	}
	values := map[string]string{}
	flags := map[string]bool{}
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--clear", "--unparsed":
			if flags[arg] {
				return ExitUsage
			}
			flags[arg] = true
		case "--reset", "--observed", "--rung", "--source", "--account-key":
			if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
				return ExitUsage
			}
			i++
			key := strings.TrimPrefix(arg, "--")
			if _, exists := values[key]; exists {
				return ExitUsage
			}
			values[key] = args[i]
		default:
			return ExitUsage
		}
	}
	if flags["--clear"] {
		if flags["--unparsed"] || len(values) > 0 && (len(values) != 1 || values["account-key"] == "") {
			return ExitUsage
		}
	}
	err := devin.ObserveFreeLimit(devin.Config{}, values["reset"], values["observed"], values["rung"], values["source"], values["account-key"], flags["--unparsed"], flags["--clear"])
	if err != nil {
		fmt.Fprintf(errOut, "quota observe: %v\n", err)
		if errors.Is(err, devin.ErrInvalidFreeLimitObservation) {
			return ExitUsage
		}
		return ExitInternal
	}
	if flags["--clear"] {
		fmt.Fprintln(out, "cleared devin-free-limit")
	} else {
		fmt.Fprintln(out, "observed devin-free-limit")
	}
	return 0
}
