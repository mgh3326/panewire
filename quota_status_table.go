package panewire

import (
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// quotaStatusRow is one printed line of the status table before alignment.
type quotaStatusRow struct {
	window, used, resets string
}

// writeQuotaStatusTable prints `quota status` for people: one row per pool and
// window, with the account id as the status JSON carries it. `--json` is the
// machine contract (writeQuotaStatusJSON); this text may change freely. The
// band is the tier selection assigns the pool on the same reading
// (fleet.StatusBand), so the table cannot disagree with placement. A credit
// pool's row carries its manual entry's used share and expiry, not a reading's
// windows.
func writeQuotaStatusTable(out io.Writer, now time.Time, host quotaStatusHost, credit map[string]fleet.CreditWindow, policy fleet.PoolPolicy) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "HOST\tPOOL\tACCOUNT\tWINDOW\tUSED\tRESETS IN\tFRESHNESS\tBAND")
	for _, r := range host.Readings {
		freshness := "fresh"
		if r.Stale {
			freshness = "stale"
		}
		account := r.Account
		if account == "" {
			account = "-"
		}
		var rows []quotaStatusRow
		var band string
		if r.Credit != "" {
			band = fleet.StatusBand(policy, r.Pool, r.Class, r.Stale, nil, now)
			row := quotaStatusRow{window: "credit", used: "-", resets: "-"}
			if w := credit[r.Pool]; !w.ExpiresAt.IsZero() {
				row.used = fmt.Sprintf("%.1f%%", w.UsedPct)
				row.resets = quotaResetsIn(w.ExpiresAt.Sub(now))
			}
			rows = append(rows, row)
		} else {
			windows := make([]fleet.StatusWindow, 0, len(r.Buckets))
			for _, b := range r.Buckets {
				windows = append(windows, fleet.StatusWindow{Window: b.Window, UsedPct: b.UsedPct, ResetsAt: b.ResetsAt, Scope: b.Scope.Kind, Model: b.Scope.Name})
			}
			band = fleet.StatusBand(policy, r.Pool, r.Class, r.Stale, windows, now)
			for _, b := range r.Buckets {
				row := quotaStatusRow{window: b.Window, used: "-", resets: "-"}
				if b.UsedPct != nil {
					row.used = fmt.Sprintf("%.1f%%", *b.UsedPct)
				}
				if reset, err := time.Parse(time.RFC3339Nano, b.ResetsAt); err == nil {
					row.resets = quotaResetsIn(reset.Sub(now))
				}
				rows = append(rows, row)
			}
			if len(rows) == 0 {
				rows = append(rows, quotaStatusRow{window: "-", used: "-", resets: "-"})
			}
		}
		for _, row := range rows {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", host.Host, r.Pool, account, row.window, row.used, row.resets, freshness, band)
		}
	}
	_ = tw.Flush()
}

// quotaResetsIn is the time left until a window resets, for people: "passed"
// once it has, "<1m" under a minute, then the two most significant units.
func quotaResetsIn(left time.Duration) string {
	if left <= 0 {
		return "passed"
	}
	if left < time.Minute {
		return "<1m"
	}
	minutes := int(left / time.Minute)
	days, hours, mins := minutes/(24*60), minutes/60%24, minutes%60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	return fmt.Sprintf("%dm", mins)
}
