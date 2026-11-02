package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CreditBalance is a prepaid credit pool's latest hand-entered balance, read
// from pacelever's manual observation store (`pacelever manual list`), the
// same record its credit gate admits on. Nothing here is measured: the
// operator records the balance and its expiry by hand.
type CreditBalance struct {
	// LeftPct is the balance left, as a percentage of the credit total.
	LeftPct float64
	// BalanceUSD is the dollar balance when the entry records one.
	BalanceUSD *float64
	ExpiresAt  time.Time
}

// finalWindow reports whether a balance is left within the policy's final
// window before expiry (credit_final_days; 0 disables), and the hours left.
// How much credit has been spent does not matter: credit ranks below
// subscription quota, which resets sooner, until this window, and then
// first so the rest is used up before it expires.
func (c *CreditBalance) finalWindow(now time.Time, days float64) (bool, float64) {
	if c == nil || c.LeftPct <= 0 || days <= 0 {
		return false, 0
	}
	hours := c.ExpiresAt.Sub(now).Hours()
	return hours > 0 && hours <= days*24, hours
}

type manualEntry struct {
	ID        string   `json:"manual_observation_id"`
	Event     string   `json:"event"`
	Pool      string   `json:"pool"`
	Window    string   `json:"window"`
	UsedPct   *float64 `json:"used_pct"`
	ExpiresAt string   `json:"expires_at"`
	Credit    *struct {
		RemainingUSD float64 `json:"remaining_usd"`
		ExpiresOn    string  `json:"expires_on"`
	} `json:"credit"`
}

// creditBalances reads the current credit-window entry of every pool in
// pacelever's manual store.
func (q CLIQuota) creditBalances(ctx context.Context) (map[string]*CreditBalance, error) {
	r := q.Runner.Run(ctx, Command{Name: q.Binary, Args: []string{"manual", "list"}})
	if e := check(r, "manual store"); e != nil {
		return nil, e
	}
	var store struct {
		Schema  string            `json:"schema"`
		History []manualEntry     `json:"history"`
		Latest  map[string]string `json:"latest"`
	}
	if e := json.Unmarshal([]byte(r.Out), &store); e != nil || store.Schema == "" {
		return nil, fail(exitConfig, "invalid manual store JSON")
	}
	out := map[string]*CreditBalance{}
	for _, m := range store.History {
		if m.Window != "credit" || m.Event != "set" || store.Latest[m.Pool+":credit"] != m.ID || m.UsedPct == nil || *m.UsedPct < 0 || *m.UsedPct > 100 {
			continue
		}
		expires, e := creditExpiry(m)
		if e != nil {
			continue
		}
		c := &CreditBalance{LeftPct: 100 - *m.UsedPct, ExpiresAt: expires}
		if m.Credit != nil {
			usd := m.Credit.RemainingUSD
			c.BalanceUSD = &usd
		}
		out[m.Pool] = c
	}
	return out, nil
}

// creditExpiry is the credit's own expiry when the entry records dollars,
// else the entry's expiry, which pacelever sets to the credit's expiry.
func creditExpiry(m manualEntry) (time.Time, error) {
	s := m.ExpiresAt
	if m.Credit != nil && m.Credit.ExpiresOn != "" {
		s = m.Credit.ExpiresOn
	}
	if t, e := time.Parse("2006-01-02", s); e == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

// creditFinal is a candidate's final-window reading: the plan text and
// record name the reason, the days left, the window and the balance.
type creditFinal struct {
	balance   *CreditBalance
	hoursLeft float64
	days      float64
}

func (c *creditFinal) note() string {
	s := fmt.Sprintf("credit_expiring final_window_days=%g days_left=%.1f balance_pct=%.1f", c.days, c.hoursLeft/24, c.balance.LeftPct)
	if c.balance.BalanceUSD != nil {
		s += fmt.Sprintf(" balance_usd=%.2f", *c.balance.BalanceUSD)
	}
	return s
}

func (c *creditFinal) record() map[string]any {
	r := map[string]any{"days_left": c.hoursLeft / 24, "final_window_days": c.days, "balance_pct": c.balance.LeftPct, "expires_at": c.balance.ExpiresAt.Format(time.RFC3339)}
	if c.balance.BalanceUSD != nil {
		r["balance_usd"] = *c.balance.BalanceUSD
	}
	return r
}
