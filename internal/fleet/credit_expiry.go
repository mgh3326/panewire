package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// Credit balances turn into percent-only credit after four hours without a
// fresh manual measurement. This is shorter than the observed six-hour spend
// interval for openai-api and prevents an old dollar value winning selection.
const creditBalanceFreshFor = 4 * time.Hour

// CreditBalance is a prepaid credit pool's latest hand-entered balance, read
// from pacelever's manual observation store (`pacelever manual list`), the
// same record its credit gate admits on. Nothing here is measured: the
// operator records the balance and its expiry by hand.
type CreditBalance struct {
	// LeftPct is the balance left, as a percentage of the credit total.
	LeftPct float64
	// BalanceUSD is the dollar balance when the entry records one.
	BalanceUSD *float64
	// BurnUSDPerH is the dollar spend rate from the pool's two latest entries
	// that record dollars: (older remaining − newer remaining) over the hours
	// between their measurements. It is 0 when fewer than two such entries
	// exist or the balance did not fall.
	BurnUSDPerH float64
	// MeasuredAt is the manual entry's measurement time.
	MeasuredAt time.Time
	// StaleAt is when a measured entry stopped being fresh, or now when its
	// measurement timestamp is missing.
	StaleAt   time.Time
	ExpiresAt time.Time
}

// expiring reports a positive balance with a future expiry.
func (c *CreditBalance) expiring(now time.Time) (bool, float64) {
	if c == nil || c.LeftPct <= 0 || c.ExpiresAt.IsZero() {
		return false, 0
	}
	hours := c.ExpiresAt.Sub(now).Hours()
	return hours > 0, hours
}

type manualEntry struct {
	ID         string   `json:"manual_observation_id"`
	Event      string   `json:"event"`
	Pool       string   `json:"pool"`
	Window     string   `json:"window"`
	UsedPct    *float64 `json:"used_pct"`
	MeasuredAt string   `json:"measured_at"`
	ExpiresAt  string   `json:"expires_at"`
	Credit     *struct {
		RemainingUSD float64 `json:"remaining_usd"`
		ExpiresOn    string  `json:"expires_on"`
	} `json:"credit"`
}

// creditTotals reads `[quota].credit_total_usd` — each prepaid credit pool's
// fixed grant total in dollars — into the map a CLIQuota derives balances
// from. LoadConfig has already refused anything but a positive number.
func creditTotals(quota map[string]any) map[string]float64 {
	m, _ := quota["credit_total_usd"].(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]float64, len(m))
	for pool := range m {
		if n, ok := number(m, pool); ok {
			out[pool] = n
		}
	}
	return out
}

// creditEntryUSD is one manual entry's dollar balance: its own remaining_usd
// when it records dollars, else the configured grant total times its unused
// share. A pool with neither source has no dollars (ok false).
func creditEntryUSD(m manualEntry, total float64, hasTotal bool) (float64, bool) {
	if m.Credit != nil {
		return m.Credit.RemainingUSD, true
	}
	if !hasTotal || m.UsedPct == nil {
		return 0, false
	}
	return total * (100 - *m.UsedPct) / 100, true
}

// manualStoreData is the manual store shape credit_expiry reads, whether it
// came from hk or from pacelever's JSON.
type manualStoreData struct {
	Schema  string            `json:"schema"`
	History []manualEntry     `json:"history"`
	Latest  map[string]string `json:"latest"`
}

func quotaAccounts(quota map[string]any) map[string]string {
	m, _ := quota["accounts"].(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for pool := range m {
		if fp, ok := m[pool].(string); ok && accountFP.MatchString(fp) {
			out[pool] = fp
		}
	}
	return out
}

// manualStore reads the operator-entered entries. A shared reader answers from
// hk with no pacelever process; the last branch is the pre-migration and test
// fallback.
func (q CLIQuota) manualStore(ctx context.Context) (manualStoreData, error) {
	out := manualStoreData{Schema: "pacelever.manual.v1", History: []manualEntry{}, Latest: map[string]string{}}
	if q.SharedLatest != nil {
		for pool, fp := range q.Accounts {
			if !slices.Contains(creditAccountPools, pool) {
				continue
			}
			if !accountFP.MatchString(fp) {
				continue
			}
			latest, err := q.SharedLatest(ctx, pool+":"+fp)
			if err != nil {
				return out, err
			}
			for _, x := range latest.Manual {
				if x.Kind != "manual" || x.ExpiresAt == nil || len(x.Buckets) != 1 || x.Buckets[0].Scope != "account" {
					continue
				}
				used := x.Buckets[0].UsedPct
				entry := manualEntry{
					ID: fmt.Sprintf("hk-%d", x.ID), Event: "set", Pool: pool, Window: x.Window,
					UsedPct: &used, MeasuredAt: x.MeasuredAt.Format(time.RFC3339Nano),
					ExpiresAt: x.ExpiresAt.Format(time.RFC3339Nano),
				}
				out.History = append(out.History, entry)
				out.Latest[pool+":"+x.Window] = entry.ID
			}
		}
		return out, nil
	}
	if q.Binary == "" {
		return out, fail(exitConfig, "native manual store reader required")
	}
	r := runQuotaCommand(ctx, q.Runner, Command{Name: q.Binary, Args: []string{"manual", "list"}}, "manual store", 30*time.Second)
	if e := checkQuota(r, "manual store"); e != nil {
		return out, e
	}
	if e := json.Unmarshal([]byte(r.Out), &out); e != nil || out.Schema == "" {
		return out, fail(exitConfig, "invalid manual store JSON")
	}
	return out, nil
}

// creditBalances reads the current credit-window entry of every pool. With a
// shared reader it reads hk directly; the configured total supplies the dollars
// the shared record does not carry (#309). Without one it keeps the pacelever
// process as a test and pre-migration fallback.
func (q CLIQuota) creditBalances(ctx context.Context) (map[string]*CreditBalance, error) {
	store, err := q.manualStore(ctx)
	if err != nil {
		return nil, err
	}
	byPool := map[string][]manualEntry{}
	for _, m := range store.History {
		if m.Window != "credit" || m.Event != "set" || m.UsedPct == nil || *m.UsedPct < 0 || *m.UsedPct > 100 {
			continue
		}
		byPool[m.Pool] = append(byPool[m.Pool], m)
	}
	out := map[string]*CreditBalance{}
	for pool, entries := range byPool {
		var current *manualEntry
		for i := range entries {
			if store.Latest[pool+":credit"] == entries[i].ID {
				current = &entries[i]
				break
			}
		}
		if current == nil {
			continue
		}
		expires, e := creditExpiry(*current)
		if e != nil {
			continue
		}
		measuredAt, measuredErr := time.Parse(time.RFC3339Nano, current.MeasuredAt)
		if measuredErr != nil {
			measuredAt = time.Time{}
		}
		c := &CreditBalance{LeftPct: 100 - *current.UsedPct, MeasuredAt: measuredAt, ExpiresAt: expires}
		total, hasTotal := q.CreditTotalUSD[pool]
		if usd, hasDollars := creditEntryUSD(*current, total, hasTotal); hasDollars {
			if measuredAt.IsZero() {
				c.StaleAt = q.now()
			} else if !measuredAt.Add(creditBalanceFreshFor).After(q.now()) {
				c.StaleAt = measuredAt.Add(creditBalanceFreshFor)
			}
			if c.StaleAt.IsZero() {
				c.BalanceUSD = &usd
				c.BurnUSDPerH = creditBurnUSDPerH(entries, total, hasTotal)
			}
		}
		out[pool] = c
	}
	return out, nil
}

func (q CLIQuota) now() time.Time {
	if q.Now != nil {
		return q.Now()
	}
	return time.Now()
}

// creditBurnUSDPerH is a credit pool's spend rate from its two latest entries
// with a dollar balance — an entry's own remaining_usd, else the configured
// grant total times its unused share: (older − newer) over the hours between
// their measurements. It is 0 when fewer than two such entries exist or the
// balance did not fall.
func creditBurnUSDPerH(entries []manualEntry, total float64, hasTotal bool) float64 {
	type reading struct {
		at  time.Time
		usd float64
	}
	var readings []reading
	for _, m := range entries {
		usd, ok := creditEntryUSD(m, total, hasTotal)
		if !ok {
			continue
		}
		if at, e := time.Parse(time.RFC3339Nano, m.MeasuredAt); e == nil {
			readings = append(readings, reading{at: at, usd: usd})
		}
	}
	if len(readings) < 2 {
		return 0
	}
	slices.SortStableFunc(readings, func(a, b reading) int { return a.at.Compare(b.at) })
	older, newer := readings[len(readings)-2], readings[len(readings)-1]
	hours := newer.at.Sub(older.at).Hours()
	if hours <= 0 {
		return 0
	}
	if burn := (older.usd - newer.usd) / hours; burn > 0 {
		return burn
	}
	return 0
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

// creditFinal records a candidate's balance and time to expiry, and for a
// dollar balance its burn rate, dollars at risk and urgency.
type creditFinal struct {
	balance     *CreditBalance
	hoursLeft   float64
	burnUSDPerH *float64
	atRiskUSD   *float64
	usdUrgency  *float64
}

func (c *creditFinal) note() string {
	s := fmt.Sprintf("credit_expiring days_left=%.1f balance_pct=%.1f", c.hoursLeft/24, c.balance.LeftPct)
	if !c.balance.StaleAt.IsZero() {
		s += " stale_since=" + c.balance.StaleAt.UTC().Format(time.RFC3339)
	}
	if c.balance.BalanceUSD != nil {
		s += fmt.Sprintf(" balance_usd=%.2f", *c.balance.BalanceUSD)
	}
	if c.burnUSDPerH != nil {
		s += fmt.Sprintf(" burn_usd_per_h=%.4f", *c.burnUSDPerH)
	}
	if c.atRiskUSD != nil {
		s += fmt.Sprintf(" at_risk_usd=%.2f", *c.atRiskUSD)
	}
	if c.usdUrgency != nil {
		s += fmt.Sprintf(" usd_urgency=%.4f", *c.usdUrgency)
	}
	return s
}

func (c *creditFinal) record() map[string]any {
	r := map[string]any{"days_left": c.hoursLeft / 24, "balance_pct": c.balance.LeftPct, "expires_at": c.balance.ExpiresAt.Format(time.RFC3339)}
	if !c.balance.StaleAt.IsZero() {
		r["state"] = "stale"
		r["stale_since"] = c.balance.StaleAt.UTC().Format(time.RFC3339)
	} else {
		r["state"] = "live"
	}
	if c.balance.BalanceUSD != nil {
		r["balance_usd"] = *c.balance.BalanceUSD
	}
	if c.burnUSDPerH != nil {
		r["burn_usd_per_h"] = *c.burnUSDPerH
	}
	if c.atRiskUSD != nil {
		r["at_risk_usd"] = *c.atRiskUSD
	}
	if c.usdUrgency != nil {
		r["usd_urgency"] = *c.usdUrgency
	}
	return r
}
