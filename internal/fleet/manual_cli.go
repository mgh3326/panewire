package fleet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

const manualUsage = `usage: panewire manual set|list|clear ...

  set    --pool P [--window W] (--used N | --credit-balance USD --credit-total USD --expires DATE)
         [--remaining USD] [--total USD] [--expires-on DATE]
         [--measured-at RFC3339] [--reason TEXT] [--resets-at RFC3339]
         [--resets-in DURATION] [--ttl DURATION] [--json]
  list   [POOL] [--pool P]
  clear  --pool P`

var manualValueFlags = map[string]bool{
	"pool": true, "window": true, "used": true,
	"remaining": true, "total": true, "expires-on": true,
	"credit-balance": true, "credit-total": true, "expires": true,
	"measured-at": true, "reason": true, "resets-at": true,
	"resets-in": true, "ttl": true,
}

// ManualMain is `panewire manual`. It writes the same hk shared manual
// observation pacelever writes; the credit dollars stay on the local record
// and #309's configured total supplies them on read.
func ManualMain(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, manualUsage)
		return exitUsage
	}
	a, err := New(out, errOut)
	if err != nil {
		fmt.Fprintln(errOut, "panewire:", err)
		return featureExitCode(err)
	}
	if err := a.manualCommand(ctx, args, out); err != nil {
		fmt.Fprintln(errOut, "panewire:", err)
		return featureExitCode(err)
	}
	return 0
}

func (a *App) manualCommand(ctx context.Context, args []string, out io.Writer) error {
	switch args[0] {
	case "list":
		return a.manualList(ctx, args[1:], out)
	case "clear":
		return a.manualClear(ctx, args[1:])
	case "set":
		return a.manualSet(ctx, args[1:], out)
	}
	return fail(exitUsage, "%s", manualUsage)
}

func (a *App) manualList(ctx context.Context, args []string, out io.Writer) error {
	values, _, positional, err := parseFeatureFlags(args, manualValueFlags, map[string]bool{"json": true})
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	pool := values["pool"]
	if pool == "" && len(positional) > 0 {
		pool = positional[0]
	}
	if len(positional) > 1 {
		return fail(exitUsage, "manual list takes one pool")
	}
	store, err := a.readManual(ctx)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	if pool == "" {
		return writeJSON(out, store)
	}
	return writeJSON(out, manualStoreValid(store, pool, a.Now()))
}

func (a *App) manualClear(ctx context.Context, args []string) error {
	values, _, positional, err := parseFeatureFlags(args, manualValueFlags, map[string]bool{"json": true})
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	pool := values["pool"]
	if pool == "" && len(positional) > 0 {
		pool = positional[0]
	}
	if pool == "" || len(positional) > 1 {
		return fail(exitUsage, "manual clear requires --pool")
	}
	if err := a.clearManual(ctx, pool); err != nil {
		return fail(exitUsage, "%v", err)
	}
	return nil
}

func (a *App) manualSet(ctx context.Context, args []string, out io.Writer) error {
	values, _, positional, err := parseFeatureFlags(args, manualValueFlags, map[string]bool{"json": true})
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	pool := values["pool"]
	if pool == "" && len(positional) > 0 {
		pool = positional[0]
	}
	if len(positional) > 1 {
		return fail(exitUsage, "manual set takes one pool")
	}
	creditForm := firstManualValue(values["remaining"], values["total"], values["expires-on"],
		values["credit-balance"], values["credit-total"], values["expires"]) != ""
	if !creditForm {
		if err := requireManualFlags(values, "measured-at", "reason"); err != nil {
			return err
		}
	} else if pool == "" {
		return fail(exitUsage, "pool required")
	}
	measured, reason := values["measured-at"], values["reason"]
	if creditForm {
		if measured == "" {
			measured = a.Now().UTC().Format(time.RFC3339Nano)
		}
		if reason == "" {
			reason = "credit balance observed"
		}
	}
	m := ManualObservation{Pool: pool, Window: values["window"], MeasuredAt: measured, Reason: reason, ResetsAt: values["resets-at"]}
	if values["used"] != "" {
		used, err := strconv.ParseFloat(values["used"], 64)
		if err != nil {
			return fail(exitUsage, "invalid --used")
		}
		m.UsedPct = manualPct(used)
	}
	balance, total, expires := values["credit-balance"], values["credit-total"], values["expires"]
	if balance == "" {
		balance = values["remaining"]
	}
	if total == "" {
		total = values["total"]
	}
	if expires == "" {
		expires = values["expires-on"]
	}
	if balance != "" || total != "" || expires != "" {
		remaining, err := strconv.ParseFloat(balance, 64)
		if err != nil {
			return fail(exitUsage, "invalid --credit-balance")
		}
		totalUSD, err := strconv.ParseFloat(total, 64)
		if err != nil {
			return fail(exitUsage, "invalid --credit-total")
		}
		m.Credit = &ManualCredit{RemainingUSD: remaining, TotalUSD: totalUSD, ExpiresOn: expires}
	}
	var ttl time.Duration
	if values["ttl"] != "" {
		ttl, err = parseManualDuration(values["ttl"])
		if err != nil {
			return fail(exitUsage, "invalid --ttl")
		}
	}
	if values["resets-in"] != "" {
		if m.ResetsAt != "" {
			return fail(exitUsage, "reset options are mutually exclusive")
		}
		duration, err := parseManualDuration(values["resets-in"])
		if err != nil {
			return fail(exitUsage, "invalid --resets-in")
		}
		at, err := parseManualTime(m.MeasuredAt)
		if err != nil {
			return fail(exitUsage, "invalid --measured-at")
		}
		m.ResetsAt = at.Add(duration).Format(time.RFC3339Nano)
	}
	prepared, err := a.addManual(ctx, m, ttl, a.Now())
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	return writeJSON(out, prepared)
}

func firstManualValue(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func requireManualFlags(values map[string]string, names ...string) error {
	for _, name := range names {
		if values[name] == "" {
			return fail(exitUsage, "manual set requires --%s", name)
		}
	}
	return nil
}

func (a *App) readManual(ctx context.Context) (ManualStore, error) {
	out := ManualStore{Schema: "pacelever.manual.v1", History: []ManualObservation{}, Latest: map[string]string{}}
	client, err := a.quotaClient()
	if err != nil {
		return out, err
	}
	for _, pool := range manualPools {
		key, err := manualAccountKey(a, pool)
		if err != nil {
			continue
		}
		var latest shared.Latest
		if err := client.get(ctx, "/v1/quota/accounts/"+url.PathEscape(key)+"/latest", &latest); err != nil {
			return out, fmt.Errorf("shared manual observations unavailable")
		}
		for _, m := range manualFromShared(latest.Manual, pool) {
			out.History = append(out.History, m)
			out.Latest[m.Pool+":"+m.Window] = m.ID
		}
	}
	return out, nil
}

func (a *App) addManual(ctx context.Context, m ManualObservation, ttl time.Duration, now time.Time) (ManualObservation, error) {
	m, err := prepareManual(m, ttl, now)
	if err != nil {
		return m, err
	}
	key, err := manualAccountKey(a, m.Pool)
	if err != nil {
		return m, err
	}
	observation, err := sharedManualObservation(m)
	if err != nil {
		return m, fmt.Errorf("shared manual write failed")
	}
	if err := a.withManualLease(ctx, key, func(holder string) error {
		return a.publishManual(ctx, key, holder, observation)
	}); err != nil {
		return m, err
	}
	return m, nil
}

func (a *App) clearManual(ctx context.Context, pool string) error {
	key, err := manualAccountKey(a, pool)
	if err != nil {
		return err
	}
	return a.withManualLease(ctx, key, func(holder string) error {
		client, err := a.quotaClient()
		if err != nil {
			return err
		}
		var latest shared.Latest
		if err := client.get(ctx, "/v1/quota/accounts/"+url.PathEscape(key)+"/latest", &latest); err != nil {
			return err
		}
		now := a.Now()
		for _, x := range latest.Manual {
			if x.MeasuredAt.After(now) {
				now = x.MeasuredAt
			}
			x.ID = 0
			x.MeasuredAt = now
			x.ExpiresAt = &now
			x.Buckets = []shared.Bucket{}
			if err := a.publishManual(ctx, key, holder, x); err != nil {
				return err
			}
		}
		return nil
	})
}

func (a *App) publishManual(ctx context.Context, key, holder string, observation shared.Observation) error {
	client, err := a.quotaClient()
	if err != nil {
		return err
	}
	observation.ConfirmedAt = nil
	return client.request(ctx, http.MethodPut, "/v1/quota/accounts/"+url.PathEscape(key)+"/observation",
		map[string]any{"holder": holder, "observation": observation}, nil)
}

func (a *App) withManualLease(ctx context.Context, key string, fn func(holder string) error) error {
	client, err := a.quotaClient()
	if err != nil {
		return err
	}
	holder, err := manualHolder()
	if err != nil {
		return fmt.Errorf("shared lease unavailable")
	}
	lease, err := a.manualLease(ctx, client, key, holder)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.request(context.Background(), http.MethodDelete, "/v1/quota/accounts/"+url.PathEscape(key)+"/lease",
			map[string]any{"holder": holder}, nil)
	}()
	if lease.Holder != holder {
		return fmt.Errorf("shared account busy; retry manual entry")
	}
	if fn(holder) != nil {
		return fmt.Errorf("shared manual write failed")
	}
	return nil
}

func (a *App) manualLease(ctx context.Context, client *hkQuota, key, holder string) (shared.Lease, error) {
	var lease shared.Lease
	path := "/v1/quota/accounts/" + url.PathEscape(key) + "/lease"
	err := client.request(ctx, http.MethodPost, path, map[string]any{"holder": holder, "ttl_s": 120}, &lease)
	if err != nil {
		return lease, fmt.Errorf("shared lease unavailable")
	}
	if !lease.Acquired {
		time.Sleep(time.Second)
		if err = client.request(ctx, http.MethodPost, path, map[string]any{"holder": holder, "ttl_s": 120}, &lease); err != nil {
			return lease, fmt.Errorf("shared lease unavailable")
		}
	}
	if !lease.Acquired {
		return lease, fmt.Errorf("shared account busy; retry manual entry")
	}
	return lease, nil
}
