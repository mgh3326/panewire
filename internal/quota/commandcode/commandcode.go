// Package commandcode measures the commandcode pool's quota from omp, the
// harness that spends it: `omp usage --json --provider commandcode` lists every
// Command Code account omp holds on the host, each with a 5-hour and a weekly
// limit and the time omp fetched them (docs/quota.md §Native measurement:
// commandcode). panewire handles no Command Code key and calls no Command Code
// API; the month is not read, and the credit balance is read only to refuse an
// account below one credit (decision 403).
//
// The reading depends on omp's output shape, so an output this package does not
// know is an error, never a guess: the pool is then not admitted.
package commandcode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

const (
	provider = "commandcode"
	// MaxReportAge is the oldest omp report this package accepts. omp caches a
	// report for 5 minutes (±25%), so an older one means omp did not refetch;
	// the cache is then invalidated once, and a report that is still older is
	// unknown. It is the age at which the shared flow would call a reading due
	// for a refresh (shared.OtherRefreshTTL): an older one could never be
	// published as current.
	MaxReportAge = shared.OtherRefreshTTL
	// ompTimeout bounds one omp call; a cold `omp usage` takes about 3 s.
	ompTimeout = 30 * time.Second
	// outputLimit bounds what is read of omp's output.
	outputLimit = 4 << 20
	futureSkew  = time.Minute

	fiveHourMs = int64(5 * time.Hour / time.Millisecond)
	weeklyMs   = int64(7 * 24 * time.Hour / time.Millisecond)
)

// Config is everything a measurement reads from its environment. The zero value
// runs this host's omp against the wall clock; tests replace the seams.
type Config struct {
	// Now is the clock for report ages; nil is the wall clock.
	Now func() time.Time
	// Run runs omp with args and returns its standard output; nil runs the real
	// binary. A run that cannot start (no omp) must wrap exec.ErrNotFound.
	Run func(ctx context.Context, args ...string) ([]byte, error)
	// Binary is the omp executable; empty is `omp` on PATH.
	Binary string
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

func (c Config) run(ctx context.Context, args ...string) ([]byte, error) {
	if c.Run != nil {
		return c.Run(ctx, args...)
	}
	binary := c.Binary
	if binary == "" {
		binary = "omp"
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, ompTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	// omp's own failure text can name an account; it is never read.
	cmd.Stderr = nil
	cmd.WaitDelay = 2 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, outputLimit+1))
	waitErr := cmd.Wait()
	switch {
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case readErr != nil:
		return nil, readErr
	case waitErr != nil:
		return nil, waitErr
	case len(out) > outputLimit:
		return nil, errors.New("omp output too large")
	}
	return out, nil
}

// Account is one Command Code account omp holds. FP is the account's
// fingerprint, hex(sha256("commandcode-account|"+id))[:16] over the Command
// Code user id, the key pacelever and the earlier API reading used for the same
// account. Observation carries the 5h and 7d windows and MeasuredAt, the time
// omp fetched them.
type Account struct {
	FP          string
	Observation quota.Observation
}

// Fingerprint is the account key's fingerprint for a Command Code user id; an
// empty id has none.
func Fingerprint(account string) string {
	if account == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("commandcode-account|" + account))
	return hex.EncodeToString(sum[:8])
}

// ompReport is the part of `omp usage --json` this package reads.
type ompReport struct {
	Reports []struct {
		Provider  string     `json:"provider"`
		FetchedAt *float64   `json:"fetchedAt"`
		Limits    []ompLimit `json:"limits"`
	} `json:"reports"`
}

type ompLimit struct {
	ID    string `json:"id"`
	Scope struct {
		AccountID string `json:"accountId"`
	} `json:"scope"`
	Window *struct {
		ID         string   `json:"id"`
		DurationMs *float64 `json:"durationMs"`
		ResetsAt   *float64 `json:"resetsAt"`
	} `json:"window"`
	Amount *struct {
		Used         *float64 `json:"used"`
		Limit        *float64 `json:"limit"`
		UsedFraction *float64 `json:"usedFraction"`
		// Remaining is `commandcode:balance`'s amount.remaining, in credits
		// (decision 403, hk 5138). It is raw so a malformed value is unknown,
		// never zero and never a parse failure of the reading's windows.
		Remaining json.RawMessage `json:"remaining"`
	} `json:"amount"`
	Status string `json:"status"`
}

// Measure reads every Command Code account omp holds, ordered by fingerprint.
// No omp, or none holding a Command Code account, is `credentials` (no login
// here). An output this package does not know, a duplicate account or an account
// with no usable window is `parse_error`; an omp that fails or whose reports are
// all older than MaxReportAge is `probe_error`; a deadline is `timeout`. An
// account whose own report is stale is left out, never guessed. The error is a
// *quota.Error with a fixed sentence: omp's output never reaches it.
func Measure(ctx context.Context, cfg Config) ([]Account, error) {
	accounts, stale, err := cfg.read(ctx)
	if err != nil {
		return nil, err
	}
	if stale {
		// omp serves its cache; ask it to fetch again, once.
		if _, err := cfg.run(ctx, "usage", "invalidate", "--provider", provider); err != nil {
			return nil, ompFailure(ctx, err)
		}
		if accounts, stale, err = cfg.read(ctx); err != nil {
			return nil, err
		}
	}
	fresh := accounts[:0:0]
	for _, account := range accounts {
		if cfg.fresh(account.Observation.MeasuredAt) {
			fresh = append(fresh, account)
		}
	}
	if len(fresh) == 0 {
		return nil, quota.Fail(quota.ErrKindProbe, "omp usage report is stale")
	}
	return fresh, nil
}

// Fresh says a report fetched at is, as of now, neither older than MaxReportAge
// nor from the future.
func Fresh(now, fetched time.Time) bool {
	age := now.Sub(fetched)
	return age >= -futureSkew && age <= MaxReportAge
}

func (c Config) fresh(fetched time.Time) bool { return Fresh(c.now(), fetched) }

// read runs `omp usage` once. stale says some account's report is too old to
// use, so a refresh is worth asking for.
func (c Config) read(ctx context.Context) (accounts []Account, stale bool, err error) {
	out, err := c.run(ctx, "usage", "--json", "--provider", provider)
	if err != nil {
		return nil, false, ompFailure(ctx, err)
	}
	accounts, err = parse(out)
	if err != nil {
		return nil, false, err
	}
	for _, account := range accounts {
		if !c.fresh(account.Observation.MeasuredAt) {
			stale = true
		}
	}
	return accounts, stale, nil
}

func ompFailure(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist):
		return quota.Fail(quota.ErrKindCredentials, "omp is not installed")
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		return quota.Fail(quota.ErrKindTimeout, "omp usage timed out")
	}
	return quota.Fail(quota.ErrKindProbe, "omp usage failed")
}

var errShape = quota.Fail(quota.ErrKindParse, "unsupported omp usage output")

// parse reads one `omp usage --json` document: the Command Code reports, one per
// account. Anything outside the shape documented in docs/quota.md fails closed.
func parse(out []byte) ([]Account, error) {
	var doc ompReport
	decoder := json.NewDecoder(bytes.NewReader(out))
	// One document and nothing after it: trailing output is an output this
	// package does not know.
	if decoder.Decode(&doc) != nil || doc.Reports == nil || decoder.More() {
		return nil, errShape
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errShape
	}
	var accounts []Account
	unmeasurable := false
	for _, report := range doc.Reports {
		if report.Provider != provider {
			continue
		}
		if report.FetchedAt == nil || !finite(*report.FetchedAt) || *report.FetchedAt <= 0 || report.Limits == nil {
			return nil, errShape
		}
		account, err := parseReport(report.Limits)
		if err != nil {
			return nil, err
		}
		if len(account.Observation.Buckets) == 0 {
			unmeasurable = true
			continue
		}
		account.Observation.MeasuredAt = time.UnixMilli(int64(*report.FetchedAt)).UTC()
		if slices.ContainsFunc(accounts, func(a Account) bool { return a.FP == account.FP }) {
			return nil, errShape
		}
		accounts = append(accounts, account)
	}
	switch {
	case len(accounts) > 0:
	case unmeasurable:
		return nil, quota.Fail(quota.ErrKindParse, "omp commandcode account has no usable window")
	default:
		return nil, quota.Fail(quota.ErrKindCredentials, "omp holds no commandcode account")
	}
	slices.SortFunc(accounts, func(a, b Account) int { return strings.Compare(a.FP, b.FP) })
	return accounts, nil
}

// parseReport reads one account's limits: the 5-hour and weekly windows, and
// the credit balance's remaining credits. Any other limit is not read. A
// pay-as-you-go account has only a balance, no window at all: it comes back
// with no bucket and the caller leaves it out. A window that is present but
// unreadable is errShape; a balance that is malformed is unknown (nil), never
// zero (decision 403).
func parseReport(limits []ompLimit) (Account, error) {
	var account Account
	id := ""
	for _, limit := range limits {
		if limit.Scope.AccountID == "" {
			return Account{}, errShape
		}
		if id != "" && limit.Scope.AccountID != id {
			return Account{}, errShape
		}
		id = limit.Scope.AccountID
	}
	if id == "" {
		return Account{}, errShape
	}
	account.FP = Fingerprint(id)
	account.Observation = quota.Observation{Source: quota.SourceOmpUsage}
	seen := map[string]bool{}
	balanceSeen := false
	for _, limit := range limits {
		if limit.ID == provider+":balance" {
			// Remaining credits, not a window. A second balance row, or one
			// whose amount cannot be read, is unknown: never zero.
			if balanceSeen {
				account.Observation.Balance = nil
				continue
			}
			balanceSeen = true
			if limit.Amount != nil && len(limit.Amount.Remaining) > 0 {
				var remaining *float64
				if json.Unmarshal(limit.Amount.Remaining, &remaining) == nil && remaining != nil && finite(*remaining) && *remaining >= 0 {
					value := *remaining
					account.Observation.Balance = &value
				}
			}
			continue
		}
		window, duration := "", int64(0)
		switch limit.ID {
		case provider + ":5h":
			window, duration = "5h", fiveHourMs
		case provider + ":7d":
			window, duration = "7d", weeklyMs
		default:
			continue
		}
		// The window is named by id and length; a length omp changed is not the
		// window this package paces.
		if seen[window] || limit.Window == nil || limit.Window.DurationMs == nil || *limit.Window.DurationMs != float64(duration) || limit.Amount == nil {
			return Account{}, errShape
		}
		seen[window] = true
		pct, ok := usedPct(limit)
		if !ok {
			// A recognized window whose amount cannot be read is an output this
			// package does not know: never a partial reading that would hide
			// the window (an exhausted week beside a valid 5h).
			return Account{}, errShape
		}
		bucket := quota.Bucket{Window: window, UsedPct: pct, Scope: quota.Scope{Kind: quota.ScopeAccount}}
		if limit.Window.ResetsAt != nil {
			bucket.ResetsAt = epochTime(*limit.Window.ResetsAt)
		}
		account.Observation.Buckets = append(account.Observation.Buckets, bucket)
	}
	return account, nil
}

// usedPct is a limit's used share in percent, clamped to 100 past the cap. An
// exhausted status is 100 whatever the numbers say; a limit with neither a valid
// fraction nor a positive limit and a used value has none.
func usedPct(limit ompLimit) (*float64, bool) {
	amount := limit.Amount
	if limit.Status == "exhausted" {
		return quota.Pct(100), true
	}
	switch {
	case amount.UsedFraction != nil:
		// A fraction that is present decides: an invalid one is not papered over
		// by used/limit.
		if finite(*amount.UsedFraction) && *amount.UsedFraction >= 0 {
			return quota.Pct(min(100**amount.UsedFraction, 100)), true
		}
		return nil, false
	case amount.Used != nil && amount.Limit != nil && finite(*amount.Used) && finite(*amount.Limit) && *amount.Limit > 0 && *amount.Used >= 0:
		return quota.Pct(min(100**amount.Used / *amount.Limit, 100)), true
	}
	return nil, false
}

func finite(n float64) bool { return !math.IsNaN(n) && !math.IsInf(n, 0) }

// epochTime is an epoch-millisecond reset as RFC 3339; an absent, non-positive or
// implausible (outside 2000..2200) time is "".
func epochTime(ms float64) string {
	if !finite(ms) || ms <= 0 || ms > 9.2e15 {
		return ""
	}
	at := time.UnixMilli(int64(ms)).UTC()
	if at.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) || at.After(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return ""
	}
	return at.Format(time.RFC3339Nano)
}
