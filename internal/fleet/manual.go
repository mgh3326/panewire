package fleet

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// ManualObservation is one operator-entered quota reading. It is pacelever's
// manual store shape so a shared reader sees the same fields.
type ManualObservation struct {
	Extra              map[string]json.RawMessage `json:"-"`
	ID                 string                     `json:"manual_observation_id"`
	Event              string                     `json:"event"`
	Pool               string                     `json:"pool"`
	Window             string                     `json:"window"`
	UsedPct            *float64                   `json:"used_pct"`
	ResetsAt           string                     `json:"resets_at,omitempty"`
	MeasuredAt         string                     `json:"measured_at"`
	EnteredAt          string                     `json:"entered_at"`
	ExpiresAt          string                     `json:"expires_at"`
	Reason             string                     `json:"reason"`
	Source             string                     `json:"source"`
	SourceVerification string                     `json:"source_verification"`
	SourceLabel        string                     `json:"source_label"`
	SupersedesRef      *string                    `json:"supersedes_ref"`
	Credit             *ManualCredit              `json:"credit,omitempty"`
}

type ManualCredit struct {
	RemainingUSD float64 `json:"remaining_usd"`
	TotalUSD     float64 `json:"total_usd"`
	ExpiresOn    string  `json:"expires_on"`
}

type ManualStore struct {
	Schema  string              `json:"schema"`
	History []ManualObservation `json:"history"`
	Latest  map[string]string   `json:"latest"`
}

// manualPools is pacelever's known pool set for `manual list`: the pools
// panewire routes plus the two hand-recorded credit pools.
var manualPools = admissionPolicyPools

func knownManualPool(pool string) bool { return slices.Contains(manualPools, pool) }

func parseManualTime(value string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, value)
}

func parseCreditExpiry(value string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

func parseManualDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty duration")
	}
	unit := value[len(value)-1]
	number := value[:len(value)-1]
	n, err := parseManualNumber(number)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid duration")
	}
	switch unit {
	case 's':
		return time.Duration(n * float64(time.Second)), nil
	case 'm':
		return time.Duration(n * float64(time.Minute)), nil
	case 'h':
		return time.Duration(n * float64(time.Hour)), nil
	case 'd':
		return time.Duration(n * float64(24*time.Hour)), nil
	case 'w':
		return time.Duration(n * float64(7*24*time.Hour)), nil
	}
	return 0, fmt.Errorf("invalid duration")
}

func parseManualNumber(value string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(value), 64)
}

func manualPct(used float64) *float64 {
	if used < 0 || used > 100 {
		return nil
	}
	return &used
}

func validManualPct(pct *float64) bool { return pct != nil && *pct >= 0 && *pct <= 100 }

// prepareManual is pacelever's validation: pool and reason, default windows,
// TTL bounds, credit checks and the stamps a shared writer carries.
func prepareManual(m ManualObservation, ttl time.Duration, now time.Time) (ManualObservation, error) {
	if !knownManualPool(m.Pool) || m.Reason == "" {
		return m, fmt.Errorf("known pool and --reason required")
	}
	if m.Window == "" {
		switch m.Pool {
		case "grok", "devin":
			m.Window = "7d"
		case "kiro":
			m.Window = "30d"
		case "anthropic-api", "openai-api", "commandcode":
			m.Window = "credit"
		default:
			return m, fmt.Errorf("--window required")
		}
	}
	measured, err := parseManualTime(m.MeasuredAt)
	if err != nil || measured.After(now.Add(time.Minute)) {
		return m, fmt.Errorf("invalid or future --measured-at")
	}
	if ttl == 0 {
		ttl = 30 * time.Minute
	}
	if ttl < 0 || (m.Window != "credit" && ttl > 2*time.Hour) {
		return m, fmt.Errorf("manual TTL must be positive and at most 2h")
	}
	expires := measured.Add(ttl)
	if m.Window == "credit" {
		if m.Credit == nil || m.Credit.TotalUSD <= 0 || m.Credit.RemainingUSD < 0 || m.Credit.RemainingUSD > m.Credit.TotalUSD {
			return m, fmt.Errorf("credit requires valid --credit-balance, --credit-total and --expires")
		}
		expiry, err := parseCreditExpiry(m.Credit.ExpiresOn)
		if err != nil || !expiry.After(now) {
			return m, fmt.Errorf("credit already expired or invalid expiry")
		}
		m.UsedPct = manualPct(100 * (1 - m.Credit.RemainingUSD/m.Credit.TotalUSD))
		expires = expiry
	} else if !validManualPct(m.UsedPct) {
		return m, fmt.Errorf("--used must be 0..100")
	}
	if m.ResetsAt != "" {
		reset, err := parseManualTime(m.ResetsAt)
		if err != nil {
			return m, fmt.Errorf("invalid reset timestamp")
		}
		if reset.Before(expires) {
			expires = reset
		}
	}
	if !expires.After(now) {
		return m, fmt.Errorf("observation already expired")
	}
	m.EnteredAt = now.UTC().Format(time.RFC3339Nano)
	m.ExpiresAt = expires.UTC().Format(time.RFC3339Nano)
	m.Event = "set"
	m.Source = "operator"
	m.SourceVerification = "unverified"
	m.SourceLabel = "self-reported manual observation"
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return m, err
	}
	m.ID = "manual-" + hex.EncodeToString(b)
	return m, nil
}

// manualStoreValid is pacelever's ManualStore.Valid for one pool.
func manualStoreValid(s ManualStore, pool string, now time.Time) []ManualObservation {
	var out []ManualObservation
	for _, m := range s.History {
		if m.Pool != pool || m.Event != "set" || s.Latest[pool+":"+m.Window] != m.ID || !validManualPct(m.UsedPct) {
			continue
		}
		measured, err := parseManualTime(m.MeasuredAt)
		if err != nil || measured.After(now.Add(60*time.Second)) {
			continue
		}
		expiry, err := parseManualTime(m.ExpiresAt)
		if err != nil || !expiry.After(now) {
			continue
		}
		if m.Credit != nil {
			creditExpiry, err := parseCreditExpiry(m.Credit.ExpiresOn)
			if err != nil || !creditExpiry.After(now) {
				continue
			}
		}
		if reset, err := parseManualTime(m.ResetsAt); err == nil && !reset.After(now) {
			continue
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Window < out[j].Window })
	return out
}

// manualFromShared rebuilds pacelever's read shape from a hk manual
// observation. The shared wire carries no dollars; #309's configured total is
// the fallback for those.
func manualFromShared(xs []shared.Observation, pool string) []ManualObservation {
	out := []ManualObservation{}
	for _, x := range xs {
		if x.Kind != "manual" || x.ExpiresAt == nil || len(x.Buckets) != 1 ||
			x.Buckets[0].Scope != "account" || x.Buckets[0].Window != x.Window {
			continue
		}
		b := x.Buckets[0]
		m := ManualObservation{
			ID: fmt.Sprintf("hk-%d", x.ID), Pool: pool, Event: "set", Window: x.Window,
			UsedPct: manualPct(b.UsedPct), MeasuredAt: x.MeasuredAt.Format(time.RFC3339Nano),
			ExpiresAt: x.ExpiresAt.Format(time.RFC3339Nano), Source: "operator",
			SourceVerification: "unverified", SourceLabel: "shared manual observation",
		}
		if b.ResetsAt != nil {
			m.ResetsAt = b.ResetsAt.Format(time.RFC3339Nano)
		}
		out = append(out, m)
	}
	return out
}

func sharedManualObservation(m ManualObservation) (shared.Observation, error) {
	measured, err := parseManualTime(m.MeasuredAt)
	if err != nil {
		return shared.Observation{}, err
	}
	expires, err := parseManualTime(m.ExpiresAt)
	if err != nil {
		return shared.Observation{}, err
	}
	bucket := shared.Bucket{Window: m.Window, UsedPct: *m.UsedPct, Scope: "account"}
	if m.ResetsAt != "" {
		reset, err := parseManualTime(m.ResetsAt)
		if err != nil {
			return shared.Observation{}, err
		}
		bucket.ResetsAt = &reset
	}
	return shared.Observation{
		Kind: "manual", Window: m.Window, MeasuredAt: measured,
		ExpiresAt: &expires, Buckets: []shared.Bucket{bucket},
	}, nil
}

func manualAccountKey(a *App, pool string) (string, error) {
	accounts, _ := a.Config.Quota["accounts"].(map[string]any)
	fp := str(accounts, pool)
	if !accountFP.MatchString(fp) {
		return "", fmt.Errorf("shared account fingerprint required")
	}
	return pool + ":" + fp, nil
}

func manualHolder() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
