package devin

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/mgh3326/panewire/internal/quota"
)

const (
	webOrigin    = "https://app.devin.ai"
	webTimeout   = 20 * time.Second
	webBodyLimit = 2 << 20
)

// orgPattern is the organization id shape app.devin.ai uses. Rejecting other
// values keeps a malformed setting out of the request path and headers.
var orgPattern = regexp.MustCompile(`^org[-_][A-Za-z0-9_-]+$`)

// fetchWeb reads the organization quota from app.devin.ai: the read-only web
// source from ClaudeBar (MIT, tddworks/ClaudeBar). The credential is limited to
// the bearer token and the organization setting; no browser storage is read.
// Every failure is a kind plus a fixed sentence, never the response, the token
// or the organization.
func (c Config) fetchWeb(ctx context.Context, token, org string) (quota.Observation, error) {
	if token == "" || !orgPattern.MatchString(org) {
		return quota.Observation{}, quota.Fail(quota.ErrKindCredentials, "credentials unavailable")
	}
	origin := c.Endpoint
	if origin == "" {
		origin = webOrigin
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: webTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(origin, "/")+"/api/"+url.PathEscape(org)+"/billing/quota/usage", nil)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("x-cog-org-id", org)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "panewire")
	response, err := client.Do(request)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindNetwork, "usage request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		failure := quota.StatusError(response.StatusCode, response.Header, c.now(), 0)
		if failure.Kind == quota.ErrKindAuth {
			failure = quota.Fail(quota.ErrKindAuthFailed, "devin authentication failed; set DEVIN_BEARER_TOKEN and pools.devin.org_id")
		}
		return quota.Observation{}, failure
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, webBodyLimit))
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "invalid usage JSON")
	}
	raw, ok := decodeBody(body)
	if !ok {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "invalid usage JSON")
	}
	return parseWeb(raw)
}

func accountBucket(window string, used *float64, reset string) quota.Bucket {
	return quota.Bucket{Window: window, UsedPct: used, ResetsAt: reset, Scope: quota.Scope{Kind: quota.ScopeAccount}}
}

func parseWeb(raw map[string]any) (quota.Observation, error) {
	observed := quota.Observation{Source: quota.SourceUsageAPI}
	nested := object(raw["quota_usage"])
	if raw["hide_daily_quota"] != true {
		if used, reset, ok := window(raw["daily_percentage"], raw["daily_reset_at"], nested["daily_quota"]); ok {
			observed.Buckets = append(observed.Buckets, accountBucket("1d", quota.Pct(used), reset))
		}
	}
	if used, reset, ok := window(raw["weekly_percentage"], raw["weekly_reset_at"], nested["weekly_quota"]); ok {
		observed.Buckets = append(observed.Buckets, accountBucket("7d", quota.Pct(used), reset))
	}
	observed.Plan = plan(raw)
	if len(observed.Buckets) == 0 {
		return quota.Observation{}, quota.Fail(quota.ErrKindParse, "usage response has no usable windows")
	}
	return observed, nil
}

// window reads one window from either the flat percentage shape or the nested
// quota_usage shape. A fraction below one is a percentage, matching the API and
// ClaudeBar's mapper.
func window(percentage, resetValue, nestedValue any) (float64, string, bool) {
	if used, ok := usedPercent(percentage); ok {
		return used, resetAtValue(resetValue), true
	}
	nested := object(nestedValue)
	if len(nested) == 0 {
		return 0, "", false
	}
	at := nested["reset_at"]
	if at == nil {
		at = nested["next_reset_at"]
	}
	reset := resetAtValue(at)
	percent := nested["used_percent"]
	if percent == nil {
		percent = nested["percentUsed"]
	}
	if used, ok := usedPercent(percent); ok {
		return used, reset, true
	}
	if remaining, ok := looseNumber(nested["remaining_percent"]); ok {
		remaining = remainingPercent(remaining)
		if remaining < 0 || remaining > 100 {
			return 0, "", false
		}
		return 100 - remaining, reset, true
	}
	if limit, ok := looseNumber(nested["limit"]); ok && limit > 0 {
		count, hasCount := looseNumber(nested["used"])
		if !hasCount {
			if remaining, ok := looseNumber(nested["remaining"]); ok {
				count, hasCount = limit-remaining, true
			}
		}
		if hasCount && count >= 0 {
			return clamp(100 * count / limit), reset, true
		}
	}
	return 0, "", false
}

// usedPercent applies the reference convention: a value below one is a
// fraction (0.12 is 12%), while exactly one is one percent. remainingPercent
// is intentionally inclusive at one, matching ClaudeBar's devin-quota.js.
func usedPercent(v any) (float64, bool) {
	n, ok := looseNumber(v)
	if !ok || n < 0 {
		return 0, false
	}
	if n < 1 {
		n *= 100
	}
	return clamp(n), true
}

func remainingPercent(n float64) float64 {
	if n <= 1 {
		return n * 100
	}
	return n
}

func clamp(n float64) float64 { return min(max(n, 0), 100) }

func plan(raw map[string]any) string {
	for _, key := range []string{"plan_name", "planName", "plan", "tier", "subscription_tier"} {
		name := strings.TrimSpace(str(raw[key]))
		if name == "" {
			continue
		}
		runes := []rune(name)
		runes[0] = unicode.ToUpper(runes[0])
		return string(runes)
	}
	return ""
}
