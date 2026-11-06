package devin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

type quotaObservation struct {
	o   quota.Observation
	err error
}

func kindOf(err error) string { return quota.KindOf(err) }

func parseFixtureWeb(t *testing.T, body string) quotaObservation {
	t.Helper()
	cfg := config(nil, nil)
	cfg.HTTP = reply(200, body, nil)
	o, err := cfg.fetchWeb(t.Context(), token, org)
	return quotaObservation{o, err}
}

func TestWebRequestShape(t *testing.T) {
	var seen *http.Request
	cfg := config(nil, nil)
	cfg.HTTP = doFunc(func(r *http.Request) (*http.Response, error) {
		seen = r
		return reply(200, fixture(t, "web_flat.json"), nil)(r)
	})
	o, err := cfg.fetchWeb(t.Context(), token, org)
	if err != nil || o.Source != quota.SourceUsageAPI {
		t.Fatalf("observation %+v err=%v", o, err)
	}
	if seen.URL.String() != "https://app.devin.ai/api/org-fixture/billing/quota/usage" || seen.Method != http.MethodGet {
		t.Fatalf("request %s %s", seen.Method, seen.URL)
	}
	if seen.Header.Get("Authorization") != "Bearer "+token || seen.Header.Get("x-cog-org-id") != org {
		t.Fatal("web headers missing")
	}
}

// Auth missing: no token, or an organization of the wrong shape, sends nothing.
func TestWebWithoutCredentialsSendsNothing(t *testing.T) {
	for name, creds := range map[string][2]string{
		"no token":          {"", org},
		"no organization":   {token, ""},
		"traversing org":    {token, "org/../../elsewhere"},
		"organization text": {token, "my-org"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config(nil, nil)
			cfg.HTTP = doFunc(func(*http.Request) (*http.Response, error) {
				t.Error("a request was sent without usable credentials")
				return nil, errors.New("unexpected")
			})
			_, err := cfg.fetchWeb(t.Context(), creds[0], creds[1])
			if kindOf(err) != quota.ErrKindCredentials {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestWebFailureKinds(t *testing.T) {
	for _, test := range []struct {
		name  string
		do    doFunc
		kind  string
		retry time.Duration
	}{
		{"unauthorized", reply(401, `{"token":"`+token+`","org":"`+org+`"}`, nil), quota.ErrKindAuthFailed, 0},
		{"forbidden", reply(403, "no", nil), quota.ErrKindAuthFailed, 0},
		{"rate limited with seconds", reply(429, "slow", http.Header{"Retry-After": {"90"}}), quota.ErrKindRateLimited, 90 * time.Second},
		{"rate limited without a header", reply(429, "slow", nil), quota.ErrKindRateLimited, 0},
		{"server error", reply(500, "boom", nil), quota.ErrKindHTTP, 0},
		{"network", func(*http.Request) (*http.Response, error) { return nil, errors.New("dial " + token) }, quota.ErrKindNetwork, 0},
		{"malformed JSON", reply(200, "<html>", nil), quota.ErrKindParse, 0},
		{"not an object", reply(200, "[1]", nil), quota.ErrKindParse, 0},
		{"trailing garbage", reply(200, `{"weekly_percentage":40}garbage`, nil), quota.ErrKindParse, 0},
		{"two objects", reply(200, `{"weekly_percentage":40}{"weekly_percentage":41}`, nil), quota.ErrKindParse, 0},
		{"no windows", reply(200, `{"unexpected":true}`, nil), quota.ErrKindParse, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config(nil, nil)
			cfg.HTTP = test.do
			_, err := cfg.fetchWeb(t.Context(), token, org)
			failure, ok := err.(*quota.Error)
			if !ok || failure.Kind != test.kind || failure.RetryAfter != test.retry {
				t.Fatalf("err = %#v, want kind %s retry %s", err, test.kind, test.retry)
			}
			for _, secret := range []string{token, org} {
				if strings.Contains(failure.Error(), secret) {
					t.Fatalf("%q leaked into %q", secret, failure.Error())
				}
			}
		})
	}
}

// A server error is not an authentication failure, and no hint about the
// credentials is attached to it.
func TestWebServerErrorIsNotAuth(t *testing.T) {
	cfg := config(nil, nil)
	cfg.HTTP = reply(500, `{}`, nil)
	_, err := cfg.fetchWeb(t.Context(), token, org)
	if kindOf(err) == quota.ErrKindAuthFailed || strings.Contains(err.Error(), "DEVIN_BEARER_TOKEN") {
		t.Fatalf("server error reported as authentication: %v", err)
	}
}

func TestWebIgnoresMalformedPercentages(t *testing.T) {
	for _, body := range []string{
		`{"daily_percentage":-5,"weekly_percentage":"nope"}`,
		`{"quota_usage":{"weekly_quota":{"remaining_percent":150}}}`,
		`{"quota_usage":{"weekly_quota":{"used":-1,"limit":10}}}`,
	} {
		var raw map[string]any
		_ = json.Unmarshal([]byte(body), &raw)
		if _, err := parseWeb(raw); kindOf(err) != quota.ErrKindParse {
			t.Fatalf("malformed percentage admitted: %s -> %v", body, err)
		}
	}
}

// The reference's boundaries: a value below one is a fraction, exactly one is
// one percent for a used value and a fraction (100% remaining) for a remaining
// one.
func TestWebPercentBoundaries(t *testing.T) {
	var raw map[string]any
	_ = json.Unmarshal([]byte(`{"quota_usage":{"daily_quota":{"remaining_percent":1},"weekly_quota":{"remaining_percent":0.5}}}`), &raw)
	o, err := parseWeb(raw)
	if err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{{"1d", 0, ""}, {"7d", 50, ""}})
	_ = json.Unmarshal([]byte(`{"quota_usage":{"daily_quota":{"used_percent":1},"weekly_quota":{"used_percent":0.5}}}`), &raw)
	if o, err = parseWeb(raw); err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{{"1d", 1, ""}, {"7d", 50, ""}})
}

// Trailing whitespace after the object is fine.
func TestWebBodyMayEndInWhitespace(t *testing.T) {
	cfg := config(nil, nil)
	cfg.HTTP = reply(200, `{"weekly_percentage":40}`+"\n \r\n", nil)
	if o, err := cfg.fetchWeb(t.Context(), token, org); err != nil || len(o.Buckets) != 1 {
		t.Fatalf("observation %+v err=%v", o, err)
	}
}

func TestWebCancelledRequestIsANetworkFailure(t *testing.T) {
	cfg := config(nil, nil)
	cfg.HTTP = doFunc(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cfg.fetchWeb(ctx, token, org); kindOf(err) != quota.ErrKindNetwork {
		t.Fatalf("err = %v", err)
	}
}
