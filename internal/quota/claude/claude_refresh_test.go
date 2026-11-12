package claude

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// refreshedToken is the access token the CLI run leaves in the store when it
// refreshed the login before it failed.
const refreshedToken = "sk-ant-oat01-refreshed-token-must-not-leak"

const refreshUsageBody = `{"five_hour":{"utilization":42,"resets_at":"2026-11-05T15:00:00Z"},"seven_day":{"utilization":10}}`

// expiredThenRefreshed is a login whose stored token expired. The CLI run
// rewrites the store to a current token, then returns runErr or runOut.
func expiredThenRefreshed(t *testing.T, api doFunc, runOut string, runErr error) (Config, *int, map[string]string) {
	t.Helper()
	files := map[string]string{
		"/home/u/.claude/.credentials.json": credentialFile(secretToken, testNow.Add(-time.Hour)),
		"/home/u/.claude.json":              `{"oauthAccount":{"accountUuid":"00000000-0000-4000-8000-000000000001"}}`,
	}
	cfg := testConfig(t, api)
	cfg.ReadFile = login{files: files}.read
	cli := new(int)
	cfg.RunCLI = func(context.Context, string, string, string) (string, error) {
		*cli++
		files["/home/u/.claude/.credentials.json"] = credentialFile(refreshedToken, testNow.Add(time.Hour))
		return runOut, runErr
	}
	return cfg, cli, files
}

// An expired stored token whose CLI fallback fails after it refreshed the
// login is a live login: one usage request with the refreshed token measures
// it. The CLI runs once and the API once.
func TestExpiredTokenRefreshedByFailingCLIMeasuresThroughTheAPI(t *testing.T) {
	for name, run := range map[string]struct {
		out string
		err error
	}{
		"cli error":      {"", errors.New("no claude")},
		"unparsable out": {"garbage", nil},
	} {
		t.Run(name, func(t *testing.T) {
			var auth []string
			cfg, cli, _ := expiredThenRefreshed(t, doFunc(func(r *http.Request) (*http.Response, error) {
				auth = append(auth, r.Header.Get("Authorization"))
				return respond(200, refreshUsageBody, nil), nil
			}), run.out, run.err)
			o, err := Measure(t.Context(), cfg)
			if err != nil {
				t.Fatalf("err = %v, want the API reading", err)
			}
			if *cli != 1 || len(auth) != 1 || auth[0] != "Bearer "+refreshedToken {
				t.Fatalf("cli calls = %d, API calls = %d, want one of each with the refreshed token", *cli, len(auth))
			}
			if o.Source != quota.SourceUsageAPI || len(o.Buckets) != 2 {
				t.Fatalf("observation source=%s buckets=%d, want the usage API reading", o.Source, len(o.Buckets))
			}
		})
	}
}

// When the refreshed token's usage request fails, that failure keeps its own
// kind: a refused token is auth, and network, http, rate_limited and parse stay
// unknown rather than token_expired. The CLI still runs once.
func TestExpiredTokenRefreshedByFailingCLIKeepsTheAPIFailureKind(t *testing.T) {
	for _, tt := range []struct {
		name string
		do   doFunc
		kind string
	}{
		{"refused", func(*http.Request) (*http.Response, error) { return respond(401, "no", nil), nil }, quota.ErrKindAuth},
		{"forbidden", func(*http.Request) (*http.Response, error) { return respond(403, "no", nil), nil }, quota.ErrKindAuth},
		{"rate limited", func(*http.Request) (*http.Response, error) { return respond(429, "slow", nil), nil }, quota.ErrKindRateLimited},
		{"server error", func(*http.Request) (*http.Response, error) { return respond(500, "boom", nil), nil }, quota.ErrKindHTTP},
		{"network", func(*http.Request) (*http.Response, error) { return nil, errors.New("dial failed") }, quota.ErrKindNetwork},
		{"unparsable", func(*http.Request) (*http.Response, error) { return respond(200, "{}", nil), nil }, quota.ErrKindParse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, cli, _ := expiredThenRefreshed(t, tt.do, "", errors.New("no claude"))
			_, err := Measure(t.Context(), cfg)
			if got := quota.KindOf(err); got != tt.kind {
				t.Fatalf("kind = %q, want %q", got, tt.kind)
			}
			if *cli != 1 {
				t.Fatalf("cli calls = %d, want 1", *cli)
			}
			if msg := err.Error(); containsAny(msg, secretToken, refreshedToken) {
				t.Fatal("a token reached the error")
			}
		})
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
