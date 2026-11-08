package fleet

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditStatusDoc is a pacelever status document with both credit pools, for
// the path that still runs `pacelever --json`.
const creditStatusDoc = `{"schema":"scopefuel.v1","generated_at":"2026-11-08T03:00:00Z","providers":[
 {"id":"anthropic-api","status":"ok","pool_class":"spend","buckets":[]},
 {"id":"openai-api","status":"ok","pool_class":"spend","buckets":[]}]}`

// creditPoolApp is an App whose credit pools read the shared store through
// latest, with pacelever's pool policy from policyTOML. Every pacelever run is
// recorded and answered with creditStatusDoc.
func creditPoolApp(t *testing.T, policyTOML string, latest func(string) shared.Latest) (*App, *fakeRunner) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(policyTOML), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", path)
	runner := &fakeRunner{Do: func(c Command) Result {
		if len(c.Args) == 1 && c.Args[0] == "--json" {
			return Result{Out: creditStatusDoc}
		}
		return Result{Code: 1, Err: "unexpected command"}
	}}
	a := &App{Home: t.TempDir(), Now: func() time.Time { return launchNow }, Runner: runner, Err: io.Discard, Config: Config{Quota: map[string]any{}, Local: map[string]any{}}}
	cli := CLIQuota{Runner: runner, Binary: "pacelever", Accounts: map[string]string{"anthropic-api": "0123456789abcdef"}}
	if latest != nil {
		cli.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) { return latest(key), nil }
	}
	a.Quota = admissionQuota{cli, a}
	return a, runner
}

// statusRuns counts `pacelever --json` runs.
func statusRuns(r *fakeRunner) int {
	n := 0
	for _, c := range r.Calls {
		if len(c.Args) == 1 && c.Args[0] == "--json" {
			n++
		}
	}
	return n
}

// creditEntry is an operator credit entry: used percent, valid for a week.
func creditEntry(used float64) shared.Latest {
	measured, expires := launchNow.Add(-time.Hour), launchNow.Add(7*24*time.Hour)
	return shared.Latest{Manual: []shared.Observation{{ID: 7, Kind: "manual", Window: "credit", MeasuredAt: measured, ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: used, Scope: "account"}}}}}
}

// excludedAnthropicPolicy is pacelever's pool policy with the operator's
// exclusion of anthropic-api, which creditStatusDoc does not carry.
const excludedAnthropicPolicy = "[pools.anthropic-api]\nclass = \"exclude\"\nuntil = \"2026-12-01\"\n"

// anthropicCreditLatest serves anthropic-api's operator credit entry at 40%.
func anthropicCreditLatest(key string) shared.Latest {
	if key == "anthropic-api:0123456789abcdef" {
		return creditEntry(40)
	}
	return shared.Latest{}
}

// TestCreditPoolViewsRunNoPacelever: with `[quota].launch = "native"`, every
// credit pool a spawn reads is a hand-recorded pool whose entries come from
// the shared store, so its view — the operator's class from the pool policy,
// the balance and the used share — is built without starting
// `pacelever --json`.
func TestCreditPoolViewsRunNoPacelever(t *testing.T) {
	a, runner := creditPoolApp(t, excludedAnthropicPolicy, anthropicCreditLatest)
	a.Config.Quota["launch"] = launchModeNative
	err := a.creditPoolStatus(context.Background(), "anthropic-api")
	if exitCode(err) != exitSeatRefused || !strings.Contains(err.Error(), "credit pool anthropic-api is excluded") {
		t.Fatalf("the operator's exclusion must refuse: %v", err)
	}
	if err := a.creditPoolStatus(context.Background(), "openai-api"); err != nil {
		t.Fatalf("an unexcluded credit pool admits: %v", err)
	}
	views, err := a.Quota.Pools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	anthropic := views["anthropic-api"]
	if anthropic.Class != "exclude" || anthropic.Blocking == nil || *anthropic.Blocking != 40 || anthropic.Credit == nil || anthropic.Credit.LeftPct != 60 {
		t.Fatalf("anthropic-api view = %+v", anthropic)
	}
	if views["openai-api"].Class != "spend" {
		t.Fatalf("openai-api view = %+v", views["openai-api"])
	}
	if len(runner.Calls) != 0 {
		t.Fatalf("pacelever ran: %v", runner.Calls)
	}
}

// TestCreditPoolViewsKeepPaceleverWhenNeeded: even with
// `[quota].launch = "native"`, a credit pool the shared store does not serve
// keeps the `pacelever --json` call as it is.
func TestCreditPoolViewsKeepPaceleverWhenNeeded(t *testing.T) {
	t.Run("no shared reader", func(t *testing.T) {
		a, runner := creditPoolApp(t, "", nil)
		a.Config.Quota["launch"] = launchModeNative
		if _, err := a.Quota.Pools(context.Background()); err != nil {
			t.Fatal(err)
		}
		if statusRuns(runner) != 1 {
			t.Fatalf("pacelever --json must run once: %v", runner.Calls)
		}
	})
	t.Run("credit pool measured by pacelever", func(t *testing.T) {
		a, runner := creditPoolApp(t, "", func(string) shared.Latest { return shared.Latest{} })
		a.Config.Quota["launch"] = launchModeNative
		policy := strings.Replace(string(defaultPoolOrder), `"credit_pools": ["anthropic-api", "openai-api"]`, `"credit_pools": ["anthropic-api", "openai-api", "kiro"]`, 1)
		if policy == string(defaultPoolOrder) {
			t.Fatal("pool policy fixture did not change")
		}
		path := filepath.Join(t.TempDir(), "pool-order.json")
		if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
			t.Fatal(err)
		}
		a.Config.Local["pool_policy"] = path
		if _, err := a.Quota.Pools(context.Background()); err != nil {
			t.Fatal(err)
		}
		if statusRuns(runner) != 1 {
			t.Fatalf("pacelever --json must run once: %v", runner.Calls)
		}
	})
}

// TestCreditPoolViewsFollowLaunchSwitch: the native credit pool view is the
// `[quota].launch = "native"` host's alone. By default (and with "shadow")
// the views are pacelever's status document, read by exactly one
// `pacelever --json` run, even when the shared store could serve every credit
// pool — the default on every host does not change (docs/quota.md §Credit
// pool views).
func TestCreditPoolViewsFollowLaunchSwitch(t *testing.T) {
	for _, mode := range []string{"", launchModeShadow} {
		t.Run("launch="+mode, func(t *testing.T) {
			a, runner := creditPoolApp(t, excludedAnthropicPolicy, anthropicCreditLatest)
			if mode != "" {
				a.Config.Quota["launch"] = mode
			}
			views, err := a.Quota.Pools(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if statusRuns(runner) != 1 {
				t.Fatalf("pacelever --json must run once, got %v", runner.Calls)
			}
			if views["anthropic-api"].Class != "spend" || views["anthropic-api"].Blocking != nil {
				t.Fatalf("the views must be pacelever's: anthropic-api = %+v", views["anthropic-api"])
			}
		})
	}
	t.Run("launch=native", func(t *testing.T) {
		a, runner := creditPoolApp(t, excludedAnthropicPolicy, anthropicCreditLatest)
		a.Config.Quota["launch"] = launchModeNative
		if _, err := a.Quota.Pools(context.Background()); err != nil {
			t.Fatal(err)
		}
		if statusRuns(runner) != 0 {
			t.Fatalf("native mode must not run pacelever --json: %v", runner.Calls)
		}
	})
}
