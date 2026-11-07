package fleet

import (
	"path/filepath"
	"testing"
	"time"
)

// agy has a native/shadow switch like commandcode and devin.
func TestAgyHasSwitchAndDefaultsToShadow(t *testing.T) {
	if !QuotaPoolHasSwitch("agy") {
		t.Fatal("agy has no switch")
	}
	for quota, want := range map[string]bool{
		"":                                false,
		`native_pools = []`:               false,
		`native_pools = ["agy"]`:          true,
		`native_pools = ["devin"]`:        false,
		`native_pools = ["agy", "devin"]`: true,
	} {
		config, err := LoadConfig(writeHostsConfig(t, "[quota]\n"+quota+"\n"))
		if err != nil {
			t.Fatalf("%q: %v", quota, err)
		}
		if got := IsNativeQuotaPool("agy", config.Quota); got != want {
			t.Errorf("%q: native = %v, want %v", quota, got, want)
		}
	}
}

// The agy account is configured (pacelever's `[quota.accounts]` and
// `measurable_accounts`): the host measures it only when both name it, and
// never guesses a key.
func TestHostQuotaAccountReadsTheConfiguredAgyAccount(t *testing.T) {
	const fp = "0123456789abcdef"
	for name, c := range map[string]struct {
		body       string
		wantFP     string
		measurable bool
	}{
		"nothing configured":      {"[quota]\nfresh_ttl_s = 300\n", "", false},
		"account only":            {"[quota.accounts]\nagy = \"" + fp + "\"\n", fp, false},
		"measurable only":         {"[quota]\nmeasurable_accounts = [\"agy:" + fp + "\"]\n", "", false},
		"account and measurable":  {"[quota]\nmeasurable_accounts = [\"agy:" + fp + "\"]\n[quota.accounts]\nagy = \"" + fp + "\"\n", fp, true},
		"measurable another key":  {"[quota]\nmeasurable_accounts = [\"agy:fedcba9876543210\"]\n[quota.accounts]\nagy = \"" + fp + "\"\n", fp, false},
		"another pool measurable": {"[quota]\nmeasurable_accounts = [\"grok:" + fp + "\"]\n[quota.accounts]\nagy = \"" + fp + "\"\n", fp, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("FLEET_HOSTS_CONFIG", writeHostsConfig(t, c.body))
			gotFP, measurable := HostQuotaAccount("agy")
			if measurable != c.measurable || (c.measurable || c.wantFP != "") && gotFP != c.wantFP {
				t.Fatalf("HostQuotaAccount = %q, %v; want %q, %v", gotFP, measurable, c.wantFP, c.measurable)
			}
		})
	}
	t.Setenv("FLEET_HOSTS_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
	if gotFP, measurable := HostQuotaAccount("agy"); gotFP != "" || measurable {
		t.Fatalf("a missing config names an account: %q, %v", gotFP, measurable)
	}
}

func TestLoadConfigValidatesMeasurableAccounts(t *testing.T) {
	for body, ok := range map[string]bool{
		"[quota]\nmeasurable_accounts = [\"agy:0123456789abcdef\"]\n":                              true,
		"[quota]\nmeasurable_accounts = []\n":                                                      true,
		"[quota]\nmeasurable_accounts = \"agy:0123456789abcdef\"\n":                                false,
		"[quota]\nmeasurable_accounts = [\"agy\"]\n":                                               false,
		"[quota]\nmeasurable_accounts = [\"agy:0123\"]\n":                                          false,
		"[quota]\nmeasurable_accounts = [7]\n":                                                     false,
		"[quota]\nmeasurable_accounts = [\"agy:0123456789abcdef\", \"claude:fedcba9876543210\"]\n": true,
	} {
		_, err := LoadConfig(writeHostsConfig(t, body))
		if (err == nil) != ok {
			t.Errorf("%q: err = %v, want ok=%v", body, err, ok)
		}
	}
}

// A group-only reading keeps the whole-account view (the strictest group, for a
// rung that maps to no group) and adds one view per group, each ranked on that
// group's windows alone.
func TestAccountPoolViewGroupViews(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reset5h := now.Add(4 * time.Hour).Format(time.RFC3339)
	reset7d := now.Add(4 * 24 * time.Hour).Format(time.RFC3339)
	buckets := []accountBucket{
		{Scope: "group", Model: "gemini", UsedPct: ptrF(100), Window: "5h", ResetsAt: reset5h},
		{Scope: "group", Model: "3p", UsedPct: ptrF(20), Window: "5h", ResetsAt: reset5h},
		{Scope: "group", Model: "3p", UsedPct: ptrF(30), Window: "7d", ResetsAt: reset7d},
	}
	v := accountPoolView(buckets, now)
	if v == nil || v.Blocking == nil || *v.Blocking != 100 {
		t.Fatalf("the account view is the strictest group: %+v", v)
	}
	gemini, third := v.Groups["gemini"], v.Groups["3p"]
	if gemini.Blocking == nil || *gemini.Blocking != 100 || gemini.Urgency != nil {
		t.Fatalf("gemini view %+v", gemini)
	}
	if third.Blocking == nil || *third.Blocking != 30 || third.Urgency == nil {
		t.Fatalf("3p view %+v", third)
	}
	if len(v.Groups) != 2 {
		t.Fatalf("groups %v", v.Groups)
	}
	// An account-scoped reading has no group views.
	if v := accountPoolView([]accountBucket{{Scope: "account", UsedPct: ptrF(10), Window: "7d", ResetsAt: reset7d}}, now); v == nil || v.Groups != nil {
		t.Fatalf("account reading has group views: %+v", v)
	}
	// One invalid bucket in any group voids the whole reading, as before.
	bad := append([]accountBucket{{Scope: "group", Model: "gemini", UsedPct: ptrF(101), Window: "5h", ResetsAt: reset5h}}, buckets[1:]...)
	if accountPoolView(bad, now) != nil {
		t.Fatal("an invalid group bucket was ranked")
	}
}
