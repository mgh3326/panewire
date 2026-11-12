package devin

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// measureWithProbe measures with a fake devin TUI and virtual time.
func measureWithProbe(t *testing.T, cfg Config, boot string, script func(string, *fakeTerminal)) (quota.Observation, error, *fakeTerminal, *int) {
	t.Helper()
	clock := newFakeClock()
	term := newFakeTerminal(script)
	starts := 0
	cfg.Clock = clock
	booted := make(chan struct{})
	cfg.StartProbe = func(_ context.Context, _, _ string, _ []string) (Terminal, error) {
		starts++
		go func() {
			<-clock.nowCalls // the probe started its clock
			term.emit(boot)
			<-clock.nowCalls // and read the boot screen
			close(booted)
		}()
		return term, nil
	}
	type result struct {
		o   quota.Observation
		err error
	}
	done := make(chan result, 1)
	go func() {
		o, err := Measure(t.Context(), cfg)
		done <- result{o, err}
	}()
	select {
	case <-booted: // virtual time starts once the boot screen has been read
	case r := <-done:
		return r.o, r.err, term, &starts
	}
	for range 2000 {
		select {
		case r := <-done:
			return r.o, r.err, term, &starts
		case clock.ticks <- clock.Now():
			clock.advance(probeTick)
		}
	}
	t.Fatal("measurement did not finish in 100 s of virtual time")
	return quota.Observation{}, nil, nil, nil
}

func neverCalled(t *testing.T, what string) doFunc {
	return func(*http.Request) (*http.Response, error) {
		t.Errorf("%s", what)
		return nil, errors.New("unexpected")
	}
}

// Auth missing: a host without a devin login measures nothing — no request, no
// probe — and answers credentials, the kind the login rule reads.
func TestMeasureWithoutALoginRunsAndSendsNothing(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"no files":              {},
		"organization only":     without(loginFiles(), "/home/u/.local/share/devin/credentials.toml"),
		"blank key":             {"/home/u/.local/share/devin/credentials.toml": "windsurf_api_key = '   '\n", "/home/u/.config/devin/config.json": `{"devin":{"org_id":"org-fixture"}}`},
		"malformed credentials": {"/home/u/.local/share/devin/credentials.toml": "[", "/home/u/.config/devin/config.json": `{"devin":{"org_id":"org-fixture"}}`},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config(files, withToken())
			cfg.HTTP = neverCalled(t, "a request was sent without a login")
			cfg.StartProbe = func(context.Context, string, string, []string) (Terminal, error) {
				t.Error("a probe was started without a login")
				return nil, errors.New("unexpected")
			}
			_, err := Measure(t.Context(), cfg)
			if kindOf(err) != quota.ErrKindCredentials || HasLogin(cfg) || AccountFP(cfg) != "" {
				t.Fatalf("err = %v login=%t fp=%q", err, HasLogin(cfg), AccountFP(cfg))
			}
		})
	}
}

// Success over the web source: pacelever's buckets, attributed to the
// configured organization, with the token only in the Authorization header.
func TestMeasureWebSource(t *testing.T) {
	cfg := config(loginFiles(), withToken())
	cfg.HTTP = reply(200, fixture(t, "web_flat.json"), nil)
	cfg.StartProbe = func(context.Context, string, string, []string) (Terminal, error) {
		t.Error("the probe ran although the web source answered")
		return nil, errors.New("unexpected")
	}
	o, err := Measure(t.Context(), cfg)
	if err != nil || o.Source != quota.SourceUsageAPI || o.Plan != "Teams" || o.AccountFP != fixtureFP {
		t.Fatalf("observation %+v err=%v", o, err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{{"1d", 25, "2026-11-07T00:00:00Z"}, {"7d", 55, "2026-11-12T16:13:20Z"}})
}

// Any web failure falls through to the probe silently, as pacelever's does —
// including a refused token and a rate limit — and the probe's reading carries
// the login's account.
func TestMeasureFallsBackToTheProbe(t *testing.T) {
	for name, web := range map[string]doFunc{
		"unauthorized": reply(401, "no", nil),
		"rate limited": reply(429, "slow", http.Header{"Retry-After": {"90"}}),
		"server error": reply(500, "boom", nil),
		"network":      func(*http.Request) (*http.Response, error) { return nil, errors.New("down") },
		"malformed":    reply(200, "<html>", nil),
		"no windows":   reply(200, "{}", nil),
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config(loginFiles(), withToken())
			cfg.HTTP = web
			o, err, term, starts := measureWithProbe(t, cfg, fixture(t, "tui_prompt_u276d.txt"), panelOnEnter(panelWeekly))
			if err != nil || *starts != 1 || len(term.typed()) != 2 {
				t.Fatalf("err=%v starts=%d typed=%q", err, *starts, term.typed())
			}
			if o.Source != quota.SourceCLI || o.AccountFP != fixtureFP {
				t.Fatalf("observation %+v", o)
			}
			if len(o.Buckets) != 1 || o.Buckets[0].Window != "7d" || *o.Buckets[0].UsedPct != 62 {
				t.Fatalf("buckets %+v", o.Buckets)
			}
		})
	}
}

// Without a bearer token or an organization setting there is no web source;
// the probe is the measurement.
func TestMeasureWithoutWebCredentialsUsesTheProbe(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no token":        config(loginFiles(), nil),
		"no organization": config(without(loginFiles(), "/home/u/.config/pacelever/config.toml"), withToken()),
	} {
		t.Run(name, func(t *testing.T) {
			cfg.HTTP = neverCalled(t, "a web request was sent without web credentials")
			o, err, _, starts := measureWithProbe(t, cfg, fixture(t, "tui_prompt_u276d.txt"), panelOnEnter(panelWeekly))
			if err != nil || *starts != 1 || o.Source != quota.SourceCLI {
				t.Fatalf("observation %+v err=%v starts=%d", o, err, *starts)
			}
		})
	}
}

// A web answer for another organization than the login's is returned with that
// organization's fingerprint, which the shared flow refuses as account_mismatch.
func TestMeasureWebOrganizationIsAttestedNotAssumed(t *testing.T) {
	files := loginFiles()
	files["/home/u/.config/pacelever/config.toml"] = "[pools.devin]\norg_id = 'org-other'\n"
	cfg := config(files, withToken())
	cfg.HTTP = reply(200, fixture(t, "web_flat.json"), nil)
	o, err := Measure(t.Context(), cfg)
	if err != nil || o.AccountFP != Fingerprint("org-other") || o.AccountFP == AccountFP(cfg) {
		t.Fatalf("fp %q login %q err=%v", o.AccountFP, AccountFP(cfg), err)
	}
}

// Timeout and failure of the probe: probe_error with the stage, never screen
// content, and malformed panels are parse_error.
func TestMeasureProbeFailures(t *testing.T) {
	cfg := config(loginFiles(), nil)
	cfg.ProbeTimeout = 2 * time.Second
	_, err, _, _ := measureWithProbe(t, cfg, fixture(t, "tui_no_prompt.txt"), nil)
	if kindOf(err) != quota.ErrKindProbe || !strings.Contains(err.Error(), "probe timeout stage=no-prompt") {
		t.Fatalf("timeout: %v", err)
	}
	cfg = config(loginFiles(), nil)
	_, err, _, _ = measureWithProbe(t, cfg, fixture(t, "tui_prompt_u276d.txt"), panelOnEnter("Usage\r\nWeekly: 120% used\r\n"))
	if kindOf(err) == "" {
		t.Fatalf("an out-of-range panel measured: %v", err)
	}
}

func TestAccountFingerprint(t *testing.T) {
	for _, test := range []struct {
		name, credentials, config, want string
	}{
		{"login", "windsurf_api_key = 'fixture-token'", `{"devin":{"org_id":"org-fixture"}}`, fixtureFP},
		{"rotated token", "windsurf_api_key = 'rotated-fixture-token'", `{"devin":{"org_id":"org-fixture"}}`, fixtureFP},
		{"other organization", "windsurf_api_key = 'fixture-token'", `{"devin":{"org_id":"org-other-fixture"}}`, Fingerprint("org-other-fixture")},
		{"token only", "windsurf_api_key = 'fixture-token'", `{}`, ""},
		{"no login", "", `{"devin":{"org_id":"org-fixture"}}`, ""},
		{"empty token", "windsurf_api_key = '   '", `{"devin":{"org_id":"org-fixture"}}`, ""},
		{"malformed credentials", "[", `{"devin":{"org_id":"org-fixture"}}`, ""},
		{"malformed identity", "windsurf_api_key = 'fixture-token'", `{`, ""},
		{"non-string identity", "windsurf_api_key = 'fixture-token'", `{"devin":{"org_id":12}}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config(map[string]string{"/home/u/.local/share/devin/credentials.toml": test.credentials, "/home/u/.config/devin/config.json": test.config}, nil)
			if got := AccountFP(cfg); got != test.want {
				t.Fatalf("fingerprint %q, want %q", got, test.want)
			}
		})
	}
}

// The XDG roots move the login files, as the CLI's do.
func TestLoginFollowsTheXDGRoots(t *testing.T) {
	files := map[string]string{
		"/data/devin/credentials.toml": "windsurf_api_key = 'k'",
		"/conf/devin/config.json":      `{"devin":{"org_id":"org-fixture"}}`,
	}
	cfg := config(files, map[string]string{"XDG_DATA_HOME": "/data", "XDG_CONFIG_HOME": "/conf"})
	if AccountFP(cfg) != fixtureFP || !HasLogin(cfg) {
		t.Fatalf("login not found under the XDG roots")
	}
}

func TestBinaryResolution(t *testing.T) {
	look := func(string) (string, error) { return "/on/path/devin", nil }
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"override", config(loginFiles(), map[string]string{"PACELEVER_DEVIN_BIN": "/override/devin"}), "/override/devin"},
		{"panewire override", config(loginFiles(), map[string]string{"PANEWIRE_DEVIN_BIN": "/new/devin", "PACELEVER_DEVIN_BIN": "/old/devin"}), "/new/devin"},
		{"settings", config(map[string]string{"/home/u/.config/pacelever/config.toml": "[pools.devin]\nbin = '~/bin/devin'\n"}, nil), "/home/u/bin/devin"},
		{"panewire settings", config(map[string]string{"/home/u/.config/panewire/config.toml": "[pools.devin]\nbin = '~/bin/new-devin'\n"}, nil), "/home/u/bin/new-devin"},
		{"configured", Config{Binary: "/configured/devin", LookupEnv: config(nil, nil).LookupEnv, ReadFile: config(nil, nil).ReadFile, Home: "/home/u"}, "/configured/devin"},
		{"path", config(nil, nil), "/on/path/devin"},
	}
	for _, test := range cases {
		cfg := test.cfg
		cfg.LookPath = look
		s, err := cfg.settings()
		if err != nil {
			t.Fatalf("%s settings: %v", test.name, err)
		}
		if got := cfg.binary(s); got != test.want {
			t.Errorf("%s: binary %q, want %q", test.name, got, test.want)
		}
	}
	none := config(nil, nil)
	s, err := none.settings()
	if err != nil {
		t.Fatal(err)
	}
	if got := none.binary(s); got != "devin" {
		t.Errorf("not found: binary %q", got)
	}
}

func TestDevinSettingsMissingExplicitDoesNotFallThroughToLegacy(t *testing.T) {
	legacy := "/home/u/.config/pacelever/config.toml"
	missing := filepath.Join(t.TempDir(), "missing.toml")
	cfg := config(map[string]string{legacy: "[pools.devin]\nbin='/legacy/devin'\n"}, map[string]string{
		"PANEWIRE_CONFIG": missing,
	})
	_, err := cfg.settings()
	if err == nil || !strings.Contains(err.Error(), "PANEWIRE_CONFIG") || !strings.Contains(err.Error(), missing) {
		t.Fatalf("missing explicit path error = %v", err)
	}
}

func TestDevinSettingsDoesNotReadScopefuelSnapshot(t *testing.T) {
	scopefuel := "/home/u/.config/scopefuel/config.toml"
	cfg := config(map[string]string{scopefuel: "[pools.devin]\nbin='/scopefuel/devin'\n"}, nil)
	s, err := cfg.settings()
	if err != nil || s.bin != "" || s.orgID != "" {
		t.Fatalf("scopefuel settings = %+v, err=%v", s, err)
	}
}

func TestDevinSettingsDoesNotIgnoreInvalidExplicitConfig(t *testing.T) {
	path := "/home/u/.config/panewire/config.toml"
	cfg := config(map[string]string{path: "[pools.devin\nbin = 'broken'"}, map[string]string{"PANEWIRE_CONFIG": path})
	_, err := cfg.settings()
	if err == nil || !strings.Contains(err.Error(), "PANEWIRE_CONFIG") || !strings.Contains(err.Error(), path) {
		t.Fatalf("invalid explicit config error = %v", err)
	}
}

func TestDevinSettingsMalformedImplicitConfigFailsInsteadOfUsingEmptySettings(t *testing.T) {
	path := "/home/u/.config/panewire/config.toml"
	cfg := config(map[string]string{path: "[pools.devin\nbin = 'broken'"}, nil)
	_, err := cfg.settings()
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("invalid implicit config error = %v", err)
	}
}

func TestMeasureReportsMissingConfigAsQuotaError(t *testing.T) {
	missing := "/explicit/missing.toml"
	cfg := config(loginFiles(), map[string]string{"PANEWIRE_CONFIG": missing})
	cfg.HTTP = neverCalled(t, "web request should not proceed with missing explicit config")
	cfg.StartProbe = func(context.Context, string, string, []string) (Terminal, error) {
		t.Fatal("probe should not proceed with missing explicit config")
		return nil, errors.New("unexpected")
	}
	_, err := Measure(t.Context(), cfg)
	var quotaErr *quota.Error
	if !errors.As(err, &quotaErr) || quotaErr.Kind != quota.ErrKindCredentials || !strings.Contains(quotaErr.Message, "PANEWIRE_CONFIG") || !strings.Contains(quotaErr.Message, missing) {
		t.Fatalf("missing config measurement error = %#v", err)
	}
}
