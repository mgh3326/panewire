package panewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 3857: each host proves its own harness login. The probe and the rc=76
// login-screen rule are both node-side; fakes only (docs/fleet-state.md,
// "Login probe").

const loginProbeInterval = 10 * time.Minute

type loginProbeRig struct {
	client *HubClient
	hk     *claudeHK
	api    *agyAPI
	files  map[string]string
	warned *warnLog
	clock  time.Time
	sent   map[string]fleetQuotaSent
}

// newLoginProbeRig is an agy node whose shared reading is fresh (another host
// published it), whose own login files are files, and whose probe interval is
// interval. measurable says whether this host lists the agy account.
func newLoginProbeRig(t *testing.T, files map[string]string, measurable bool, interval time.Duration) *loginProbeRig {
	t.Helper()
	hk := newClaudeHK(t)
	reset := claudeTestNow.Add(2 * time.Hour).UTC().Format(time.RFC3339)
	hk.latest = json.RawMessage(fmt.Sprintf(`{"kind":"auto","measured_at":%q,"buckets":[{"window":"5h","used_pct":10,"resets_at":%q,"scope":"group","model":"3p"}]}`,
		claudeTestNow.UTC().Format(time.RFC3339), reset))
	client := quotaClient(t)
	rig := &loginProbeRig{client: client, hk: hk, api: newAgyAPI(), files: files, clock: claudeTestNow, sent: map[string]fleetQuotaSent{}}
	client.now = func() time.Time { return rig.clock }
	client.spawnConfigPath = claudeStore(t, hk.server.URL)
	client.nativeQuota = map[string]*nativeQuotaSource{"agy": agyTestSource(client.spawnConfigPath, files, rig.api, agyFPForTest, measurable)}
	client.loginProbeIntervalOf = func() time.Duration { return interval }
	rig.warned = newWarnLog(client)
	return rig
}

// round is one pass of the node quota loop over the agy pool.
func (r *loginProbeRig) round(t *testing.T) {
	t.Helper()
	r.client.refreshFleetQuota(t.Context(), "agy", r.sent)
}

func (r *loginProbeRig) login(t *testing.T) string {
	t.Helper()
	return poolLogin(t, r.client, "agy").Login
}

// loginLines are the warnings that report a login state change.
func (r *loginProbeRig) loginLines() []string {
	var lines []string
	for _, line := range r.warned.all() {
		if strings.Contains(line, "login") {
			lines = append(lines, line)
		}
	}
	return lines
}

// (a) With the shared reading fresh, the loop alone never measures: only the
// probe shows this host's credential failing, and a passing credential keeps the
// host ready. The probe publishes nothing.
func TestLoginProbeMarksLoginMissingBehindAFreshSharedReading(t *testing.T) {
	files := agyLoginFiles("") // the host has no usable credential
	rig := newLoginProbeRig(t, files, true, loginProbeInterval)

	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginMissing {
		t.Fatalf("login after the first probe = %q, want missing", got)
	}
	if rig.api.count() != 0 {
		t.Fatalf("a host with no credential reached the provider: %d requests", rig.api.count())
	}
	rig.clock = rig.clock.Add(loginProbeInterval / 2)
	rig.round(t)
	if lines := rig.loginLines(); len(lines) != 1 || strings.Contains(strings.Join(lines, "\n"), agyToken) {
		t.Fatalf("login log lines = %q, want one state-change line", lines)
	}

	// The owner logs in. The probe is not due yet, so nothing changes; one
	// interval after the first probe it passes and clears the state.
	for path, body := range agyLoginFiles(agyToken) {
		files[path] = body
	}
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginMissing || rig.api.count() != 0 {
		t.Fatalf("probed before its interval: login=%q requests=%d", got, rig.api.count())
	}
	rig.clock = rig.clock.Add(loginProbeInterval / 2)
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("login after a passing probe = %q, want present", got)
	}
	if rig.api.count() == 0 {
		t.Fatal("the passing probe never measured")
	}
	if lines := rig.loginLines(); len(lines) != 2 {
		t.Fatalf("login log lines = %q, want one per state change", lines)
	}

	rig.hk.mu.Lock()
	defer rig.hk.mu.Unlock()
	for _, call := range rig.hk.calls {
		if strings.HasPrefix(call, "PUT ") && strings.HasSuffix(call, "/observation") || strings.HasPrefix(call, "POST ") && strings.HasSuffix(call, "/lease") {
			t.Fatalf("the probe used the shared store beyond reads and presence: %s", call)
		}
	}
	if len(rig.hk.pubs) != 0 {
		t.Fatalf("the probe published %d readings", len(rig.hk.pubs))
	}
}

func TestLoginProbeWithAWorkingCredentialStaysReady(t *testing.T) {
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, loginProbeInterval)
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("login = %q, want present", got)
	}
	if rig.api.count() == 0 {
		t.Fatal("the probe never measured with the host's credential")
	}
	if len(rig.hk.pubs) != 0 {
		t.Fatalf("the probe published %d readings", len(rig.hk.pubs))
	}
	if lines := rig.loginLines(); len(lines) != 0 {
		t.Fatalf("a probe that changed nothing logged %q", lines)
	}
}

// (b) A host with no measurable account for the pool is never probed and never
// marked login_missing by the probe (hk 3783).
func TestLoginProbeSkipsAHostWithNoMeasurableAccount(t *testing.T) {
	for name, tc := range map[string]struct {
		files      map[string]string
		fp         string
		measurable bool
	}{
		"account not listed": {agyLoginFiles(""), agyFPForTest, false},
		"account not set":    {agyLoginFiles(""), "", false},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newLoginProbeRig(t, tc.files, tc.measurable, loginProbeInterval)
			rig.client.nativeQuota["agy"] = agyTestSource(rig.client.spawnConfigPath, tc.files, rig.api, tc.fp, tc.measurable)
			rig.client.probeFleetLogin(t.Context(), "agy")
			rig.clock = rig.clock.Add(loginProbeInterval)
			rig.client.probeFleetLogin(t.Context(), "agy")
			if got := rig.login(t); got != fleet.ReadinessLoginUnknown {
				t.Fatalf("login = %q, an unmeasurable pool must stay unknown", got)
			}
			if rig.api.count() != 0 || len(rig.warned.all()) != 0 {
				t.Fatalf("probed: requests=%d warnings=%q", rig.api.count(), rig.warned.all())
			}
		})
	}
}

// The same gate for a pool whose account comes from its login (claude): only an
// account the host lists as measurable is probed.
func TestLoginProbeClaudeNeedsAListedAccount(t *testing.T) {
	for _, listed := range []bool{false, true} {
		t.Run(fmt.Sprintf("listed=%v", listed), func(t *testing.T) {
			client := quotaClient(t)
			client.loginProbeIntervalOf = func() time.Duration { return loginProbeInterval }
			var requests int
			source := claudeTestSource("", claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), func(r *http.Request) (*http.Response, error) {
				requests++
				return claudeAPIReply(200, claudeAPIBody)(r)
			})
			source.MeasurableAccount = func(key string) bool { return listed && strings.HasPrefix(key, "claude:") }
			client.nativeQuota = map[string]*nativeQuotaSource{"claude": source}
			client.probeFleetLogin(t.Context(), "claude")
			if probed := requests > 0; probed != listed {
				t.Fatalf("probed=%v with the account listed=%v", probed, listed)
			}
		})
	}
}

// (c) A network or timeout failure says nothing about the login.
func TestLoginProbeIgnoresFailuresThatAreNotLoginFailures(t *testing.T) {
	rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, loginProbeInterval)
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("login = %q, want present", got)
	}
	rig.client.nativeQuota["agy"].Agy.HTTP = claudeAPIFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") })
	rig.clock = rig.clock.Add(loginProbeInterval)
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("a network failure changed the login to %q", got)
	}
	rig.client.nativeQuota["agy"].Agy.HTTP = claudeAPIReply(429, `{}`)
	rig.clock = rig.clock.Add(loginProbeInterval)
	rig.round(t)
	if got := rig.login(t); got != fleet.ReadinessLoginPresent {
		t.Fatalf("a rate limit changed the login to %q", got)
	}
	if lines := rig.loginLines(); len(lines) != 0 {
		t.Fatalf("login log lines = %q", lines)
	}

	// And the other way: a host already marked missing stays missing through a
	// failure that is not a pass.
	missing := newLoginProbeRig(t, agyLoginFiles(""), true, loginProbeInterval)
	missing.round(t)
	for path, body := range agyLoginFiles(agyToken) {
		missing.files[path] = body
	}
	missing.client.nativeQuota["agy"].Agy.HTTP = claudeAPIFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") })
	missing.clock = missing.clock.Add(loginProbeInterval)
	missing.round(t)
	if got := missing.login(t); got != fleet.ReadinessLoginMissing {
		t.Fatalf("a network failure cleared login_missing: %q", got)
	}
}

// (e) The probe is off until `[quota].login_probe_interval` is set.
func TestLoginProbeIsOffByDefaultAndOnWithAnInterval(t *testing.T) {
	setHostConfig := func(extra string) {
		body := "[quota]\nmeasurable_accounts = [\"" + agyKeyForTest + "\"]\n" + extra + "\n[quota.accounts]\nagy = \"" + agyFPForTest + "\"\n"
		path := filepath.Join(t.TempDir(), "hosts.toml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("FLEET_HOSTS_CONFIG", path)
	}
	for name, tc := range map[string]struct {
		config string
		probed bool
	}{
		"key unset":    {"", false},
		"zero":         {`login_probe_interval = "0s"`, false},
		"interval set": {`login_probe_interval = "10m"`, true},
	} {
		t.Run(name, func(t *testing.T) {
			setHostConfig(tc.config)
			rig := newLoginProbeRig(t, agyLoginFiles(agyToken), true, 0)
			rig.client.loginProbeIntervalOf = nil // read the host config, as the daemon does
			for range 3 {
				rig.round(t)
				rig.clock = rig.clock.Add(time.Hour)
			}
			if probed := rig.api.count() > 0; probed != tc.probed {
				t.Fatalf("probed=%v, want %v (requests=%d)", probed, tc.probed, rig.api.count())
			}
		})
	}
	t.Run("invalid value", func(t *testing.T) {
		setHostConfig(`login_probe_interval = "soon"`)
		if got := fleet.HostLoginProbeInterval(); got != 0 {
			t.Fatalf("an invalid interval enabled the probe: %v", got)
		}
		app, err := fleet.LoadConfig(os.Getenv("FLEET_HOSTS_CONFIG"))
		if err == nil || !strings.Contains(err.Error(), "login_probe_interval") {
			t.Fatalf("config load = %v, %v; want a refusal naming login_probe_interval", app, err)
		}
	})
}

// The agy login screen as the 2026-11-10 mac-dev captures show it (URL shortened).
const agyLoginScreen = `     ▄▀▀▄
    ▀▀▀▀▀▀

 Open the URL below in your browser:
 ─────────────────────────────────────────────────────────────────────────────
 https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=x
 ─────────────────────────────────────────────────────────────────────────────

 After authenticating, copy the code displayed in the browser and paste it below:
 >
`

const agyComposerScreen = ` Antigravity CLI

 > Your brief is the file /x/brief-delivered.md (14416 bytes). Read it now.
`

// launchWithCapture runs a failed spawn through the node's launch hook with
// the capture the executing node saved for job.
func launchWithCapture(t *testing.T, client *HubClient, profile string, rc int, capture string) {
	t.Helper()
	root := client.jobsInboxRoot
	if capture != "" {
		dir := filepath.Join(root, "jobs", "at-3816")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "spawn-failure-123.txt"), []byte(capture), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	client.noteLaunchResult(hubOutboundMessage{Args: []string{"--job", "at-3816", "-m", profile}}, hubSpawnResult{RC: rc, JobID: "at-3816", Error: "spawn_failed", StdoutTail: "stderr: BRIEF_SUBMISSION_UNCONFIRMED: no working transition"})
}

// (d) rc=76 whose saved screen is the harness's login screen marks the host
// login_missing at once and the desk hears it once; rc=76 without that screen
// is what it was.
func TestSubmitUnconfirmedAtTheLoginScreenMarksLoginMissingAtOnce(t *testing.T) {
	newClient := func() *HubClient {
		client := quotaClient(t)
		client.jobsInboxRoot = t.TempDir()
		return client
	}
	t.Run("login screen", func(t *testing.T) {
		client := newClient()
		client.noteQuotaAccount("agy", agyKeyForTest) // the loop reads a fresh shared reading
		if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginPresent {
			t.Fatalf("login before = %q", got)
		}
		launchWithCapture(t, client, "agy-flash", fleet.ExitSubmitUnconfirmed, agyLoginScreen)
		client.noteQuotaAccount("agy", agyKeyForTest) // the next loop round still reads it
		report := client.collectReadiness(t.Context())
		if got := report.Harnesses["agy"].Login; got != fleet.ReadinessLoginMissing {
			t.Fatalf("login after one rc=76 at the login screen = %q, want missing", got)
		}
		if report.Harnesses["claude"].Login == fleet.ReadinessLoginMissing {
			t.Fatal("another harness was marked")
		}
		if strings.Contains(readinessJSON(report), "authenticating") {
			t.Fatal("screen text left the node")
		}

		// The hub tells the desk once while the host keeps reporting it.
		h, _, destination := spawnNotifyHubLane(t, "", "operator-desk")
		start := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
		for i := range 3 {
			at := start.Add(time.Duration(i) * 45 * time.Second)
			copied := *report
			copied.CheckedAt = at
			h.noteReadinessActions("mac-dev", &copied, at)
		}
		var notices []string
		eventually(t, 5*time.Second, "desk notice", func() bool {
			notices = append(notices, deskNotices(t, h, destination)...)
			return len(notices) > 0
		})
		if len(notices) != 1 || notices[0] != "[mac-dev] agy needs a one-time action: log in" {
			t.Fatalf("notices = %q", notices)
		}
		if more := deskNotices(t, h, destination); len(more) != 0 {
			t.Fatalf("an uncleared need was told again: %q", more)
		}

		// A launch that gets through clears it, and a probe pass does too.
		client.noteLaunchResult(hubOutboundMessage{Args: []string{"-m", "agy-flash"}}, hubSpawnResult{RC: 0})
		if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginPresent {
			t.Fatalf("login after a good launch = %q", got)
		}
		launchWithCapture(t, client, "agy-flash", fleet.ExitSubmitUnconfirmed, agyLoginScreen)
		client.noteLoginProbe("agy", false)
		if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginPresent {
			t.Fatalf("login after a passing probe = %q", got)
		}
	})
	t.Run("expires so the pair gets one more launch", func(t *testing.T) {
		client := newClient()
		now := client.now()
		launchWithCapture(t, client, "agy-flash", fleet.ExitSubmitUnconfirmed, agyLoginScreen)
		client.now = func() time.Time { return now.Add(nodeLoginScreenTTL + time.Second) }
		if got := poolLogin(t, client, "agy").Login; got != fleet.ReadinessLoginUnknown {
			t.Fatalf("login after the TTL = %q", got)
		}
	})
	for name, tc := range map[string]struct {
		profile string
		rc      int
		capture string
	}{
		"rc=76 without the screen":              {"agy-flash", fleet.ExitSubmitUnconfirmed, agyComposerScreen},
		"rc=76 with half of the screen":         {"agy-flash", fleet.ExitSubmitUnconfirmed, " Open the URL below in your browser:\n"},
		"rc=76 and no capture":                  {"agy-flash", fleet.ExitSubmitUnconfirmed, ""},
		"login screen, other rc":                {"agy-flash", 75, agyLoginScreen},
		"login screen, harness with no pattern": {"opus", fleet.ExitSubmitUnconfirmed, agyLoginScreen},
	} {
		t.Run(name, func(t *testing.T) {
			client := newClient()
			client.noteQuotaAccount("agy", agyKeyForTest)
			launchWithCapture(t, client, tc.profile, tc.rc, tc.capture)
			report := client.collectReadiness(t.Context())
			if got := report.Harnesses["agy"].Login; got != fleet.ReadinessLoginPresent {
				t.Fatalf("agy login = %q, want present as before", got)
			}
			if got := report.Harnesses["claude"].Login; got != fleet.ReadinessLoginUnknown {
				t.Fatalf("claude login = %q, want unknown as before", got)
			}
			client.readiness.mu.Lock()
			dirty := client.readiness.dirty
			client.readiness.mu.Unlock()
			if tc.rc != 0 && !dirty {
				t.Fatal("a failed launch no longer asks for a readiness refresh")
			}
		})
	}
}
