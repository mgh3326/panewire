package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// remoteFake is what the fake host answers to the preflight's SSH probe.
type remoteFake struct {
	review, plaintext, panewire string
	rc                          int
}

func goodRemote() remoteFake {
	return remoteFake{review: "present", plaintext: "true", panewire: "panewire v1 (abc)"}
}

func (r remoteFake) output() string {
	return "review_config=" + r.review + "\nplaintext_tailnet=" + r.plaintext + "\npanewire=" + r.panewire + "\n"
}

// preflightFixture is one healthy remote host: accepted by the hub with a load
// reading, a 1-minute-old shared observation, an https hk URL and matching
// versions. Each test breaks one thing.
func preflightFixture(t *testing.T) (*App, *fakePlacementSource, *remoteFake, *bytes.Buffer) {
	t.Helper()
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	a := testApp(t)
	const key = "claude:00000000000000aa"
	load := 0.2
	src := &fakePlacementSource{
		hosts:    []HostView{{Name: "host-b", Connected: true, LoadRatio: &load, Reason: "accepting"}},
		presence: []PresenceRecord{{HostID: "presence-b", AccountKey: key, LastSeen: a.Now()}},
		accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now().Add(-time.Minute)}},
	}
	a.Source = src
	a.Config.Local = map[string]any{}
	a.Config.Quota = map[string]any{"url": "https://hk.example.invalid"}
	a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b"}}
	a.Config.HostOrder = []string{"host-b"}
	a.Version = "panewire v1 (abc)"
	remote := goodRemote()
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		switch c.Name {
		case "ssh":
			return Result{Out: remote.output(), Code: remote.rc}
		case "panewire":
			return Result{Out: "panewire v2\n"}
		}
		return Result{}
	}}
	out := &bytes.Buffer{}
	a.Out = out
	return a, src, &remote, out
}

func runPreflight(a *App) hostPreflight {
	return a.preflightHost(context.Background(), a.newPreflightEnv(context.Background()), "host-b", a.Config.Hosts["host-b"], "", true)
}

// Each of the six conditions alone yields its named reason and nothing else;
// all good yields ok.
func TestPreflightEachConditionAlone(t *testing.T) {
	tailnet := "http://100.64.0.1:8080"
	tests := []struct {
		name   string
		break_ func(a *App, src *fakePlacementSource, r *remoteFake)
		want   []string // name:reason; empty is all good
		detail string   // substring of the first failed check's detail
	}{
		{name: "all good", break_: func(*App, *fakePlacementSource, *remoteFake) {}},
		{name: "plaintext tailnet opted in", break_: func(a *App, _ *fakePlacementSource, _ *remoteFake) {
			a.Config.Quota["url"] = tailnet
		}},
		{name: "hub not accepting", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hosts[0].Reason = "not_accepting: acceptance overrides off"
		}, want: []string{"hub_acceptance:not_accepting"}, detail: "acceptance overrides off"},
		{name: "hub at its lane row cap", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hosts[0].Reason = "lanes_full"
		}, want: []string{"hub_acceptance:lanes_full"}, detail: "lanes_full"},
		{name: "hub does not list the host", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hosts = []HostView{{Name: "host-z", Connected: true}}
		}, want: []string{"hub_acceptance:not_listed", "hub_load:load_unknown"}},
		{name: "hub unreadable", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hostsErr = fail(exitConfig, "hub placement unavailable")
		}, want: []string{"hub_acceptance:hub_unavailable", "hub_load:load_unknown"}},
		{name: "shared observation older than fresh_ttl", break_: func(a *App, src *fakePlacementSource, _ *remoteFake) {
			const key = "claude:00000000000000aa"
			src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-20 * time.Minute)}
		}, want: []string{"quota_observation:claude:stale"}, detail: "age=20m0s fresh_ttl=15m0s"},
		{name: "shared observation missing", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			delete(src.accounts, "claude:00000000000000aa")
		}, want: []string{"quota_observation:claude:not_observed"}},
		{name: "no presence for the host", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.presence = nil
		}, want: []string{"quota_observation:no_usable_presence"}},
		{name: "plaintext hk URL without the remote opt-in", break_: func(a *App, _ *fakePlacementSource, r *remoteFake) {
			a.Config.Quota["url"] = tailnet
			r.plaintext = "false"
		}, want: []string{"quota_transport:plaintext_tailnet_not_allowed"}},
		{name: "plaintext hk URL, no remote config", break_: func(a *App, _ *fakePlacementSource, r *remoteFake) {
			a.Config.Quota["url"] = tailnet
			r.plaintext = "noconfig"
		}, want: []string{"quota_transport:remote_config_missing"}},
		{name: "plaintext hk URL off the tailnet", break_: func(a *App, _ *fakePlacementSource, _ *remoteFake) {
			a.Config.Quota["url"] = "http://198.51.100.7:8080"
		}, want: []string{"quota_transport:plaintext_non_tailnet"}},
		{name: "hub memory pressure", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hosts[0].Reason = "memory_pressure: swap in use"
		}, want: []string{"hub_load:memory_pressure"}, detail: "swap in use"},
		{name: "hub metrics source lacks the host", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			src.hosts[0].LoadRatio = nil
		}, want: []string{"hub_load:load_unknown"}},
		{name: "hub load over the threshold", break_: func(_ *App, src *fakePlacementSource, _ *remoteFake) {
			over := 1.5
			src.hosts[0].LoadRatio = &over
		}, want: []string{"hub_load:load_over_threshold"}},
		{name: "review config missing", break_: func(_ *App, _ *fakePlacementSource, r *remoteFake) {
			r.review = "absent"
		}, want: []string{"review_config:absent"}},
		{name: "panewire skew", break_: func(_ *App, _ *fakePlacementSource, r *remoteFake) {
			r.panewire = "panewire v0 (old)"
		}, want: []string{"version_skew:panewire_skew"}, detail: `remote="panewire v0 (old)"`},
		{name: "panewire skew", break_: func(_ *App, _ *fakePlacementSource, r *remoteFake) {
			r.panewire = "panewire v1"
		}, want: []string{"version_skew:panewire_skew"}},
		{name: "panewire missing on the host", break_: func(_ *App, _ *fakePlacementSource, r *remoteFake) {
			r.panewire = ""
		}, want: []string{"version_skew:panewire_missing"}},
		{name: "host unreachable", break_: func(_ *App, _ *fakePlacementSource, r *remoteFake) {
			r.rc = exitUnreachable
		}, want: []string{"review_config:unreachable", "version_skew:unreachable"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, src, remote, _ := preflightFixture(t)
			tt.break_(a, src, remote)
			got := runPreflight(a)
			if !slices.Equal(got.reasons(), append([]string{}, tt.want...)) {
				t.Fatalf("reasons: want %v, got %v\n%+v", tt.want, got.reasons(), got.Checks)
			}
			if got.OK != (len(tt.want) == 0) {
				t.Fatalf("ok=%t with reasons %v", got.OK, got.reasons())
			}
			if tt.detail != "" {
				found := false
				for _, c := range got.Checks {
					if !c.OK && strings.Contains(c.Detail, tt.detail) {
						found = true
					}
				}
				if !found {
					t.Fatalf("no failed check detail contains %q: %+v", tt.detail, got.Checks)
				}
			}
		})
	}
}

// The unreachable table row names only two checks because the plaintext rule
// needs no probe under https; a plaintext URL adds the transport check.
func TestPreflightUnreachableNamesEveryProbeCheck(t *testing.T) {
	a, _, remote, _ := preflightFixture(t)
	a.Config.Quota["url"] = "http://100.64.0.1:8080"
	remote.rc = exitUnreachable
	want := []string{"quota_transport:unreachable", "review_config:unreachable", "version_skew:unreachable"}
	if got := runPreflight(a).reasons(); !slices.Equal(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// The probe only tests two paths and reads one boolean; it must never print a
// file's contents or read the mergewake config.
func TestPreflightProbeIsReadOnly(t *testing.T) {
	for _, bad := range []string{"cat ", "rm ", "tee ", ">>", "source ", "mergewake config", "sed -i"} {
		if strings.Contains(probeScript, bad) {
			t.Fatalf("probe script must stay read-only, found %q", bad)
		}
	}
	if strings.Contains(probeScript, "config.yaml\"") && !strings.Contains(probeScript, `[ -f "$HOME/.config/mergewake/config.yaml" ]`) {
		t.Fatal("the review config is only tested for existence")
	}
}

func TestPreflightCommandExitCodes(t *testing.T) {
	ctx := context.Background()
	a, src, remote, out := preflightFixture(t)
	if e := a.preflightCmd(ctx, nil); e != nil {
		t.Fatalf("all good must exit 0: %v\n%s", e, out)
	}
	if !strings.Contains(out.String(), "preflight host=host-b ok=true") {
		t.Fatalf("summary line:\n%s", out)
	}

	out.Reset()
	remote.review = "absent"
	src.hosts[0].LoadRatio = nil
	e := a.preflightCmd(ctx, []string{"--host", "host-b"})
	if exitCode(e) != exitPreflight {
		t.Fatalf("want exit %d, got %v", exitPreflight, e)
	}
	for _, want := range []string{"review_config:absent", "hub_load:load_unknown"} {
		if !strings.Contains(e.Error(), want) || !strings.Contains(out.String(), want) {
			t.Fatalf("%s missing from error %q or output:\n%s", want, e, out)
		}
	}
	if !strings.Contains(out.String(), "check review_config refused reason=absent") {
		t.Fatalf("per-check lines:\n%s", out)
	}

	out.Reset()
	if e := a.preflightCmd(ctx, []string{"--json"}); exitCode(e) != exitPreflight {
		t.Fatalf("json: %v", e)
	}
	var doc struct {
		OK    bool            `json:"ok"`
		Hosts []hostPreflight `json:"hosts"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || doc.OK || len(doc.Hosts) != 1 || doc.Hosts[0].OK || len(doc.Hosts[0].Checks) != 6 {
		t.Fatalf("--json: %v %+v\n%s", err, doc, out)
	}

	if e := a.preflightCmd(ctx, []string{"--host", "nope"}); exitCode(e) != exitUsage {
		t.Fatalf("unknown host: %v", e)
	}
	if e := a.preflightCmd(ctx, []string{"extra"}); exitCode(e) != exitUsage {
		t.Fatalf("positional: %v", e)
	}
	a.Config.Hosts, a.Config.HostOrder = nil, nil
	if e := a.preflightCmd(ctx, nil); exitCode(e) != exitConfig {
		t.Fatalf("no hosts: %v", e)
	}
}

// preflight is read-only: no spawn, wake, job or hk write; only the probe and
// the version question reach a host or binary.
func TestPreflightCommandIsReadOnly(t *testing.T) {
	a, _, _, _ := preflightFixture(t)
	fr := a.Runner.(*fakeRunner)
	if e := a.preflightCmd(context.Background(), nil); e != nil {
		t.Fatal(e)
	}
	for _, c := range fr.Calls {
		switch {
		case c.Name == "ssh" && c.Args[len(c.Args)-1] == probeScript:
		case c.Name == "panewire" && strings.Join(c.Args, " ") == "--version":
		default:
			t.Fatalf("unexpected call %v", c)
		}
	}
}

func TestPlanPrintsPreflightPerHost(t *testing.T) {
	a, _, fr, _, out := planFixture(t)
	a.Version = "panewire v1 (abc)"
	a.Config.Quota = map[string]any{"url": "https://hk.example.invalid"}
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	remote := goodRemote()
	remote.review = "absent"
	fr.Do = func(c Command) Result {
		switch c.Name {
		case "ssh":
			return Result{Out: remote.output()}
		case "panewire":
			return Result{Out: "panewire v2\n"}
		}
		return Result{}
	}
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"}); e != nil {
		t.Fatalf("a failing preflight must not change the plan outcome: %v\n%s", e, out)
	}
	text := out.String()
	// planFixture's hub reports no load for host-b, and the review config is
	// absent; the version and transport checks pass.
	if !strings.Contains(text, "preflight host=host-b ok=false reasons=hub_load:load_unknown,review_config:absent") {
		t.Fatalf("plan prints the preflight per host:\n%s", text)
	}

	out.Reset()
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1", "--json"}); e != nil {
		t.Fatal(e)
	}
	var doc struct {
		Preflight []hostPreflight `json:"preflight"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || len(doc.Preflight) != 1 || doc.Preflight[0].Host != "host-b" || len(doc.Preflight[0].Checks) != 6 {
		t.Fatalf("--json preflight: %v %+v", err, doc.Preflight)
	}

	out.Reset()
	remote.review = "present"
	if src, ok := a.Source.(*fakePlacementSource); ok {
		load := 0.1
		src.hosts[0].LoadRatio = &load
	}
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "preflight host=host-b ok=true") {
		t.Fatalf("a healthy host prints ok=true:\n%s", out)
	}
}

// placementPreflightFixture is a two-host `--host auto` round: "first" ranks
// ahead (lower load) and is refused, "second" accepts. The fake host answers
// the preflight probe with a missing review config.
func placementPreflightFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
	t.Helper()
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	src := &fakePlacementSource{hosts: []HostView{
		{Name: "first", Connected: true, LoadRatio: ptrF(0.1), Reason: "memory_pressure: swap in use"},
		{Name: "second", Connected: true, LoadRatio: ptrF(0.5)},
	}}
	remote := goodRemote()
	remote.review = "absent"
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		switch {
		case strings.Contains(args, "review_config="):
			return Result{Out: remote.output()}
		case strings.Contains(args, "_spawn-stdin"):
			if strings.Contains(args, "first") {
				return Result{Code: 77, Err: "quota role-denied"}
			}
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		return Result{}
	})
	base := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "panewire" {
			return Result{Out: "panewire v2\n"}
		}
		return base(c)
	}
	a.Version = "panewire v1 (abc)"
	a.Config.Quota = map[string]any{"url": "https://hk.example.invalid"}
	a.Config.Hosts = map[string]map[string]any{"first": {}, "second": {}}
	a.Config.HostOrder = []string{"first", "second"}
	return a, s, f
}

func candidateRows(t *testing.T, rec map[string]any) map[string]map[string]any {
	t.Helper()
	rows := map[string]map[string]any{}
	for _, c := range rec["candidates"].([]any) {
		row := c.(map[string]any)
		rows[row["host"].(string)] = row
	}
	return rows
}

// --host auto records the preflight reasons on each refused candidate in the
// placement record; the host that accepted needs none.
func TestPlacementRecordCarriesPreflightReasons(t *testing.T) {
	a, s, _ := placementPreflightFixture(t)
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || rec["chose"] != "second" {
		t.Fatalf("second accepts after first refuses: %v %v", rec["chose"], err)
	}
	rows := candidateRows(t, rec)
	pre, ok := rows["first"]["preflight"].(map[string]any)
	if !ok {
		t.Fatalf("the refused candidate carries a preflight: %v", rows["first"])
	}
	reasons := toStrings(pre["reasons"])
	for _, want := range []string{"hub_load:memory_pressure", "review_config:absent"} {
		if !slices.Contains(reasons, want) {
			t.Fatalf("reasons %v lack %s", reasons, want)
		}
	}
	if pre["ok"] != false {
		t.Fatalf("ok: %v", pre)
	}
	if _, has := rows["second"]["preflight"]; has {
		t.Fatalf("the accepting host needs no preflight: %v", rows["second"])
	}
	// The record survives into the job event the desk reads.
	events, e := a.events("job-a")
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, ev := range events {
		if ev.Kind == "job.placement" {
			_, found = candidateRows(t, ev.Payload)["first"]["preflight"]
		}
	}
	if !found {
		t.Fatal("job.placement event lacks the preflight")
	}
}

// A candidate the quota gate refused was never contacted over SSH; its
// preflight reads the hub and hk only and marks the probe checks skipped.
func TestPlacementPreflightDoesNotContactQuotaRefusedHost(t *testing.T) {
	a, s, f := placementPreflightFixture(t)
	n := 0
	a.Quota = &fakeQuota{LaunchValue: Launch{Pool: "claude"}, GateDo: func(r QuotaRequest) (string, error) {
		if r.AccountKey != "" {
			if n++; n == 1 {
				return "", fail(3, "quota exhausted")
			}
		}
		return "pool=claude profile=sonnet\n", nil
	}}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || rec["chose"] != "second" {
		t.Fatalf("%v %v", rec["chose"], err)
	}
	first := candidateRows(t, rec)["first"]
	if first["outcome"] != "quota_refused" {
		t.Fatalf("first outcome: %v", first["outcome"])
	}
	pre := first["preflight"].(map[string]any)
	reasons := toStrings(pre["reasons"])
	if !slices.Contains(reasons, "hub_load:memory_pressure") || slices.Contains(reasons, "review_config:absent") {
		t.Fatalf("hub reasons only, no probe reasons: %v", reasons)
	}
	for _, c := range f.Calls {
		if c.Name == "ssh" && slices.Contains(c.Args, "first") {
			t.Fatalf("a quota-refused host must not be contacted: %v", c)
		}
	}
}

// Exhausted placement keeps the reasons in the error, since a failed spawn
// writes no placement record.
func TestPlacementFailureNamesPreflightReasons(t *testing.T) {
	a, s, _ := placementPreflightFixture(t)
	a.Config.Hosts = map[string]map[string]any{"first": {}}
	a.Config.HostOrder = []string{"first"}
	_, _, err := a.placeAuto(context.Background(), s, "brief")
	if exitCode(err) != exitLocalDisabled || !strings.Contains(err.Error(), "first hub_load:memory_pressure") {
		t.Fatalf("want a placement failure naming the reasons: %v", err)
	}
}

func toStrings(v any) []string {
	var out []string
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		for _, s := range x {
			out = append(out, s.(string))
		}
	}
	return out
}

// A candidate explained once is not probed again (the wake loop can refuse the
// same candidate a second time), unless the first preflight skipped the probe.
func TestAttachPreflightProbesACandidateOnce(t *testing.T) {
	a, _, _, _ := preflightFixture(t)
	fr := a.Runner.(*fakeRunner)
	env := a.newPreflightEnv(context.Background())
	probes := func() int {
		n := 0
		for _, c := range fr.Calls {
			if c.Name == "ssh" && c.Args[len(c.Args)-1] == probeScript {
				n++
			}
		}
		return n
	}
	c := &placeCandidate{name: "host-b", cfg: a.Config.Hosts["host-b"]}
	a.attachPreflight(context.Background(), env, c, "", false)
	if probes() != 0 || !c.preflight.hasSkipped() {
		t.Fatalf("a quota-refused candidate is not probed: %d", probes())
	}
	a.attachPreflight(context.Background(), env, c, "", true)
	a.attachPreflight(context.Background(), env, c, "", true)
	if probes() != 1 || c.preflight.hasSkipped() {
		t.Fatalf("a later probe fills the skipped checks once: %d", probes())
	}
}
