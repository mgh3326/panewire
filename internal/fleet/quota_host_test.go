package fleet

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCLIQuotaAccountReceipt(t *testing.T) {
	const key = "claude:0123456789abcdef"
	good := `{"schema":"scopefuel.gate.v1","account_key":"` + key + `","provider_id":"claude","ok":true,"exit_code":0}`
	for _, tc := range []struct {
		name, out    string
		status, want int
	}{
		{"bound receipt", good, 0, 0},
		{"old gate ignores flag", strings.ReplaceAll(good, `"account_key":"`+key+`",`, ""), 0, 70},
		{"different account", strings.ReplaceAll(good, key, "claude:fedcba9876543210"), 0, 70},
		{"different pool", strings.ReplaceAll(good, `"provider_id":"claude"`, `"provider_id":"codex"`), 0, 70},
		{"duplicate identity", strings.ReplaceAll(good, `"ok":true`, `"account_key":"`+key+`","ok":true`), 0, 70},
		{"text only", "pool=claude\n", 0, 70},
		{"missing result", strings.ReplaceAll(good, `,"exit_code":0`, ""), 0, 70},
		{"denied receipt", strings.ReplaceAll(good, `"ok":true`, `"ok":false`), 0, 70},
		{"inconsistent blocked exit", good, 3, 3},
		{"configuration error", "invalid gate configuration", exitConfig, exitConfig},
		{"role denial", good, 5, 77},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeRunner{Do: func(Command) Result { return Result{Out: tc.out, Code: tc.status} }}
			q := CLIQuota{Runner: f, Binary: "pacelever"}
			_, err := q.Gate(context.Background(), QuotaRequest{Profile: "example-profile", GateProfile: "sonnet", Role: "implementer", Effort: "high", AccountKey: key})
			if code(err) != tc.want {
				t.Fatalf("code=%d want=%d err=%v", code(err), tc.want, err)
			}
			want := []string{"gate", "-m", "sonnet", "--effort", "high", "--role", "implementer", "--account-key", key, "--json"}
			if len(f.Calls) != 1 || !reflect.DeepEqual(f.Calls[0].Args, want) {
				t.Fatalf("target gate args: %+v", f.Calls)
			}
		})
	}
}

func TestCLIQuotaLocalGateUnchanged(t *testing.T) {
	f := &fakeRunner{Do: func(Command) Result { return Result{Out: `{"pool":"claude"}`} }}
	q := CLIQuota{Runner: f, Binary: "pacelever"}
	if _, err := q.Gate(context.Background(), QuotaRequest{Profile: "sonnet", Role: "implementer"}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"gate", "-m", "sonnet", "--role", "implementer", "--json"}; !reflect.DeepEqual(f.Calls[0].Args, want) {
		t.Fatalf("local gate args changed: %v", f.Calls[0].Args)
	}
}

func TestHostAccountUsesNewestPresence(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const current = "claude:0123456789abcdef"
	const previous = "claude:fedcba9876543210"
	for _, tc := range []struct {
		name string
		rows []PresenceRecord
		want string
	}{
		{"newest wins", []PresenceRecord{{HostID: "host-a", AccountKey: current, LastSeen: now}, {HostID: "host-a", AccountKey: previous, LastSeen: now.Add(-time.Hour)}}, current},
		{"ambiguous", []PresenceRecord{{HostID: "host-a", AccountKey: current, LastSeen: now}, {HostID: "host-a", AccountKey: previous, LastSeen: now}}, ""},
		{"stale", []PresenceRecord{{HostID: "host-a", AccountKey: current, LastSeen: now.Add(-48 * time.Hour)}}, ""},
		{"future", []PresenceRecord{{HostID: "host-a", AccountKey: current, LastSeen: now.Add(time.Hour)}}, ""},
		{"other host", []PresenceRecord{{HostID: "host-b", AccountKey: current, LastSeen: now}}, ""},
		{"other pool", []PresenceRecord{{HostID: "host-a", AccountKey: "codex:0123456789abcdef", LastSeen: now}}, ""},
		{"invalid fingerprint", []PresenceRecord{{HostID: "host-a", AccountKey: "claude:invalid", LastSeen: now}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if got := hostAccount(tc.rows, "claude", "host-a", now, 24*time.Hour); got != tc.want {
					t.Fatalf("got=%q want=%q", got, tc.want)
				}
				if len(tc.rows) == 2 {
					tc.rows[0], tc.rows[1] = tc.rows[1], tc.rows[0]
				}
			}
		})
	}
}

func TestPlacementGatesEachHostsAccount(t *testing.T) {
	var spawns []string
	a, s, _ := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			spawns = append(spawns, args)
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		return Result{}
	})
	const tight = "claude:0123456789abcdef"
	const free = "claude:fedcba9876543210"
	a.Config.Hosts = map[string]map[string]any{"host-a": {"presence_id": "presence-a"}, "host-b": {"presence_id": "presence-b"}}
	a.Config.HostOrder = []string{"host-a", "host-b"}
	a.Config.Quota = map[string]any{"accounts": map[string]any{"claude": "0000000000000001"}}
	a.Source = &fakePlacementSource{
		hosts:    []HostView{{Name: "host-a", Connected: true, LoadRatio: ptrF(.1)}, {Name: "host-b", Connected: true, LoadRatio: ptrF(.2)}},
		presence: []PresenceRecord{{HostID: "presence-a", AccountKey: tight, LastSeen: a.Now()}, {HostID: "presence-b", AccountKey: free, LastSeen: a.Now()}},
		accounts: map[string]SharedAccount{tight: {Key: tight, Observed: true, Fresh: true}, free: {Key: free, Observed: true, Fresh: true}},
	}
	var judged []string
	a.Quota = &fakeQuota{LaunchValue: Launch{Pool: "claude"}, GateDo: func(r QuotaRequest) (string, error) {
		judged = append(judged, r.AccountKey)
		if r.AccountKey == tight {
			return "", fail(3, "quota exhausted")
		}
		if r.AccountKey != free {
			t.Fatalf("judged the spawner or unknown account: %q", r.AccountKey)
		}
		return "pool=claude\n", nil
	}}
	handled, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || !handled || rec["chose"] != "host-b" {
		t.Fatalf("handled=%v record=%v err=%v", handled, rec, err)
	}
	if !reflect.DeepEqual(judged, []string{tight, free}) || len(spawns) != 1 || !strings.Contains(spawns[0], "host-b") {
		t.Fatalf("judged=%v spawns=%v", judged, spawns)
	}
	data, _ := json.Marshal(rec)
	var record struct {
		Candidates []struct {
			Host string        `json:"host"`
			Gate quotaJudgment `json:"quota_gate"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	for _, c := range record.Candidates {
		if c.Host == "host-a" && (c.Gate.AccountKey != tight || c.Gate.Outcome != "refused" || c.Gate.Host != c.Host) {
			t.Fatalf("tight account judgement missing: %s", data)
		}
		if c.Host == "host-b" && (c.Gate.AccountKey != free || c.Gate.Outcome != "admitted" || c.Gate.Host != c.Host) {
			t.Fatalf("free account judgement missing: %s", data)
		}
	}
}

// isQuotaRefresh says an ssh argument line or a local command line is an
// on-demand refresh: pacelever's for most pools, `panewire quota refresh` for
// claude, which panewire measures itself.
func isQuotaRefresh(args string) bool {
	return strings.Contains(args, "pacelever refresh") || strings.Contains(args, "panewire quota refresh")
}

func TestPlacementTargetAccountUnavailable(t *testing.T) {
	for _, mode := range []string{"missing presence", "stale presence", "ambiguous presence", "missing observation", "stale observation", "degraded observation", "wrong account", "read error", "presence error", "unknown pool"} {
		for _, host := range []string{"auto", "host-a"} {
			t.Run(mode+"/"+host, func(t *testing.T) {
				a, s, f := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
					if isQuotaRefresh(args) {
						return Result{Code: 1}
					}
					t.Error("unavailable account reached SSH")
					return Result{}
				})
				const key = "claude:0123456789abcdef"
				s.Host = host
				a.Config.Hosts = map[string]map[string]any{"host-a": {}}
				a.Config.HostOrder = []string{"host-a"}
				src := &fakePlacementSource{hosts: []HostView{{Name: "host-a", Connected: true}}, presence: []PresenceRecord{{HostID: "host-a", AccountKey: key, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true}}}
				switch mode {
				case "missing presence":
					src.presence = nil
				case "stale presence":
					src.presence[0].LastSeen = a.Now().Add(-48 * time.Hour)
				case "ambiguous presence":
					src.presence = append(src.presence, PresenceRecord{HostID: "host-a", AccountKey: "claude:fedcba9876543210", LastSeen: a.Now()})
				case "missing observation":
					src.accounts = nil
				case "stale observation":
					src.accounts[key] = SharedAccount{Key: key, Observed: true}
				case "degraded observation":
					src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, Degraded: true}
				case "wrong account":
					src.accounts[key] = SharedAccount{Key: "claude:fedcba9876543210", Observed: true, Fresh: true}
				case "read error":
					src.accountErr = errString("offline")
				case "presence error":
					src.presenceErr = errString("offline")
				}
				a.Source = src
				q := a.Quota.(*fakeQuota)
				q.GateDo = func(QuotaRequest) (string, error) {
					t.Error("unavailable target invoked a gate")
					return "pool=claude\n", nil
				}
				if mode == "unknown pool" {
					q.LaunchValue.Pool = ""
				}
				if _, _, err := a.place(context.Background(), s, "brief"); err == nil {
					t.Fatal("unavailable target admitted")
				}
				for _, c := range f.Calls {
					if (c.Name == "ssh" && !isQuotaRefresh(strings.Join(c.Args, " "))) || c.Name == "git" {
						t.Fatalf("quota refusal happened after remote preparation: %+v", c)
					}
				}
			})
		}
	}
}

func TestSelectUsesRemoteAccountAfterLocalQuotaRefusal(t *testing.T) {
	rows := []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "model-a"}}
	a, s, q := selFixture(t, "B", rows, map[string]PoolView{"claude": {Status: "ok", Class: "spend"}}, []string{"claude"})
	const key = "claude:fedcba9876543210"
	src := a.Source.(*fakePlacementSource)
	src.hosts = []HostView{{Name: "host-b", Connected: true}}
	src.presence = append(src.presence, PresenceRecord{HostID: "presence-b", AccountKey: key, LastSeen: a.Now()})
	src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
	a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b"}}
	a.Config.HostOrder = []string{"host-b"}
	var judged []string
	q.GateDo = func(r QuotaRequest) (string, error) {
		judged = append(judged, r.AccountKey)
		if r.AccountKey == "claude:0000000000000001" {
			return "", fail(3, "local quota exhausted")
		}
		if r.AccountKey != key {
			t.Fatalf("wrong target account %q", r.AccountKey)
		}
		return "pool=claude\n", nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || !reflect.DeepEqual(judged, []string{"claude:0000000000000001", key}) {
		t.Fatalf("judged=%v err=%v", judged, err)
	}
	gates := sel.chosen.judgments
	if len(gates) != 1 || gates[0].Host != "host-b" || gates[0].AccountKey != key || gates[0].Outcome != "admitted" {
		t.Fatalf("host/account selection evidence: %+v", gates)
	}
	// The exact same evidence cannot admit an explicitly local placement.
	s.Host = "local"
	judged = nil
	if _, err := a.selectProfile(context.Background(), s); err == nil || !reflect.DeepEqual(judged, []string{"claude:0000000000000001"}) {
		t.Fatalf("local override used remote account: %v %v", judged, err)
	}
	// A degraded remote account cannot rescue the refused local login.
	s.Host = "auto"
	src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, Degraded: true}
	judged = nil
	if _, err := a.selectProfile(context.Background(), s); err == nil || !reflect.DeepEqual(judged, []string{"claude:0000000000000001"}) {
		t.Fatalf("degraded target admitted: %v %v", judged, err)
	}
}

func TestSpawnHostLocalRecordsLocalAccount(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	const key = "example:0000000000000001"
	a.Source = &fakePlacementSource{presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true}}}
	q := a.Quota.(*fakeQuota)
	q.GateDo = func(r QuotaRequest) (string, error) { return "pool=example\n", nil }
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	evs, _ := a.events(s.Job)
	for _, ev := range evs {
		if ev.Kind == "job.placement" {
			if sel, _ := ev.Payload["selection"].(map[string]any); sel["account_key"] != key {
				t.Fatalf("local account receipt: %v", sel)
			}
			return
		}
	}
	t.Fatal("missing placement receipt")
}

func TestPlacementRechecksAccountAfterWake(t *testing.T) {
	for _, host := range []string{"auto", "host-a"} {
		t.Run(host, func(t *testing.T) {
			const before = "claude:0123456789abcdef"
			const after = "claude:fedcba9876543210"
			src := &fakePlacementSource{}
			w := &fakeWaker{}
			spawns := 0
			a, s, _ := remoteFixture(t, src, w, "2026-01-01T00:00:00Z", func(args string) Result {
				if strings.Contains(args, "_spawn-stdin") {
					spawns++
					if len(w.calls) == 0 {
						return Result{Code: 255}
					}
					return Result{Out: "OK pane=w:p1 job=job-a\n"}
				}
				return Result{}
			})
			s.Host = host
			a.Config.Hosts = map[string]map[string]any{"host-a": {"wake": "panewire"}}
			a.Config.HostOrder = []string{"host-a"}
			src.presenceDo = nil
			src.presence = []PresenceRecord{{HostID: "host-a", AccountKey: before, LastSeen: a.Now()}}
			src.hosts = []HostView{{Name: "host-a", Connected: host != "auto"}}
			src.accounts = map[string]SharedAccount{before: {Key: before, Observed: true, Fresh: true}, after: {Key: after, Observed: true, Fresh: true}}
			w.onWake = func(string) {
				src.presence[0].AccountKey = after
				src.hosts[0].Connected = true
			}
			var judged []string
			a.Quota.(*fakeQuota).GateDo = func(r QuotaRequest) (string, error) {
				judged = append(judged, r.AccountKey)
				return "pool=claude\n", nil
			}
			handled, rec, err := a.place(context.Background(), s, "brief")
			if err != nil || !handled || len(w.calls) != 1 {
				t.Fatalf("handled=%v wake=%v err=%v", handled, w.calls, err)
			}
			want := []string{after}
			wantSpawns := 1
			if host != "auto" {
				want = []string{before, after}
				wantSpawns = 2
			}
			if !reflect.DeepEqual(judged, want) || spawns != wantSpawns {
				t.Fatalf("judged=%v spawns=%d", judged, spawns)
			}
			data, _ := json.Marshal(rec)
			if !strings.Contains(string(data), `"account_key":"`+after+`"`) {
				t.Fatalf("woken target judgement missing: %s", data)
			}
		})
	}
}

func TestSelectRequiresAccountEvidenceOnLocalHost(t *testing.T) {
	for _, host := range []string{"auto", "local"} {
		for _, mode := range []string{"missing local presence", "degraded local observation"} {
			t.Run(host+"/"+mode, func(t *testing.T) {
				rows := []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "model-a"}}
				a, s, q := selFixture(t, "B", rows, map[string]PoolView{"claude": {Status: "ok", Class: "spend"}}, []string{"claude"})
				s.Host = host
				src := a.Source.(*fakePlacementSource)
				if mode == "missing local presence" {
					src.presence = nil
				} else {
					key := src.presence[0].AccountKey
					src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, Degraded: true}
				}
				const remote = "claude:fedcba9876543210"
				src.presence = append(src.presence, PresenceRecord{HostID: "host-b", AccountKey: remote, LastSeen: a.Now()})
				src.accounts[remote] = SharedAccount{Key: remote, Observed: true, Fresh: true}
				a.Config.Hosts = map[string]map[string]any{"host-b": {}}
				a.Config.HostOrder = []string{"host-b"}
				q.GateDo = func(r QuotaRequest) (string, error) {
					if r.AccountKey != remote {
						t.Fatalf("only the measured remote account may admit: %+v", r)
					}
					return "pool=claude\n", nil
				}
				sel, err := a.selectProfile(context.Background(), s)
				if host == "local" {
					if err == nil || sel.chosen != nil {
						t.Fatal("local without usable account evidence admitted")
					}
					return
				}
				if err != nil || sel.chosen == nil || sel.chosen.accountKey != remote {
					t.Fatalf("remote account was not selectable: %v", err)
				}
			})
		}
	}
}

func TestPlacementForcedRequiresQuotaRequest(t *testing.T) {
	a, s, f := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(string) Result { t.Error("invalid quota request reached SSH"); return Result{} })
	s.Host, s.Profile = "host-a", "unknown-profile"
	a.Config.Hosts = map[string]map[string]any{"host-a": {}}
	if _, _, err := a.placeForced(context.Background(), s, "brief"); code(err) != 70 || !strings.Contains(err.Error(), "quota_request_unavailable") {
		t.Fatalf("invalid quota request: %v", err)
	}
	if len(a.Quota.(*fakeQuota).Calls) != 0 {
		t.Fatal("invalid request reached the quota CLI")
	}
	for _, c := range f.Calls {
		if c.Name == "ssh" || c.Name == "git" {
			t.Fatalf("invalid request prepared a remote checkout: %+v", c)
		}
	}
}

func TestPlacementDegradedAccountTriesNextHost(t *testing.T) {
	a, s, _ := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		return Result{}
	})
	const bad = "claude:0123456789abcdef"
	const good = "claude:fedcba9876543210"
	a.Config.Hosts = map[string]map[string]any{"host-a": {}, "host-b": {}}
	a.Config.HostOrder = []string{"host-a", "host-b"}
	a.Source = &fakePlacementSource{
		hosts:    []HostView{{Name: "host-a", Connected: true, LoadRatio: ptrF(.1)}, {Name: "host-b", Connected: true, LoadRatio: ptrF(.2)}},
		presence: []PresenceRecord{{HostID: "host-a", AccountKey: bad, LastSeen: a.Now()}, {HostID: "host-b", AccountKey: good, LastSeen: a.Now()}},
		accounts: map[string]SharedAccount{bad: {Key: bad, Observed: true, Fresh: true, Degraded: true}, good: {Key: good, Observed: true, Fresh: true}},
	}
	// Saturate the healthy host so placement first refuses the degraded
	// candidate, then tries this last resort with its own account.
	src := a.Source.(*fakePlacementSource)
	src.hosts[0].LoadRatio = nil
	src.hosts[1].LoadRatio = ptrF(1.1)
	q := a.Quota.(*fakeQuota)
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != good {
			t.Fatalf("degraded account reached gate: %q", r.AccountKey)
		}
		return "pool=claude\n", nil
	}
	_, rec, err := a.placeAuto(context.Background(), s, "brief")
	if err != nil || rec["chose"] != "host-b" {
		t.Fatalf("record=%v err=%v", rec, err)
	}
	for _, v := range rec["candidates"].([]any) {
		row := v.(map[string]any)
		if row["host"] == "host-a" {
			j, ok := row["quota_gate"].(quotaJudgment)
			if !ok || j.Reason != "degraded" || j.AccountKey != bad || j.Outcome != "unknown" {
				t.Fatalf("degraded reason missing: %+v", row)
			}
		}
	}
}

func TestPlacementRanksByHubScoreThenLoad(t *testing.T) {
	for _, tc := range []struct {
		name         string
		scoreA, scoB *float64
		want         string
	}{
		{"fast busy host beats slow idle host", ptrF(10), ptrF(50), "host-b"},
		{"without scores the lower load wins", nil, nil, "host-a"},
		{"a missing score falls back to load", ptrF(10), nil, "host-a"},
		{"a missing score on the faster host still falls back to load", nil, ptrF(50), "host-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := remoteFixture(t, &fakePlacementSource{}, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
				if strings.Contains(args, "_spawn-stdin") {
					return Result{Out: "OK pane=w:p1 job=job-a\n"}
				}
				return Result{}
			})
			const key = "claude:0123456789abcdef"
			a.Config.Hosts = map[string]map[string]any{"host-a": {"presence_id": "presence-a"}, "host-b": {"presence_id": "presence-b"}}
			a.Config.HostOrder = []string{"host-a", "host-b"}
			a.Config.Quota = map[string]any{"accounts": map[string]any{"claude": "0000000000000001"}}
			a.Source = &fakePlacementSource{
				hosts: []HostView{
					{Name: "host-a", Connected: true, LoadRatio: ptrF(.05), Score: tc.scoreA},
					{Name: "host-b", Connected: true, LoadRatio: ptrF(.3), Score: tc.scoB},
				},
				presence: []PresenceRecord{{HostID: "presence-a", AccountKey: key, LastSeen: a.Now()}, {HostID: "presence-b", AccountKey: key, LastSeen: a.Now()}},
				accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true}},
			}
			a.Quota = &fakeQuota{LaunchValue: Launch{Pool: "claude"}, GateDo: func(QuotaRequest) (string, error) { return "pool=claude\n", nil }}
			handled, rec, err := a.placeAuto(context.Background(), s, "brief")
			if err != nil || !handled || rec["chose"] != tc.want {
				t.Fatalf("want %s: handled=%v record=%v err=%v", tc.want, handled, rec, err)
			}
		})
	}
}
