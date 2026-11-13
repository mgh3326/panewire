package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type reviewAccountSource struct {
	PlacementSource
	read func(context.Context, string) (SharedAccount, error)
}

func (s reviewAccountSource) SharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	return s.read(ctx, key)
}

// reviewSpawnFixture drives Spawn through real selection and placement, with
// only the external runner, quota and presence boundaries faked.
func reviewSpawnFixture(t *testing.T, sharedClaude, explicit bool) (*App, Spawn, *fakeQuota, *[]Spawn) {
	t.Helper()
	a, s, fr, _ := cmdFixture(t)
	s.Profile, s.Host, s.Role = "auto", "auto", "consult"
	if explicit {
		s.Profile, s.Host = "builder-opus", "host-b"
	}
	a.Config.Local = map[string]any{"machine_id": "initiator", "spawn": true}
	a.Config.Hosts = map[string]map[string]any{
		"host-a": {"cwd_map": map[string]any{s.CWD: "/work/project"}},
		"host-b": {"cwd_map": map[string]any{s.CWD: "/work/project"}},
	}
	a.Config.HostOrder = []string{"host-a", "host-b"}
	tight, healthy := riskPool(120, 8), riskPool(30, 8)
	src := &fakePlacementSource{
		hosts: []HostView{{Name: "host-a", Connected: true}, {Name: "host-b", Connected: true}},
		presence: []PresenceRecord{
			{HostID: "initiator", AccountKey: testClaudeA, LastSeen: a.Now()},
			{HostID: "host-a", AccountKey: testClaudeA, LastSeen: a.Now()},
			{HostID: "host-b", AccountKey: testClaudeB, LastSeen: a.Now()},
			{HostID: "host-b", AccountKey: testCodexA, LastSeen: a.Now()},
		},
		accounts: map[string]SharedAccount{
			testClaudeA: {Key: testClaudeA, Observed: true, Fresh: true, Quota: &tight},
			testClaudeB: {Key: testClaudeB, Observed: true, Fresh: true, Quota: &healthy},
			testCodexA:  {Key: testCodexA, Observed: true, Fresh: true, Quota: &healthy},
		},
	}
	if sharedClaude {
		src.presence[2].AccountKey = testClaudeA
	}
	a.Source = src
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	}
	q.LaunchValue = Launch{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"}
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != testClaudeB && r.AccountKey != testCodexA {
			t.Fatalf("initiator or exhausted account reached admission: %+v", r)
		}
		pool, _, _ := strings.Cut(r.AccountKey, ":")
		return "pool=" + pool + "\n", nil
	}
	serveRouting(t, routingDocV1)
	var delivered []Spawn
	next := fr.Do
	fr.Do = func(c Command) Result {
		if c.Name == "ssh" && strings.Contains(strings.Join(c.Args, " "), "_spawn-stdin") {
			if !strings.Contains(strings.Join(c.Args, " "), "host-b") {
				t.Fatalf("wrong remote target: %+v", c.Args)
			}
			var envelope struct {
				Spawn Spawn
				Brief string
			}
			if err := json.Unmarshal([]byte(c.Input), &envelope); err != nil {
				t.Fatal(err)
			}
			delivered = append(delivered, envelope.Spawn)
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		if c.Name == "herdr" {
			t.Fatalf("remote spawn reached the initiator's local launcher: %+v", c.Args)
		}
		return next(c)
	}
	return a, s, q, &delivered
}

func TestReviewRemoteSpawnPrecedesInitiatorAdmission(t *testing.T) {
	for _, shared := range []bool{false, true} {
		a, s, _, delivered := reviewSpawnFixture(t, shared, false)
		if err := a.Spawn(context.Background(), s); err != nil {
			t.Fatalf("remote account launch failed: %v", err)
		}
		wantKey, wantProfile := testClaudeB, "opus"
		if shared {
			wantKey, wantProfile = testCodexA, "codex-sol"
		}
		if len(*delivered) != 1 {
			t.Fatalf("launches=%d", len(*delivered))
		}
		remote := (*delivered)[0]
		if remote.AccountKey != wantKey || remote.Host != "local" || a.Policy.Profiles[remote.Profile].Canon != wantProfile {
			t.Fatalf("wrong delivered selection: %+v", remote)
		}
		events, _ := a.events(s.Job)
		found := false
		for _, event := range events {
			if event.Kind == "job.placement" {
				selection := event.Payload["selection"].(map[string]any)
				if selection["host"] != "host-b" || selection["account_key"] != wantKey || selection["outcome_code"] != outcomeAdmitted {
					t.Fatalf("wrong placement receipt: %v", selection)
				}
				if shared {
					accounts := 0
					for _, raw := range selection["candidates"].([]any) {
						c := raw.(map[string]any)
						if c["profile"] == "opus" && c["account_key"] == testClaudeA {
							accounts++
						}
					}
					if accounts != 1 {
						t.Fatalf("one exhausted account became %d candidates", accounts)
					}
				}
				found = true
			}
		}
		if !found {
			t.Fatal("remote launch has no placement receipt")
		}
		if len(a.JobStore.(*fakeJobStore).records) != 0 {
			t.Fatal("initiator claimed the remote job")
		}
	}
}

func TestReviewPinnedRemoteDoesNotInheritInitiatorAccount(t *testing.T) {
	a, s, _, delivered := reviewSpawnFixture(t, false, true)
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("explicit remote profile was rebound to initiator account: %v", err)
	}
	if len(*delivered) != 1 || (*delivered)[0].Host != "local" {
		t.Fatalf("remote launch missing: %+v", *delivered)
	}
}

func TestReviewLocalAccountJudgmentIsLocalAndBound(t *testing.T) {
	a, s, q := consultFixture(t)
	s.Role, s.Host = "consult", "local"
	q.GateDo = gatePools(map[string]string{"opus": "claude"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	j := sel.chosen.judgments[0]
	if j.Source != "local" || j.AccountKey != sel.chosen.accountKey {
		t.Fatalf("local account judgment lost its origin or binding: %+v", j)
	}
}

func TestReviewSameAccountRefreshTriesAnotherHost(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	src.presence = src.presence[:2]
	src.presence[1].AccountKey = testClaudeA
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true}
	q.CatalogRows = q.CatalogRows[1:2]
	q.GateDo = gatePools(map[string]string{"opus": "claude"})
	var attempts []string
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		if c.Name != "ssh" {
			return Result{}, false
		}
		args := strings.Join(c.Args, " ")
		if !isQuotaRefresh(args) {
			t.Fatalf("unexpected host contact: %s", args)
		}
		if strings.Contains(args, "host-a") {
			attempts = append(attempts, "host-a")
			return Result{Code: exitUnreachable}, true
		}
		attempts = append(attempts, "host-b")
		v := riskPool(30, 8)
		src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, Quota: &v}
		return Result{}, true
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("second holder could refresh the same account: %v; attempts=%v", err, attempts)
	}
	if strings.Join(attempts, ",") != "host-a,host-b" {
		t.Fatalf("host refresh fallback: %v", attempts)
	}
	if sel.chosen.host != "host-b" {
		t.Fatalf("unreachable holder was still preferred: %s", sel.chosen.host)
	}
}

func TestReviewHostGateFailureTriesAnotherHolder(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	src.presence = src.presence[:2]
	src.presence[0].AccountKey = testClaudeB
	q.CatalogRows = q.CatalogRows[1:2]
	calls := 0
	q.GateDo = func(QuotaRequest) (string, error) {
		calls++
		if calls == 1 {
			return "", fail(exitUnreachable, "host unavailable")
		}
		return "pool=claude\n", nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.host != "host-b" {
		t.Fatalf("usable sibling host was skipped: %v; calls=%d", err, calls)
	}
	if len(sel.chosen.judgments) != 2 || sel.chosen.judgments[0].Host != "host-a" || sel.chosen.judgments[0].ExitCode != exitUnreachable || sel.chosen.judgments[1].Host != "host-b" {
		t.Fatalf("missing holder fallback evidence: %+v", sel.chosen.judgments)
	}
}

func TestReviewAccountQuotaRefusalDoesNotRetryHolders(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	src.presence = src.presence[:2]
	src.presence[0].AccountKey = testClaudeB
	q.CatalogRows = q.CatalogRows[1:2]
	calls := 0
	q.GateDo = func(r QuotaRequest) (string, error) {
		calls++
		if r.AccountKey != testClaudeB {
			t.Fatalf("wrong account: %+v", r)
		}
		return "", fail(3, "quota exhausted")
	}
	sel, err := a.selectProfile(context.Background(), s)
	if exitCode(err) != exitRoleWait || sel.outcome != outcomeQuotaExhausted || calls != 1 || len(sel.cands) != 1 {
		t.Fatalf("same quota was retried per holder: %v, gates=%d, candidates=%d", err, calls, len(sel.cands))
	}
}

func TestReviewRefreshInfrastructureFailuresAreNotQuotaUnknown(t *testing.T) {
	for _, tc := range []struct {
		status, exit int
		outcome      string
	}{
		{exitUnreachable, exitPlacement, outcomeNoEligibleHost},
		{exitTimeout, exitPlacement, outcomeNoEligibleHost},
		{exitUsage, exitConfig, outcomePolicyUnavailable},
	} {
		a, s, q, src := accountConsultFixture(t)
		q.CatalogRows = q.CatalogRows[1:2]
		src.presence = src.presence[:1]
		src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA}
		a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
			if c.Name == "ssh" {
				return Result{Code: tc.status}, true
			}
			return Result{}, false
		})
		sel, err := a.selectProfile(context.Background(), s)
		if exitCode(err) != tc.exit || sel.outcome != tc.outcome {
			t.Fatalf("refresh exit=%d: got %v/%s, want %s/%d", tc.status, err, sel.outcome, tc.outcome, tc.exit)
		}
	}
}

func TestReviewPostRefreshReadSharesDeadline(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		a, s, q, src := accountConsultFixture(t)
		q.CatalogRows = q.CatalogRows[1:2]
		src.presence = src.presence[1:2]
		src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB}
		q.GateDo = gatePools(map[string]string{"opus": "claude"})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reads := 0
		a.Source = reviewAccountSource{PlacementSource: src, read: func(readCtx context.Context, key string) (SharedAccount, error) {
			reads++
			if reads > 1 {
				if _, ok := readCtx.Deadline(); !ok {
					t.Fatal("post-refresh account read has no refresh deadline")
				}
				if cancelRead {
					cancel()
					return SharedAccount{Key: key}, readCtx.Err()
				}
			}
			return src.SharedAccount(readCtx, key)
		}}
		a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
			if c.Name != "ssh" {
				return Result{}, false
			}
			v := riskPool(30, 8)
			src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &v}
			return Result{}, true
		})
		sel, err := a.selectProfile(ctx, s)
		if cancelRead {
			if exitCode(err) != exitPlacement || sel.outcome != outcomeNoEligibleHost {
				t.Fatalf("expired refresh read lost its cause: %v", err)
			}
		} else if err != nil || sel.chosen == nil {
			t.Fatalf("bounded refreshed reading failed: %v", err)
		}
	}
}

func TestReviewGateInfrastructureFailuresAreNotQuotaUnknown(t *testing.T) {
	for _, tc := range []struct {
		status, exit int
		outcome      string
	}{
		{exitUnreachable, exitPlacement, outcomeNoEligibleHost},
		{exitTimeout, exitPlacement, outcomeNoEligibleHost},
		{exitUsage, exitConfig, outcomePolicyUnavailable},
		{exitConfig, exitConfig, outcomePolicyUnavailable},
		{exitFailure, exitConfig, outcomePolicyUnavailable},
	} {
		a, s, q, src := accountConsultFixture(t)
		q.CatalogRows = q.CatalogRows[1:2]
		src.presence = src.presence[1:2]
		q.GateDo = func(QuotaRequest) (string, error) { return "", fail(tc.status, "infrastructure failure") }
		a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
			if c.Name == "ssh" {
				t.Fatal("infrastructure failure triggered a quota refresh")
			}
			return Result{}, false
		})
		sel, err := a.selectProfile(context.Background(), s)
		if exitCode(err) != tc.exit || sel.outcome != tc.outcome {
			t.Fatalf("gate exit=%d: outcome=%s exit=%d, want %s/%d", tc.status, sel.outcome, exitCode(err), tc.outcome, tc.exit)
		}
	}
}
