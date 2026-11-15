package fleet

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// A request the gate refuses whatever any reading says (operator request on an
// ordinary rung, say) is decided once before the host fan-out and keeps its
// gate code in the host note; only a refusal a reading decided is
// quota_refused (hk 3856, docs/placement.md §Gate refusals).

// gateCountQuota is the production gate counting the host-level questions.
type gateCountQuota struct {
	admissionQuota
	gates int
}

func (q *gateCountQuota) Gate(ctx context.Context, r QuotaRequest) (string, error) {
	q.gates++
	return q.admissionQuota.Gate(ctx, r)
}

const gateReasonCatalog = `[{"profile":"sonnet","effort":"high","model_id":"claude-sonnet-5-5","pool":"claude","grade":"A","gate":"default"}]`

// gateReasonFixture is a two-host `--host auto` round judged by the production
// gate over a catalog and a stored reading, with weekly the 7-day use in percent.
// An accepted host answers the remote spawn.
func gateReasonFixture(t *testing.T, weekly float64) (*App, Spawn, *gateCountQuota, *fakeRunner) {
	t.Helper()
	isolateConfigHome(t)
	t.Setenv("PACELEVER_CONFIG", "")
	src := &fakePlacementSource{hosts: []HostView{
		{Name: "first", Connected: true, LoadRatio: ptrF(0.1)},
		{Name: "second", Connected: true, LoadRatio: ptrF(0.5)},
	}}
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			return Result{Out: "OK pane=w:p1 job=job-a\n"}
		}
		return Result{}
	})
	a.modelFamilies = func(context.Context) (ModelFamiliesPolicy, error) { return launchTestModelFamilies(), nil }
	a.Config.Hosts = map[string]map[string]any{"first": {}, "second": {}}
	a.Config.HostOrder = []string{"first", "second"}
	q := &gateCountQuota{admissionQuota: admissionQuota{CLIQuota{Runner: f, Binary: "pacelever"}, a}}
	a.Quota = q
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) {
		var rows []CatalogRow
		return rows, json.Unmarshal([]byte(gateReasonCatalog), &rows)
	}
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		now := a.Now()
		reset5h, reset7d := now.Add(3*time.Hour), now.Add(5*24*time.Hour)
		return shared.Latest{Observation: &shared.Observation{Kind: shared.KindAuto, MeasuredAt: now.Add(-time.Minute), Buckets: []shared.Bucket{
			{Window: "5h", UsedPct: 20, ResetsAt: &reset5h, Scope: "account"},
			{Window: "7d", UsedPct: weekly, ResetsAt: &reset7d, Scope: "account"},
		}}}, nil
	}
	return a, s, q, f
}

// (a) hk 3836: `-m` a profile with --operator-request on a gate=default rung.
func TestRequestShapeRefusalIsDecidedOnceBeforeAnyHostIsJudged(t *testing.T) {
	a, s, q, f := gateReasonFixture(t, 40)
	s.OperatorRequest, s.Purpose = "hk:task/3836", "operator-request"
	handled, rec, err := a.placeAuto(context.Background(), s, "brief")
	if !handled || rec != nil || err == nil {
		t.Fatalf("handled=%v rec=%v err=%v", handled, rec, err)
	}
	if !strings.Contains(err.Error(), "operator_request_not_applicable: ordinary placement") || exitCode(err) != quotaGateBlocked {
		t.Fatalf("the gate's reason and exit code must reach the caller verbatim: rc=%d %v", exitCode(err), err)
	}
	if strings.Contains(err.Error(), "quota_refused") || strings.Contains(err.Error(), "tried:") {
		t.Fatalf("a request refusal read as a per-host quota refusal: %v", err)
	}
	if q.gates != 0 {
		t.Fatalf("the host gate was asked %d times; no host may be judged", q.gates)
	}
	if n := sshCalls(f); n != 0 {
		t.Fatalf("a host was contacted %d times", n)
	}
}

// Every refusal the request, the catalog or policy decides is caught before the
// fan-out with its own code; none is judged on a host.
func TestRequestRefusalCodesCoverEveryReadingIndependentRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func(*Spawn)
	}{
		{"operator request on an ordinary rung", "operator_request_not_applicable", func(s *Spawn) { s.OperatorRequest, s.Purpose = "hk:task/3836", "operator-request" }},
		{"invalid operator request ref", "operator_request_ref_invalid", func(s *Spawn) { s.OperatorRequest, s.Purpose = "task 3836", "operator-request" }},
		{"unknown effort rung", "unknown_effort_rung", func(s *Spawn) { s.Effort = "turbo" }},
		{"retired or unknown profile", "profile_sonnet_is_unknown_or_retired", func(*Spawn) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, _ := gateReasonFixture(t, 40)
			if tc.name == "retired or unknown profile" {
				a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return nil, nil }
			}
			tc.edit(&s)
			_, _, err := a.placeAuto(context.Background(), s, "brief")
			if err == nil || gateCodeNote(err) != "gate_blocked:"+tc.want || q.gates != 0 {
				t.Fatalf("note=%q gates=%d err=%v, want gate_blocked:%s before any host", gateCodeNote(err), q.gates, err, tc.want)
			}
		})
	}
}

// receiptGate answers a gate with the receipt the native gate prints for a
// refusal: gateCode set for a refusal no reading decided, empty otherwise.
func receiptGate(r QuotaRequest, reason, gateCode string) (string, error) {
	b, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "provider_id": "claude", "account_key": r.AccountKey, "profile": "sonnet", "ok": false, "exit_code": 3, "reason": reason, "gate_code": gateCode})
	return gateResult(Result{Code: 3, Out: string(b)}, r)
}

// (b) Each refused host is noted with the gate's own code; only a refusal a
// reading decided is quota_refused.
func TestHostNoteKeepsGateCodeAndQuotaRefusedStaysForReadings(t *testing.T) {
	a, s, _, _ := gateReasonFixture(t, 40)
	n := 0
	a.Quota = &fakeQuota{LaunchValue: Launch{Pool: "claude"}, GateDo: func(r QuotaRequest) (string, error) {
		if n++; n == 1 {
			return receiptGate(r, "operator_request_not_applicable: ordinary placement", "operator_request_not_applicable")
		}
		return receiptGate(r, "quota exhausted window=7d cutoff=90", "")
	}}
	_, _, err := a.placeAuto(context.Background(), s, "brief")
	if err == nil || !strings.Contains(err.Error(), "first gate_blocked:operator_request_not_applicable; second quota_refused") {
		t.Fatalf("host notes: %v", err)
	}
}

// A refusal the production gate derives from a reading stays quota_refused on
// every host and carries no gate code.
func TestReadingRefusalIsNotACodedGateRefusal(t *testing.T) {
	a, s, q, _ := gateReasonFixture(t, 99)
	if refusal := q.RequestRefusal(context.Background(), QuotaRequest{Profile: "sonnet", Effort: "high", Role: "implementer"}); refusal != nil {
		t.Fatalf("a reading's refusal was decided before the hosts: %v", refusal)
	}
	_, _, err := a.placeAuto(context.Background(), s, "brief")
	if err == nil || !strings.Contains(err.Error(), "first quota_refused; second quota_refused") || strings.Contains(err.Error(), "gate_blocked") {
		t.Fatalf("host notes: %v", err)
	}
	if q.gates == 0 {
		t.Fatal("the hosts were never judged")
	}
}

// (c) The placement record of a gate-refused attempt carries the gate code in
// the host row's reason and no field the record does not already have.
func TestPlacementRecordCarriesGateCodeInHostReason(t *testing.T) {
	keys := func(v any) []string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	record := func(coded bool) PlacementRecord {
		a, s := remotePlacementFixture(t, "ssh", "auto", "refused", true)
		a.Quota.(*fakeQuota).GateDo = func(r QuotaRequest) (string, error) {
			if coded {
				return receiptGate(r, "operator_request_not_applicable: ordinary placement", "operator_request_not_applicable")
			}
			return receiptGate(r, "quota exhausted window=7d cutoff=90", "")
		}
		h := placementServer(t, a)
		if err := a.Spawn(context.Background(), s); err == nil {
			t.Fatal("a refused gate placed the job")
		}
		return onePlacement(t, h)
	}
	hostRow := func(r PlacementRecord) PlacementCandidateRecord {
		for _, c := range r.Candidates {
			if c.Host == "host-b" {
				return c
			}
		}
		t.Fatalf("no host-b row: %+v", r.Candidates)
		return PlacementCandidateRecord{}
	}
	coded, plain := record(true), record(false)
	if row := hostRow(coded); row.Outcome != "skipped" || row.Reason != "gate_blocked:operator_request_not_applicable" {
		t.Fatalf("host row: %+v", row)
	}
	if row := hostRow(plain); row.Reason != "quota_refused" {
		t.Fatalf("a reading's refusal changed its reason: %+v", row)
	}
	if got, want := keys(coded), keys(plain); !slices.Equal(got, want) {
		t.Fatalf("record fields changed: %v, want %v", got, want)
	}
	want := []string{"attempt", "candidates", "decided_at", "family", "job_id", "outcome", "pool", "profile", "ranking", "reason", "repo", "role", "source", "state_source", "task_id", "tool_version"}
	if got := keys(coded); !slices.Equal(got, want) {
		t.Fatalf("record fields: %v, want %v", got, want)
	}
	if got, want := keys(hostRow(coded)), keys(hostRow(plain)); !slices.Equal(got, want) {
		t.Fatalf("host row fields: %v, want %v", got, want)
	}
}

// (d) A request the gate admits is judged and placed exactly as before: no
// refusal before the hosts, one host-level gate question, the same receipt.
func TestAdmittedRequestKeepsItsVerdictAndPlacement(t *testing.T) {
	a, s, q, _ := gateReasonFixture(t, 40)
	ctx := context.Background()
	qr, _ := a.quotaRequest(s)
	if refusal := q.RequestRefusal(ctx, qr); refusal != nil {
		t.Fatalf("an admitted request was refused before the hosts: %v", refusal)
	}
	handled, rec, err := a.placeAuto(ctx, s, "brief")
	if err != nil || !handled || rec["chose"] != "first" {
		t.Fatalf("placement: handled=%v chose=%v err=%v", handled, rec["chose"], err)
	}
	if q.gates != 1 {
		t.Fatalf("host gate asked %d times, want 1", q.gates)
	}
	qr.AccountKey = "claude:0123456789abcdef"
	res := a.nativeGate(ctx, qr, true)
	var receipt map[string]any
	if err := json.Unmarshal([]byte(res.Out), &receipt); err != nil || res.Code != 0 {
		t.Fatalf("verdict: rc=%d %v %s", res.Code, err, res.Out)
	}
	if receipt["ok"] != true || receipt["pool_class"] == nil || receipt["used_pct"] != 40.0 || receipt["grade"] != "A" {
		t.Fatalf("verdict changed: %s", res.Out)
	}
	var got []string
	for k := range receipt {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"account_key", "alternatives", "escalation_override", "exit_code", "generated_at", "grade", "judged_by", "manual_observation_ids", "missing_windows", "ok", "operator_request_ref", "pool_class", "profile", "provider_id", "purpose", "reason", "ref_resolution", "requested_by", "role_denied", "schema", "source", "unmeasurable", "used_pct"}
	if !slices.Equal(got, want) {
		t.Fatalf("receipt fields: %v, want %v", got, want)
	}
}
