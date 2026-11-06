package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The hk fixtures under testdata/pr_status are real answers captured for
// mgh3326/panewire#269 (merged): hk's GET /v1/changes/one, and the snapshot
// mergewake held for the same PR and head, whose state was still "ready".

// prStatusFixtureNow is ten minutes after hk's newest section of the fixture.
var prStatusFixtureNow = time.Date(2026, 11, 6, 19, 15, 0, 0, time.FixedZone("KST", 9*3600))

func hkFixtureBytes(t *testing.T) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "pr_status", "changes_one_269.json"))
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

// hkFixtureWith applies edit to the decoded fixture and re-encodes it.
func hkFixtureWith(t *testing.T, edit func(doc map[string]any)) string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(hkFixtureBytes(t), &doc); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(doc)
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// openPR turns the fixture into the same PR before it merged.
func openPR(doc map[string]any) {
	change := doc["change"].(map[string]any)
	change["state"] = "open"
	delete(change, "merged_at")
}

// mergewakeFixture269 is the generic mergewake fixture, addressed to #269.
var mergewakeFixture269 = strings.NewReplacer(`"owner/name"`, `"mgh3326/panewire"`, `"pr": 152`, `"pr": 269`).Replace(prStatusFixture)

func hkNodeClient(t *testing.T, hkURL string, now time.Time) *HubClient {
	t.Helper()
	client := prStatusNodeClient(t, hkURL)
	client.now = func() time.Time { return now }
	return client
}

// prFixtureHK is the hk half the node answers for the #269 fixture.
func prFixtureHK(t *testing.T, now time.Time) nodePRStatusHK {
	t.Helper()
	hk := newPRStatusHKServer(t, http.StatusOK, string(hkFixtureBytes(t)))
	return hkNodeClient(t, hk.URL, now).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
}

func TestPRStatusNodeDefaultReadAnswersFromHKAlone(t *testing.T) {
	t.Setenv("HANDOFFKEEP_TOKEN", "hk_leak_canary_value")
	fake := installFakeMergewake(t, "cat '"+fixtureFile(t, prStatusFixture)+"'")
	hk := newPRStatusHKServer(t, http.StatusOK, string(hkFixtureBytes(t)))
	result, code := nodeRun(t, hkNodeClient(t, hk.URL, prStatusFixtureNow), nodeCommandPRStatus, prStatusArgsJSON("mgh3326/panewire", 269, false))
	if code != "" {
		t.Fatal(code)
	}
	if got := result["mergewake"].(map[string]any); got["outcome"] != "not_asked" || got["snapshot"] != nil {
		t.Fatalf("mergewake %v", got)
	}
	// Zero mergewake: the fake would have recorded its argv.
	if _, err := os.Stat(fake.argv); err == nil {
		t.Fatalf("mergewake ran: %s", fake.read(t, "argv"))
	}
	if len(hk.requests) != 1 {
		t.Fatalf("hk requests %d", len(hk.requests))
	}
	request := hk.requests[0]
	if request.Method != http.MethodGet || request.URL.Path != "/v1/changes/one" || request.URL.Query().Get("repo") != "mgh3326/panewire" || request.URL.Query().Get("pr") != "269" || request.Header.Get("Authorization") != "Bearer hk-secret-token-1" {
		t.Fatalf("hk request %s auth=%q", request.URL.String(), request.Header.Get("Authorization"))
	}
	source := result["hk"].(map[string]any)
	if source["outcome"] != "ok" || source["status"] != float64(200) {
		t.Fatalf("hk %v", source)
	}
	snapshot := source["snapshot"].(map[string]any)
	if snapshot["state"] != "merged" || snapshot["source"] != "hk" || snapshot["head"] != "7ce60957ab4f4b77b5f25be4ba14d81d0414e6d9" || snapshot["observed_at"] != "2026-11-06T18:43:55.983983+09:00" {
		t.Fatalf("snapshot %v", snapshot)
	}
	checks := snapshot["checks"].([]any)
	if len(checks) != 1 || checks[0].(map[string]any)["name"] != "test (ubuntu-latest)" || checks[0].(map[string]any)["state"] != "green" {
		t.Fatalf("checks %v: only the required check is listed", checks)
	}
	sections := snapshot["sections"].(map[string]any)
	if gate := sections["gate"].(map[string]any); gate["observed_at"] != "2026-11-06T18:44:21.809581+09:00" || gate["source"] != "review" || len(sections) != 5 {
		t.Fatalf("sections %v", sections)
	}
	// The record the node passes on is a projection: no thread excerpt travels.
	if encoded, _ := json.Marshal(source["change"]); len(encoded) > prStatusMaxHKChange || strings.Contains(string(encoded), "coderabbit") || strings.Contains(string(encoded), "hk-secret-token-1") {
		t.Fatalf("change %s", encoded)
	}
}

// Acceptance 1: the same PR renders the same from hk and from the mergewake
// snapshot, except that hk knows the PR merged.
func TestPRStatusHKFixtureRendersLikeTheMergewakeFixture(t *testing.T) {
	hkView := prStatusView{Source: prSourcePanewire, Repo: "mgh3326/panewire", PR: 269, Node: &nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusNotAsked}, HK: prFixtureHK(t, prStatusFixtureNow),
	}}
	mergewakeFixture, err := os.ReadFile(filepath.Join("testdata", "pr_status", "mergewake_269.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot PRStatusSnapshot
	if err := json.Unmarshal(mergewakeFixture, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = boundPRStatusSnapshot(snapshot)
	mergewakeView := prStatusView{Source: prSourcePanewire, Repo: "mgh3326/panewire", PR: 269, Node: &nodePRStatusResult{
		Mergewake: nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot}, HK: nodePRStatusHK{Outcome: prStatusNotFound, Status: 404},
	}}
	render := func(view prStatusView) (status, findings string) {
		var out, errOut bytes.Buffer
		if code := printPRStatus(&out, view, false, prStatusFixtureNow); code != ExitOK {
			t.Fatalf("status code %d", code)
		}
		status = out.String()
		var list bytes.Buffer
		if code := printPRFindings(&list, &errOut, view, false); code != ExitOK {
			t.Fatalf("findings code %d", code)
		}
		return status, list.String()
	}
	hkStatus, hkFindings := render(hkView)
	mergewakeStatus, mergewakeFindings := render(mergewakeView)
	const want = "mgh3326/panewire#269 merged head=7ce6095 blocking=0 checks=1/1 green observed=31m ago\n"
	if hkStatus != want {
		t.Fatalf("hk status %q", hkStatus)
	}
	if mergewakeStatus != strings.Replace(want, " merged ", " ready ", 1) {
		t.Fatalf("mergewake status %q", mergewakeStatus)
	}
	const wantFinding = "-\tnonblocking\t.github/workflows/ci.yml:30\tMinor · Stability & Availability · Low value\tunreplied\thttps://github.com/mgh3326/panewire/pull/269#discussion_r4491011144\n"
	if hkFindings != wantFinding || mergewakeFindings != wantFinding {
		t.Fatalf("findings hk=%q mergewake=%q", hkFindings, mergewakeFindings)
	}
}

// A webhook-written check has no required flag yet; it is shown rather than
// hidden, and a check known to be optional is not.
func TestPRStatusHKChecksWithUnknownRequiredAreShown(t *testing.T) {
	body := hkFixtureWith(t, func(doc map[string]any) {
		openPR(doc)
		doc["checks"] = []any{
			map[string]any{"name": "unknown-green", "state": "completed", "conclusion": "success"},
			map[string]any{"name": "unknown-running", "state": "in_progress"},
			map[string]any{"name": "optional", "state": "completed", "conclusion": "failure", "required": false},
			map[string]any{"name": "required-red", "state": "completed", "conclusion": "failure", "required": true},
			map[string]any{"name": "gone", "state": "completed", "conclusion": "success", "required": true, "gone": true},
		}
	})
	hk := newPRStatusHKServer(t, http.StatusOK, body)
	result := hkNodeClient(t, hk.URL, prStatusFixtureNow).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
	if result.Outcome != prStatusOK {
		t.Fatalf("hk %+v", result)
	}
	if got := prChecksSummary(result.Snapshot.Checks); got != "1/3 green pending:1 red:1" {
		t.Fatalf("checks %q: %+v", got, result.Snapshot.Checks)
	}
}

func TestPRStatusHKOpenPRSnapshot(t *testing.T) {
	// A human thread blocks, a bot thread does not; a resolved one is not a finding.
	body := hkFixtureWith(t, func(doc map[string]any) {
		openPR(doc)
		doc["gate"] = map[string]any{"state": "blocked", "head_sha": "abc1234deadbeef", "current": false, "reasons": []string{"threads_open:1"}, "decided_at": "2026-11-06T19:00:00+09:00", "source": "review"}
		threads := doc["threads"].([]any)
		human := map[string]any{"thread_id": "PRRT_human", "url": "https://x/1", "path": "a.go", "line": 7, "author_kind": "human", "severity": "high", "resolved": false, "replied": true, "excerpt": "<!-- m -->\n\n**fix this** now"}
		doc["threads"] = append([]any{human}, threads...)
		doc["findings"] = []any{map[string]any{"finding_id": "f1", "status": "open"}, map[string]any{"finding_id": "f2", "status": "fixed"}, map[string]any{"finding_id": "f3", "thread_id": "PRRT_human", "status": "open"}}
	})
	hk := newPRStatusHKServer(t, http.StatusOK, body)
	result := hkNodeClient(t, hk.URL, prStatusFixtureNow.Add(-5*time.Minute)).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
	if result.Outcome != prStatusOK || result.Snapshot == nil {
		t.Fatalf("hk %+v", result)
	}
	snapshot := result.Snapshot
	if snapshot.State != "blocked" || snapshot.GateHead != "abc1234deadbeef" || snapshot.Head == snapshot.GateHead || strings.Join(snapshot.Reasons, ",") != "threads_open:1" {
		t.Fatalf("snapshot %+v", snapshot)
	}
	if snapshot.Threads.Blocking != 1 || snapshot.Threads.Other != 1 || *snapshot.Threads.Unresolved != 2 || len(snapshot.Findings) != 2 {
		t.Fatalf("threads %+v findings %+v", snapshot.Threads, snapshot.Findings)
	}
	first := snapshot.Findings[0]
	if first.ID != "PRRT_human" || first.Location != "a.go:7" || first.Severity != "high" || !first.Blocking || !first.AuthorReplied || first.Summary != "fix this now" {
		t.Fatalf("finding %+v", first)
	}
	if result.Unposted != 1 {
		t.Fatalf("unposted %d: only the open finding with no thread counts", result.Unposted)
	}
	// Observed is the oldest section the snapshot used.
	if snapshot.ObservedAt != "2026-11-06T18:43:55.983983+09:00" {
		t.Fatalf("observed %q", snapshot.ObservedAt)
	}
}

// Acceptance 2: every reason hk cannot answer falls back to mergewake, with a
// closed outcome the CLI turns into a note.
func TestPRStatusNodeFallsBackToMergewakeWhenHKCannotAnswer(t *testing.T) {
	late := prStatusFixtureNow.Add(3 * time.Hour)
	cases := map[string]struct {
		status  int
		body    string
		now     time.Time
		outcome string
		reason  string
	}{
		"hk 404":        {404, `{"error":"not found"}`, prStatusFixtureNow, "not_found", ""},
		"hk error":      {500, `boom`, prStatusFixtureNow, "failed", "http_status"},
		"forbidden":     {403, `{}`, prStatusFixtureNow, "failed", "http_status"},
		"not an object": {200, `[1,2]`, prStatusFixtureNow, "failed", "unreadable_body"},
		"not json":      {200, `<html>`, prStatusFixtureNow, "failed", "unreadable_body"},
		"another PR":    {200, hkFixtureWith(t, func(doc map[string]any) { doc["change"].(map[string]any)["pr_number"] = 270 }), prStatusFixtureNow, "failed", "unreadable_body"},
		"too large":     {200, `{"pad":"` + strings.Repeat("x", prStatusMaxHKBody) + `"}`, prStatusFixtureNow, "too_large", ""},
		"stale state":   {200, hkFixtureWith(t, openPR), late, "stale", ""},
		"missing gate": {200, hkFixtureWith(t, func(doc map[string]any) {
			delete(doc["freshness"].(map[string]any), "gate")
		}), prStatusFixtureNow, "stale", ""},
		"gate row absent": {200, hkFixtureWith(t, func(doc map[string]any) { doc["gate"] = nil }), prStatusFixtureNow, "stale", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fake := installFakeMergewake(t, "cat '"+fixtureFile(t, mergewakeFixture269)+"'")
			hk := newPRStatusHKServer(t, tc.status, tc.body)
			args := prStatusArgsJSON("mgh3326/panewire", 269, false)
			result, code := nodeRun(t, hkNodeClient(t, hk.URL, tc.now), nodeCommandPRStatus, args)
			if code != "" {
				t.Fatalf("op error %q: an hk that cannot answer must not fail the op", code)
			}
			source := result["hk"].(map[string]any)
			if source["outcome"] != tc.outcome || (tc.reason != "" && source["reason"] != tc.reason) || source["snapshot"] != nil {
				t.Fatalf("hk %v", source)
			}
			if got := result["mergewake"].(map[string]any); got["outcome"] != "ok" || fake.read(t, "argv") == "" {
				t.Fatalf("mergewake %v", got)
			}
			if encoded, _ := json.Marshal(result); strings.Contains(string(encoded), "hk-secret-token-1") {
				t.Fatalf("hk token in result: %s", encoded)
			}
		})
	}
}

func TestPRStatusNodeStaleSectionsAreNamedWithTheirTimes(t *testing.T) {
	installFakeMergewake(t, "exit 1")
	hk := newPRStatusHKServer(t, http.StatusOK, hkFixtureWith(t, openPR))
	// At 19:15 the 19:05 state is inside its fifteen minute bound and the 18:43
	// checks inside their hour; at 20:00 every section is past its bound.
	result := hkNodeClient(t, hk.URL, prStatusFixtureNow).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
	if result.Outcome != prStatusOK {
		t.Fatalf("hk %+v", result)
	}
	result = hkNodeClient(t, hk.URL, prStatusFixtureNow.Add(45*time.Minute)).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
	if result.Outcome != prStatusStale || len(result.Stale) != 4 {
		t.Fatalf("hk %+v", result)
	}
	want := PRStatusStale{Section: "state", ObservedAt: "2026-11-06T19:05:19.974184+09:00", Source: "poll", Problem: "older_than_bound", BoundSeconds: 900}
	if result.Stale[0] != want {
		t.Fatalf("stale[0] %+v", result.Stale[0])
	}
	if describe := prStatusDescribeStale(result.Stale); !strings.HasPrefix(describe, "state observed_at=2026-11-06T19:05:19.974184+09:00 source=poll is older than 15m0s; checks observed_at=2026-11-06T18:43:55.983983+09:00 source=fresh is older than 1h0m0s") {
		t.Fatalf("describe %q", describe)
	}
}

// A merged PR cannot change, so no section ages out.
func TestPRStatusHKMergedPRHasNoFreshnessBound(t *testing.T) {
	hk := newPRStatusHKServer(t, http.StatusOK, string(hkFixtureBytes(t)))
	result := hkNodeClient(t, hk.URL, prStatusFixtureNow.Add(30*24*time.Hour)).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
	if result.Outcome != prStatusOK || result.Snapshot.State != "merged" {
		t.Fatalf("hk %+v", result)
	}
}

func TestPRStatusNodeFreshAsksMergewakeFirstAndKeepsItsSnapshot(t *testing.T) {
	var mu sync.Mutex
	var order []string
	fake := installFakeMergewake(t, "cat '"+fixtureFile(t, mergewakeFixture269)+"'")
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, "hk")
		mu.Unlock()
		if _, err := os.Stat(fake.argv); err != nil {
			t.Error("hk was read before mergewake ran")
		}
		_, _ = w.Write(hkFixtureBytes(t))
	}))
	t.Cleanup(hk.Close)
	args := prStatusArgsJSON("mgh3326/panewire", 269, true)
	result, code := nodeRun(t, hkNodeClient(t, hk.URL, prStatusFixtureNow), nodeCommandPRStatus, args)
	if code != "" {
		t.Fatal(code)
	}
	merge, source := result["mergewake"].(map[string]any), result["hk"].(map[string]any)
	if merge["outcome"] != "ok" || source["outcome"] != "ok" || source["snapshot"] != nil || !strings.Contains(fake.read(t, "argv"), "--fresh") || len(order) != 1 {
		t.Fatalf("mergewake=%v hk=%v argv=%q", merge, source, fake.read(t, "argv"))
	}
	// If mergewake cannot answer, hk's snapshot is kept for the CLI.
	installFakeMergewake(t, "exit 1")
	result, _ = nodeRun(t, hkNodeClient(t, hk.URL, prStatusFixtureNow), nodeCommandPRStatus, args)
	if result["hk"].(map[string]any)["snapshot"] == nil {
		t.Fatalf("hk %v", result["hk"])
	}
}

func TestPRStatusExcerptSummary(t *testing.T) {
	for name, tc := range map[string]struct{ in, want string }{
		"bot header":      {"<!-- coderabbit-ai-generated:v1 -->\n\n**<picture><img src=\"https://x/a.svg\" width=\"16\" alt=\"\"></picture> Minor** · **Stability** · **Low value**\n\n<details>", "Minor · Stability · Low value"},
		"cut mid tag":     {"<picture><img src=\"https://x/a.sv", ""},
		"plain":           {"  Please rename this.\nsecond line", "Please rename this."},
		"code and breaks": {"\n\n`x := 1` is wrong\t here", "x := 1 is wrong here"},
		"empty":           {"", ""},
	} {
		if got := prStatusExcerptSummary(tc.in); got != tc.want {
			t.Errorf("%s: %q want %q", name, got, tc.want)
		}
	}
}

func TestValidNodePRStatusResultForHKAnswers(t *testing.T) {
	hk := prFixtureHK(t, prStatusFixtureNow)
	var snapshot PRStatusSnapshot
	if err := json.Unmarshal([]byte(prStatusFixture), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = boundPRStatusSnapshot(snapshot)
	mergewake := nodePRStatusMergewake{Outcome: prStatusOK, Snapshot: &snapshot}
	notAsked := nodePRStatusMergewake{Outcome: prStatusNotAsked}
	stale := nodePRStatusHK{Outcome: prStatusStale, Status: 200, Change: hk.Change, Stale: []PRStatusStale{{Section: "gate", Problem: prStatusSectionMissing}}}
	withStale := func(edit func(*nodePRStatusHK)) nodePRStatusHK {
		copied := stale
		copied.Stale = append([]PRStatusStale(nil), stale.Stale...)
		edit(&copied)
		return copied
	}
	for name, tc := range map[string]struct {
		result nodePRStatusResult
		valid  bool
	}{
		"hk answered alone":             {nodePRStatusResult{notAsked, hk}, true},
		"not asked without hk snapshot": {nodePRStatusResult{notAsked, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: hk.Change}}, false},
		"not asked and hk 404":          {nodePRStatusResult{notAsked, nodePRStatusHK{Outcome: prStatusNotFound, Status: 404}}, false},
		"not asked with a snapshot":     {nodePRStatusResult{nodePRStatusMergewake{Outcome: prStatusNotAsked, Snapshot: &snapshot}, hk}, false},
		"mergewake and hk record":       {nodePRStatusResult{mergewake, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: hk.Change}}, true},
		"stale":                         {nodePRStatusResult{mergewake, stale}, true},
		"stale without sections":        {nodePRStatusResult{mergewake, withStale(func(h *nodePRStatusHK) { h.Stale = nil })}, false},
		"stale with a free problem":     {nodePRStatusResult{mergewake, withStale(func(h *nodePRStatusHK) { h.Stale[0].Problem = "token ghp_x" })}, false},
		"stale without a record":        {nodePRStatusResult{mergewake, withStale(func(h *nodePRStatusHK) { h.Change = nil })}, false},
		"stale carrying a snapshot":     {nodePRStatusResult{mergewake, withStale(func(h *nodePRStatusHK) { h.Snapshot = hk.Snapshot })}, false},
		"stale list on an ok record":    {nodePRStatusResult{mergewake, nodePRStatusHK{Outcome: prStatusOK, Status: 200, Change: hk.Change, Stale: stale.Stale}}, false},
		"snapshot on a 404":             {nodePRStatusResult{mergewake, nodePRStatusHK{Outcome: prStatusNotFound, Status: 404, Snapshot: hk.Snapshot}}, false},
	} {
		if got := validNodePRStatusResult(tc.result); got != tc.valid {
			t.Errorf("%s: valid=%v want %v", name, got, tc.valid)
		}
	}
}

func prStaleHK(t *testing.T) nodePRStatusHK {
	t.Helper()
	hk := newPRStatusHKServer(t, http.StatusOK, hkFixtureWith(t, openPR))
	return hkNodeClient(t, hk.URL, prStatusFixtureNow.Add(45*time.Minute)).readNodePRStatusHK(t.Context(), nodePRStatusArgs{Repo: "mgh3326/panewire", PR: 269})
}

// Acceptance 2 at the CLI: each way hk cannot answer says so, section and
// observed_at included, and then mergewake answers; gh only when it fails too.
func TestPRStatusCLINotesWhyHKWasNotUsed(t *testing.T) {
	gh := fakeGH(t, ghViewFixture, ghThreadsFixture)
	stale := prStaleHK(t)
	if stale.Outcome != prStatusStale {
		t.Fatalf("hk %+v", stale)
	}
	stub := newPRCLIStub(t, 200, prNodeReply(t, prFixtureResult(t, stale)))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	wantNote := "note: hk record not used: state observed_at=2026-11-06T19:05:19.974184+09:00 source=poll is older than 15m0s; checks observed_at=2026-11-06T18:43:55.983983+09:00 source=fresh is older than 1h0m0s"
	if run.code != ExitOK || !strings.HasPrefix(run.stdout, "owner/name#152 blocked") || !strings.Contains(run.stderr, wantNote) || !strings.Contains(run.stderr, "; used the mergewake snapshot") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	for name, hk := range map[string]nodePRStatusHK{
		"hk error":   {Outcome: prStatusFailed, Reason: prStatusReasonHTTPStatus},
		"hk 404":     prHKNotDeployed,
		"hk too big": {Outcome: prStatusTooLarge},
	} {
		stub.response = prNodeReply(t, prFixtureResult(t, hk))
		run = runPR(t, append([]string{"findings", "owner/name", "152"}, connection...)...)
		if run.code != ExitOK || !strings.Contains(run.stderr, "note: hk change record omitted: ") || !strings.Contains(run.stderr, "; used the mergewake snapshot") || !strings.Contains(run.stdout, "internal/prstatus/handler.go:42") {
			t.Fatalf("%s: code=%d stdout=%q stderr=%q", name, run.code, run.stdout, run.stderr)
		}
	}
	// mergewake failing as well falls back to gh, and says why.
	both := nodePRStatusResult{Mergewake: nodePRStatusMergewake{Outcome: prStatusFailed, Reason: prStatusReasonExit}, HK: stale}
	stub.response = prNodeReply(t, both)
	run = runPR(t, append([]string{"status", "owner/name", "152"}, connection...)...)
	if run.code != ExitOK || !strings.Contains(run.stdout, "source=gh (fallback: mergewake failed (exit_status), handoffkeep record is stale (state observed_at=2026-11-06T19:05:19.974184+09:00") {
		t.Fatalf("code=%d stdout=%q stderr=%q", run.code, run.stdout, run.stderr)
	}
	if log := readLog(t, gh); !strings.Contains(log, "pr view") {
		t.Fatalf("gh log %q", log)
	}
}

func TestPRStatusCLIJSONCarriesHKSections(t *testing.T) {
	hk := nodePRStatusResult{Mergewake: nodePRStatusMergewake{Outcome: prStatusNotAsked}, HK: prFixtureHK(t, prStatusFixtureNow)}
	stub := newPRCLIStub(t, 200, prNodeReply(t, hk))
	connection := prCLIConnection(t, stub.URL)
	run := runPR(t, append([]string{"status", "mgh3326/panewire", "269", "--json"}, connection...)...)
	var merged struct {
		Source    string                     `json:"source"`
		Mergewake *PRStatusSnapshot          `json:"mergewake"`
		HK        map[string]json.RawMessage `json:"hk"`
		Snapshot  PRStatusSnapshot           `json:"snapshot"`
		Sections  map[string]PRStatusSection `json:"sections"`
		Notes     []string                   `json:"notes"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &merged); err != nil {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
	if merged.Source != "panewire" || merged.Mergewake != nil || merged.HK["gate"] == nil || merged.Snapshot.State != "merged" || len(merged.Notes) != 0 {
		t.Fatalf("merged %+v", merged)
	}
	if section := merged.Sections["threads"]; section.ObservedAt != "2026-11-06T18:44:01.476986+09:00" || section.Source != "fresh" {
		t.Fatalf("sections %+v", merged.Sections)
	}
	run = runPR(t, append([]string{"findings", "mgh3326/panewire", "269", "--json"}, connection...)...)
	var listed struct {
		Sections map[string]PRStatusSection `json:"sections"`
		Findings []PRStatusFinding          `json:"findings"`
	}
	if err := json.Unmarshal([]byte(run.stdout), &listed); err != nil || len(listed.Findings) != 1 || listed.Sections["state"].Source != "poll" {
		t.Fatalf("stdout=%q err=%v", run.stdout, err)
	}
}
