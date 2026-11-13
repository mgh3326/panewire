package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/handoffkeep/client"
)

// hkSiteApp is a test App whose hk is a fake reached with the svc-panewire
// credential and whose runner fails the test if the spawn path runs the hk
// CLI.
func hkSiteApp(t *testing.T) (*App, *spawnHK) {
	t.Helper()
	a := testApp(t)
	hk := useFakeHK(t)
	failHKCLI(t, a.Runner.(*fakeRunner))
	return a, hk
}

// hkBody decodes a request's JSON body into a string map.
func hkBody(t *testing.T, r hkRequest) map[string]string {
	t.Helper()
	var out map[string]string
	if e := json.Unmarshal(r.Body, &out); e != nil {
		t.Fatalf("%s: body %q is not a JSON object of strings: %v", r.Route(), r.Body, e)
	}
	return out
}

// hkOnly fails the test unless the fake hk received exactly routes, in order.
func hkOnly(t *testing.T, hk *spawnHK, routes ...string) []hkRequest {
	t.Helper()
	if got := hk.Routes(); !slices.Equal(got, routes) {
		t.Fatalf("hk routes=%q want %q", got, routes)
	}
	return hk.Requests()
}

// Every spawn-path hk call listed in hk 3591 reaches its hk route with the
// svc-panewire bearer (the fake checks it on every request) and the body the
// hk CLI sent, and none runs the hk CLI.
func TestSpawnPathHKCallsGoOverHTTPWithSvcToken(t *testing.T) {
	ctx := context.Background()

	t.Run("precheck and claim", func(t *testing.T) {
		a, s, f, _ := cmdFixture(t)
		hk := useFakeHK(t)
		failHKCLI(t, f)
		hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`)).Handle(hkClaimAnswer)
		s.Owner = "owner"
		if e := a.Spawn(ctx, s); e != nil {
			t.Fatal(e)
		}
		// Task calls only; the routing read and the placement record are
		// other subtests' and other tickets' subjects.
		var reqs, claims []hkRequest
		for _, r := range hk.Requests() {
			switch {
			case r.Route() == "GET /v1/tasks/42":
			case r.Route() == "POST /v1/tasks/42/claim":
				claims = append(claims, r)
			case strings.HasPrefix(r.Path, "/v1/tasks/"):
				t.Errorf("unexpected hk task call %s", r.Route())
			default:
				continue
			}
			reqs = append(reqs, r)
		}
		if len(claims) != 1 || len(reqs) < 2 || reqs[0].Route() != "GET /v1/tasks/42" {
			t.Fatalf("hk routes=%q want the task read before one claim", hk.Routes())
		}
		if got := hkBody(t, claims[0]); len(got) != 2 || got["claimed_by"] != s.Label || got["job_id"] != s.Job {
			t.Fatalf("claim body=%v", got)
		}
	})

	t.Run("claim failure reads the task link", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`)).
			Handle(hkFails("POST /v1/tasks/42/claim", http.StatusConflict, `{"error":"conflict","reason":"claimed by another lane"}`))
		s := Spawn{Task: "42", Job: "job-a", Label: "worker-a"}
		task, bypassed, e := a.taskPrecheck(ctx, s)
		if e != nil {
			t.Fatal(e)
		}
		if e := a.taskLink(ctx, s, task, bypassed); e == nil || e.Error() != "task claim exited with status 1" {
			t.Fatalf("taskLink=%v", e)
		}
		hkOnly(t, hk, "GET /v1/tasks/42", "POST /v1/tasks/42/claim", "GET /v1/tasks/42")
	})

	t.Run("rebind", func(t *testing.T) {
		fx := newRebindFixture(t, "completed", rebindParent)
		hk := useFakeHK(t)
		failHKCLI(t, fx.f)
		hk.SetTask("42", hkTaskJSON(42, "developing", "implement (grade B, T1)", rebindParent, "panewire", `{"job_id":"`+rebindParent+`"}`)).
			Handle(func(r hkRequest) (int, string, bool) {
				if r.Route() != "POST /v1/tasks/42/rebind" {
					return 0, "", false
				}
				return http.StatusOK, hkTaskJSON(42, "developing", "implement (grade B, T1)", fx.s.Label, "panewire", `{"job_id":"`+fx.s.Job+`"}`), true
			})
		if e := fx.a.Spawn(ctx, fx.s); e != nil {
			t.Fatal(e)
		}
		var rebinds []hkRequest
		for _, r := range hk.Requests() {
			if r.Route() == "POST /v1/tasks/42/rebind" {
				rebinds = append(rebinds, r)
			} else if strings.HasPrefix(r.Path, "/v1/tasks/") && r.Route() != "GET /v1/tasks/42" {
				t.Errorf("unexpected hk task call %s", r.Route())
			}
		}
		if len(rebinds) != 1 {
			t.Fatalf("hk routes=%q want one rebind", hk.Routes())
		}
		want := map[string]string{"from_job": rebindParent, "to_job": fx.s.Job, "by": fx.s.Label, "reason": "respawn continues completed parent job " + rebindParent}
		if got := hkBody(t, rebinds[0]); !maps.Equal(got, want) {
			t.Fatalf("rebind body=%v want %v", got, want)
		}
		if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("owner", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("77", hkTaskJSON(77, "ready", "implement (grade B, T1)", "", "mergewake", `{}`)).
			SetOwner("mergewake", `{"name":"mergewake","design_lane":"mergewake-design","effective_owner":"mergewake-design","events":[]}`)
		s := Spawn{Task: "77"}
		if e := a.resolveOwner(ctx, &s); e != nil {
			t.Fatal(e)
		}
		if s.Owner != "mergewake-design" {
			t.Fatalf("owner=%q", s.Owner)
		}
		hkOnly(t, hk, "GET /v1/tasks/77", "GET /v1/tasks/projects/mergewake/owner")
	})

	t.Run("routing policy", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetDoc(routingDocKey, routingDocV1)
		p, source, e := a.routingPolicy(ctx)
		if e != nil || source != routingSourceHK || p.Version != 1 {
			t.Fatalf("routingPolicy=v%d %q %v", p.Version, source, e)
		}
		hkOnly(t, hk, "GET /v1/documents/policy%2Fmodel-routing")
	})

	t.Run("brief upload", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		body := "# brief\n\nDo the work — exactly.\n" + strings.Repeat("x", 70<<10)
		key, e := a.putBrief(ctx, Spawn{Job: "job-a"}, "briefs/node-a/", body)
		if e != nil || key != "briefs/node-a/job-a.md" {
			t.Fatalf("putBrief=%q %v", key, e)
		}
		reqs := hkOnly(t, hk, "PUT /v1/documents/briefs%2Fnode-a%2Fjob-a.md")
		var doc struct{ Key, Kind, Job, Body, Session string }
		if e := json.Unmarshal(reqs[0].Body, &doc); e != nil {
			t.Fatal(e)
		}
		if doc.Key != key || doc.Kind != "brief" || doc.Job != "job-a" || doc.Body != body || doc.Session != "" {
			t.Fatalf("document key=%q kind=%q job=%q session=%q body %d bytes (want %d)", doc.Key, doc.Kind, doc.Job, doc.Session, len(doc.Body), len(body))
		}
	})

	t.Run("brief upload refused", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.Handle(hkFails("PUT /v1/documents/", http.StatusUnprocessableEntity, `{"error":"secret_like_content","pattern":"github_token"}`))
		_, e := a.putBrief(ctx, Spawn{Job: "job-a"}, "briefs/node-a/", "brief")
		if exitCode(e) != exitPlacement || e.Error() != "brief upload to briefs/node-a/job-a.md failed: handoffkeep: secret_like_content:github_token" {
			t.Fatalf("putBrief error=%v (exit %d)", e, exitCode(e))
		}
	})
}

// With no svc-panewire credential every spawn-path hk call fails closed with
// the unconfigured credential named, as an unreachable hk did, and none runs
// the hk CLI or reads its config.env.
func TestSpawnPathHKUnconfiguredRunsNoSubprocess(t *testing.T) {
	ctx := context.Background()
	a := testApp(t)
	f := a.Runner.(*fakeRunner)
	failHKCLI(t, f)
	// The hk CLI's own credential is present and must stay unused.
	t.Setenv("HANDOFFKEEP_TOKEN", hkCLITestToken)
	const unconfigured = "hk service credential unconfigured"

	if _, _, e := a.taskPrecheck(ctx, Spawn{Task: "42", Job: "job-a", Label: "worker-a"}); exitCode(e) != exitConfig || !strings.Contains(e.Error(), "task precheck unavailable") || !strings.Contains(e.Error(), unconfigured) {
		t.Fatalf("precheck=%v", e)
	}
	if _, bypassed, e := a.taskPrecheck(ctx, Spawn{Task: "42", Job: "job-a", Label: "worker-a", Bypass: true}); e != nil || !bypassed {
		t.Fatalf("bypassed precheck=%t %v", bypassed, e)
	}
	s := Spawn{Task: "42"}
	if e := a.resolveOwner(ctx, &s); exitCode(e) != exitUsage || !strings.Contains(e.Error(), unconfigured) || !strings.Contains(e.Error(), "--owner") {
		t.Fatalf("owner=%v", e)
	}
	if a.taskLinkedTo(ctx, "42", "job-a") {
		t.Fatal("an unreadable task counted as linked")
	}
	if _, e := a.putBrief(ctx, Spawn{Job: "job-a"}, "briefs/n/", "brief"); exitCode(e) != exitPlacement || !strings.Contains(e.Error(), unconfigured) {
		t.Fatalf("putBrief=%v", e)
	}
	if _, _, e := a.routingPolicy(ctx); e == nil || !strings.Contains(e.Error(), unconfigured) {
		t.Fatalf("routingPolicy without a cache=%v", e)
	}
	for _, c := range f.Calls {
		if c.Name == "handoffkeep" {
			t.Errorf("ran the hk CLI: %v", c.Args)
		}
	}
}

// Contract ② (hk 3591): a routing doc that fails parseRoutingDoc never
// overwrites the last good cache, and the cached copy is used with a warning;
// so is the cache when hk is unreachable or the credential unconfigured.
func TestRoutingPolicyKeepsLastGoodCacheOverHTTP(t *testing.T) {
	ctx := context.Background()
	seed := func(t *testing.T, a *App) []byte {
		t.Helper()
		p, e := parseRoutingDoc(routingDocV1)
		if e != nil {
			t.Fatal(e)
		}
		if e := a.writeRoutingCache(p); e != nil {
			t.Fatal(e)
		}
		b, e := os.ReadFile(a.routingCachePath())
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	check := func(t *testing.T, a *App, cached []byte, why string) {
		t.Helper()
		p, source, e := a.routingPolicy(ctx)
		if e != nil || source != "cache@v1" || p.Version != 1 {
			t.Fatalf("routingPolicy=v%d %q %v", p.Version, source, e)
		}
		warn := a.Err.(*bytes.Buffer).String()
		if !strings.Contains(warn, "using the cached routing policy v1") || !strings.Contains(warn, why) {
			t.Fatalf("warning=%q want the cache named and %q", warn, why)
		}
		if b, e := os.ReadFile(a.routingCachePath()); e != nil || !bytes.Equal(b, cached) {
			t.Fatalf("cache changed: %v\n%s", e, b)
		}
	}

	for _, tc := range []struct{ name, body, why string }{
		{"no json block", "# policy/model-routing\n\nprose only\n", "unreadable policy/model-routing: no json block"},
		{"schema invalid", routingDoc(`{"version": 2, "roles": {"implementer": {"allowed": [], "effort": "high", "on_exhaustion": "wait"}}}`), "unreadable policy/model-routing"},
		{"duplicate key", routingDoc(`{"version": 2, "version": 3}`), "unreadable policy/model-routing"},
	} {
		t.Run("invalid doc "+tc.name, func(t *testing.T) {
			a, hk := hkSiteApp(t)
			cached := seed(t, a)
			hk.SetDoc(routingDocKey, tc.body)
			check(t, a, cached, tc.why)
			hkOnly(t, hk, "GET /v1/documents/policy%2Fmodel-routing")
		})
	}
	t.Run("doc missing", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		cached := seed(t, a)
		check(t, a, cached, "not_found")
		hkOnly(t, hk, "GET /v1/documents/policy%2Fmodel-routing")
	})
	t.Run("hk unreachable", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		cached := seed(t, a)
		hk.Close()
		check(t, a, cached, "hk doc get policy/model-routing")
	})
	t.Run("unconfigured", func(t *testing.T) {
		a := testApp(t)
		failHKCLI(t, a.Runner.(*fakeRunner))
		cached := seed(t, a)
		check(t, a, cached, "hk service credential unconfigured")
	})
}

// The CLI path's parsers, as panewire@8422639 ran them on `handoffkeep tasks
// show` and `tasks owner` output (spawn.go taskPrecheck and taskLinkedTo,
// spawn_owner.go resolveOwner, placement.go taskWaited, select.go
// resolveUrgency), copied verbatim but for their names. The parity test below
// feeds them the JSON the fake hk serves.
type cliTask struct {
	ID        any            `json:"id"`
	Title     string         `json:"title"`
	State     string         `json:"state"`
	ClaimedBy string         `json:"claimed_by"`
	Refs      map[string]any `json:"refs"`
	Project   string         `json:"project"`
	CreatedAt string         `json:"created_at"`
	Priority  any            `json:"priority"`
}

func cliPrecheck(out string, s Spawn) (cliTask, bool, error) {
	var t cliTask
	if e := json.Unmarshal([]byte(out), &t); e != nil || t.State == "" {
		return t, false, fail(exitConfig, "unreadable task precheck")
	}
	if t.ID != nil && scalar(t.ID) != s.Task {
		return t, false, fail(exitConfig, "task response ID mismatch")
	}
	if s.Inherited != "" || s.unlinkedReviewer() {
		return t, false, nil
	}
	if slices.Contains([]string{"merged", "dropped", "hold", "needs_decision"}, t.State) {
		return t, false, fail(exitUsage, "task unavailable: %s", t.State)
	}
	if j := str(t.Refs, "job_id"); j != "" && j != s.Job {
		return t, false, fail(exitUsage, "task bound to another job")
	}
	if str(t.Refs, "job_id") == "" && t.ClaimedBy != "" && t.ClaimedBy != s.Label {
		return t, false, fail(exitUsage, "task has another claimant without a job binding")
	}
	return t, false, nil
}

func cliLinkedTo(out, task, job string) bool {
	var t cliTask
	if json.Unmarshal([]byte(out), &t) != nil {
		return false
	}
	return job != "" && (t.ID == nil || scalar(t.ID) == task) && str(t.Refs, "job_id") == job
}

func cliWaited(t cliTask, now time.Time) time.Duration {
	if t.CreatedAt == "" {
		return 0
	}
	ts, e := time.Parse(time.RFC3339Nano, t.CreatedAt)
	if e != nil {
		return 0
	}
	if d := now.Sub(ts); d > 0 {
		return d
	}
	return 0
}

func cliUrgency(t cliTask) string {
	priority := func(v any) (int, bool) {
		switch n := v.(type) {
		case float64:
			return int(n), n == float64(int(n))
		case string:
			i, e := strconv.Atoi(strings.TrimSpace(n))
			return i, e == nil
		}
		return 0, false
	}
	if n, ok := priority(t.Priority); ok && slices.Contains(urgentPriorities, n) {
		return urgencyUrgent
	}
	return urgencyNormal
}

func cliOwner(showOut, ownerOut, task string) (string, string, error) {
	var t cliTask
	if json.Unmarshal([]byte(showOut), &t) != nil || (t.ID != nil && scalar(t.ID) != task) {
		return "", "", fail(exitUsage, "unreadable task %s from hk to resolve the owner; pass --owner LANE explicitly", task)
	}
	if t.Project == "" {
		return "", "", fail(exitUsage, "task %s names no project, so hk cannot resolve an owner; pass --owner LANE explicitly", task)
	}
	var owner struct {
		DesignLane     string `json:"design_lane"`
		EffectiveOwner string `json:"effective_owner"`
	}
	if json.Unmarshal([]byte(ownerOut), &owner) != nil || strings.TrimSpace(owner.EffectiveOwner) == "" {
		return "", t.Project, fail(exitUsage, "hk returned no effective owner for project %s; pass --owner LANE explicitly", t.Project)
	}
	return owner.EffectiveOwner, t.Project, nil
}

// hkErrOf is an error's text, "" for nil.
func hkErrOf(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}

// The values the spawn path sees over HTTP are the ones the CLI path gave:
// the same hk JSON, read by the copied CLI-output parsers and by the HTTP
// path, gives the same precheck verdict (exit class and text), title, state,
// claimant, wait, urgency, link check and owner (hk 3591 Desired 3).
func TestSpawnPathHKCLIHTTPParity(t *testing.T) {
	ctx := context.Background()
	tasks := []struct{ name, body string }{
		{"ready unbound", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`)},
		{"bound to this job", hkTaskJSON(42, "developing", "implement (grade A, T2)", "worker-a", "panewire", `{"job_id":"job-a"}`)},
		{"bound to another job", hkTaskJSON(42, "developing", "implement (grade B, T1)", "worker-b", "panewire", `{"job_id":"job-b"}`)},
		{"other claimant no job", hkTaskJSON(42, "developing", "implement (grade B, T1)", "worker-b", "panewire", `{}`)},
		{"merged", hkTaskJSON(42, "merged", "done (grade B, T1)", "worker-a", "panewire", `{"job_id":"job-a","pr":"https://github.com/example/p/pull/7","closure":{"type":"merged","evidence":"pr 7"}}`)},
		{"needs decision", hkTaskJSON(42, "needs_decision", "decide (grade S, T3)", "", "panewire",
			`{"decision_request":{"id":"dr-42-1","revision":1,"status":"open","question":"Approve?","requested_by":"svc-mergewake","requested_at":"2025-12-31T00:00:00Z"}}`)},
		{"hold", hkTaskJSON(42, "hold", "held (grade B, T1)", "", "", `{"report_path":"/tmp/r.md","head_sha":"abc"}`)},
		{"urgent fractional created", `{"id":42,"title":"urgent (grade B, T1)","state":"ready","priority":1,"refs":{},"created_at":"2025-12-30T12:34:56.789Z","project":"panewire"}`},
		{"minimal", `{"id":42,"title":"t","state":"ready","refs":{}}`},
		{"no id", `{"title":"t","state":"ready","refs":{"job_id":"job-a"},"claimed_by":"worker-a"}`},
		{"id mismatch", hkTaskJSON(43, "ready", "implement (grade B, T1)", "", "panewire", `{}`)},
		{"no state", `{"id":42,"title":"t","refs":{}}`},
		// What the CLI parser read and hk's typed client does not: a 2xx body
		// that is not JSON, an explicit id 0 or null, a string id, a string
		// priority, a non-string job_id and a timestamp that does not parse.
		{"malformed json", `{"id":42,"title":"t","state":"ready","refs":{`},
		{"truncated empty", ``},
		{"explicit id zero", `{"id":0,"title":"t","state":"ready","refs":{}}`},
		{"null id", `{"id":null,"title":"t","state":"ready","refs":{"job_id":"job-a"},"claimed_by":"worker-a"}`},
		{"string id", `{"id":"42","title":"urgent (grade B, T1)","state":"ready","priority":"1","refs":{"job_id":"job-a"},"claimed_by":"worker-a","created_at":"2025-12-31T00:00:00Z","project":"panewire"}`},
		{"string id mismatch", `{"id":"042","title":"t","state":"ready","refs":{}}`},
		{"invalid created_at hold", `{"id":42,"title":"held (grade B, T1)","state":"hold","priority":2,"refs":{},"created_at":"yesterday","project":"panewire"}`},
		{"invalid created_at ready", `{"id":42,"title":"t","state":"ready","priority":1,"refs":{"job_id":"job-a"},"claimed_by":"worker-a","created_at":"yesterday","updated_at":"later"}`},
		{"non-string job_id", `{"id":42,"title":"t","state":"developing","refs":{"job_id":7},"claimed_by":"worker-b"}`},
	}
	spawns := []struct {
		name string
		s    Spawn
	}{
		{"fresh", Spawn{Task: "42", Job: "job-a", Label: "worker-a", Role: "worker"}},
		{"inherited", Spawn{Task: "42", Job: "job-c", Label: "worker-c", Role: "worker", Inherited: "job-b"}},
		// --bypass covers an hk the precheck cannot reach, never an answer
		// it cannot read: the CLI path reached bypass on a nonzero exit only.
		{"fresh bypass", Spawn{Task: "42", Job: "job-a", Label: "worker-a", Role: "worker", Bypass: true}},
		{"inherited bypass", Spawn{Task: "42", Job: "job-c", Label: "worker-c", Role: "worker", Inherited: "job-b", Bypass: true}},
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range tasks {
		for _, sp := range spawns {
			t.Run(tc.name+"/"+sp.name, func(t *testing.T) {
				a, hk := hkSiteApp(t)
				hk.SetTask("42", tc.body)
				got, gotBypassed, ge := a.taskPrecheck(ctx, sp.s)
				want, wantBypassed, we := cliPrecheck(tc.body, sp.s)
				if exitCode(ge) != exitCode(we) || hkErrOf(ge) != hkErrOf(we) || gotBypassed != wantBypassed {
					t.Fatalf("precheck: http %d %q bypassed=%t, cli %d %q bypassed=%t", exitCode(ge), hkErrOf(ge), gotBypassed, exitCode(we), hkErrOf(we), wantBypassed)
				}
				for _, job := range []string{"job-a", "job-b", ""} {
					if g, w := a.taskLinkedTo(ctx, "42", job), cliLinkedTo(tc.body, "42", job); g != w {
						t.Errorf("linked to %q: http %t, cli %t", job, g, w)
					}
				}
				if ge != nil {
					return
				}
				if got.Title != want.Title || got.State != want.State || got.ClaimedBy != want.ClaimedBy {
					t.Errorf("title/state/claimed_by: http %q %q %q, cli %q %q %q", got.Title, got.State, got.ClaimedBy, want.Title, want.State, want.ClaimedBy)
				}
				if g, w := taskWaited(got, now), cliWaited(want, now); g != w {
					t.Errorf("waited: http %v, cli %v", g, w)
				}
				if g, _ := resolveUrgency("", got); g != cliUrgency(want) {
					t.Errorf("urgency: http %q, cli %q", g, cliUrgency(want))
				}
			})
		}
	}

	owners := []struct{ name, task, owner string }{
		{"design lane", hkTaskJSON(77, "ready", "t", "", "panewire", `{}`), `{"name":"panewire","design_lane":"panewire-design","effective_owner":"panewire-design","updated_at":"2025-12-31T00:00:00Z","events":[{"lane":"panewire-design"}]}`},
		{"operator desk fallback", hkTaskJSON(77, "ready", "t", "", "other", `{}`), `{"name":"other","design_lane":"","effective_owner":"operator-desk","events":[]}`},
		{"no effective owner", hkTaskJSON(77, "ready", "t", "", "blank", `{}`), `{"name":"blank","design_lane":"","effective_owner":"  "}`},
		{"no project", hkTaskJSON(77, "ready", "t", "", "", `{}`), `{}`},
		{"id mismatch", hkTaskJSON(78, "ready", "t", "", "panewire", `{}`), `{}`},
		{"malformed task json", `{"id":77,"project":"panewire"`, `{"effective_owner":"panewire-design"}`},
		{"malformed owner json", hkTaskJSON(77, "ready", "t", "", "panewire", `{}`), `{"name":"panewire","effective_owner":"panewire-design"`},
		{"string id", `{"id":"77","project":"panewire"}`, `{"name":"panewire","design_lane":"panewire-design","effective_owner":"panewire-design"}`},
		{"explicit id zero", `{"id":0,"project":"panewire"}`, `{"name":"panewire","effective_owner":"panewire-design"}`},
		{"invalid created_at", `{"id":77,"state":"hold","project":"panewire","created_at":"yesterday"}`, `{"name":"panewire","effective_owner":"panewire-design"}`},
		{"invalid owner updated_at", hkTaskJSON(77, "ready", "t", "", "panewire", `{}`), `{"name":"panewire","design_lane":"panewire-design","effective_owner":"panewire-design","updated_at":"later"}`},
	}
	for _, tc := range owners {
		for _, bypass := range []bool{false, true} {
			t.Run("owner/"+tc.name+"/bypass="+strconv.FormatBool(bypass), func(t *testing.T) {
				a, hk := hkSiteApp(t)
				hk.SetTask("77", tc.task)
				for _, project := range []string{"panewire", "other", "blank", "mergewake"} {
					hk.SetOwner(project, tc.owner)
				}
				s := Spawn{Task: "77", Bypass: bypass}
				ge := a.resolveOwner(ctx, &s)
				wantOwner, wantProject, we := cliOwner(tc.task, tc.owner, "77")
				if exitCode(ge) != exitCode(we) || hkErrOf(ge) != hkErrOf(we) || s.Owner != wantOwner {
					t.Fatalf("owner: http %q %d %q, cli %q %d %q", s.Owner, exitCode(ge), hkErrOf(ge), wantOwner, exitCode(we), hkErrOf(we))
				}
				if we == nil {
					line := "owner=" + wantOwner + " source=hk project=" + wantProject + "\n"
					if out := a.Out.(*bytes.Buffer).String(); out != line {
						t.Fatalf("owner line %q want %q", out, line)
					}
				}
			})
		}
	}
}

// The hk refusals the spawn path reports keep their exit class and text: a
// missing task is exitUsage "task does not exist", an hk outage is exitConfig
// with the CLI's "handoffkeep: <error>" detail (bypassed under Bypass), a
// claim refusal is "task claim exited with status 1", a rebind fence names its
// hk code, and an unknown project reads "no such project".
func TestSpawnPathHKRefusalsKeepTheirClassAndText(t *testing.T) {
	ctx := context.Background()
	s := Spawn{Task: "42", Job: "job-a", Label: "worker-a"}

	t.Run("task not found", func(t *testing.T) {
		a, _ := hkSiteApp(t)
		if _, _, e := a.taskPrecheck(ctx, s); exitCode(e) != exitUsage || hkErrOf(e) != "task does not exist" {
			t.Fatalf("precheck=%d %v", exitCode(e), e)
		}
		o := Spawn{Task: "42"}
		if e := a.resolveOwner(ctx, &o); exitCode(e) != exitUsage || hkErrOf(e) != "cannot read task 42 from hk to resolve the owner (handoffkeep: not_found); pass --owner LANE explicitly" {
			t.Fatalf("owner=%v", e)
		}
	})
	t.Run("hk 500", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.Handle(hkFails("GET /v1/tasks/42", http.StatusInternalServerError, `{"error":"internal"}`))
		if _, _, e := a.taskPrecheck(ctx, s); exitCode(e) != exitConfig || hkErrOf(e) != "task precheck unavailable: handoffkeep: internal" {
			t.Fatalf("precheck=%d %v", exitCode(e), e)
		}
		bypass := s
		bypass.Bypass = true
		if _, bypassed, e := a.taskPrecheck(ctx, bypass); e != nil || !bypassed {
			t.Fatalf("bypassed precheck=%t %v", bypassed, e)
		}
	})
	t.Run("claim refused under bypass", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.Handle(hkFails("POST /v1/tasks/42/claim", http.StatusConflict, `{"error":"conflict"}`))
		bypass := s
		bypass.Bypass = true
		hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`))
		task, _, e := a.taskPrecheck(ctx, s)
		if e != nil {
			t.Fatal(e)
		}
		if e := a.taskLink(ctx, bypass, task, true); hkErrOf(e) != "task claim exited with status 1" {
			t.Fatalf("taskLink=%v", e)
		}
	})
	t.Run("claim outage under bypass", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.Handle(hkFails("POST /v1/tasks/42/claim", http.StatusServiceUnavailable, `{"error":"unavailable"}`))
		bypass := s
		bypass.Bypass = true
		hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`))
		task, _, e := a.taskPrecheck(ctx, s)
		if e != nil {
			t.Fatal(e)
		}
		if e := a.taskLink(ctx, bypass, task, true); e != nil {
			t.Fatalf("taskLink=%v", e)
		}
		events, e := a.ledger().Events(ctx, s.Job)
		if e != nil || len(events) != 1 || events[0].Payload["status"] != "bypassed" {
			t.Fatalf("events=%v %v", events, e)
		}
	})
	t.Run("claim already linked", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`))
		task, _, e := a.taskPrecheck(ctx, s)
		if e != nil {
			t.Fatal(e)
		}
		hk.SetTask("42", hkTaskJSON(42, "developing", "implement (grade B, T1)", "design", "panewire", `{"job_id":"job-a"}`)).
			Handle(hkFails("POST /v1/tasks/42/claim", http.StatusConflict, `{"error":"conflict"}`))
		if e := a.taskLink(ctx, s, task, false); e != nil {
			t.Fatalf("taskLink=%v", e)
		}
		events, e := a.ledger().Events(ctx, s.Job)
		if e != nil || len(events) != 1 || events[0].Payload["status"] != "already_linked" {
			t.Fatalf("events=%v %v", events, e)
		}
	})
	t.Run("rebind fence", func(t *testing.T) {
		fx := newRebindFixture(t, "lost", rebindParent)
		hk := useFakeHK(t)
		failHKCLI(t, fx.f)
		hk.SetTask("42", hkTaskJSON(42, "developing", "implement (grade B, T1)", rebindParent, "panewire", `{"job_id":"`+rebindParent+`"}`)).
			Handle(hkFails("POST /v1/tasks/42/rebind", http.StatusConflict, `{"error":"wrk_receipt_required","reason":"the parent has no wrk receipt"}`))
		if e := fx.a.Spawn(ctx, fx.s); e != nil {
			t.Fatalf("a refused rebind failed the landed spawn: %v", e)
		}
		ev := fx.taskRebindEvent(t)
		if ev["status"] != "inherited" || ev["rebind_error"] != "task rebind refused: handoffkeep: wrk_receipt_required: the parent has no wrk receipt" {
			t.Fatalf("event=%v", ev)
		}
	})
	t.Run("unknown project", func(t *testing.T) {
		a, hk := hkSiteApp(t)
		hk.SetTask("77", hkTaskJSON(77, "ready", "t", "", "nope", `{}`))
		o := Spawn{Task: "77"}
		if e := a.resolveOwner(ctx, &o); exitCode(e) != exitUsage || hkErrOf(e) != "cannot resolve the hk owner of project nope (handoffkeep: no such project: nope); pass --owner LANE explicitly" {
			t.Fatalf("owner=%v", e)
		}
	})
}

// cliClaim is the claim branch of panewire@8422639 taskLink, copied but for
// its name and its inputs: the CLI's exit code, stdout and stderr, and
// whether hk already bound the task to the job. It returns the
// job.task_link status and the failure reason.
func cliClaim(code int, out, errText string, s Spawn, bypassed, linked bool) (string, string) {
	if code != 0 {
		if !bypassed || !s.Bypass || strings.Contains(out+errText, "conflict") || strings.Contains(out+errText, "refused") {
			if linked {
				return "already_linked", ""
			}
			return "failed", fmt.Sprintf("task claim exited with status %d", code)
		}
		return "bypassed", ""
	}
	var t cliTask
	if e := json.Unmarshal([]byte(out), &t); e != nil || t.ClaimedBy != s.Label || str(t.Refs, "job_id") != s.Job {
		return "failed", "task claim did not confirm claimed_by and refs.job_id"
	}
	return "linked", ""
}

// The claim reaches the job.task_link status and reason the CLI path
// recorded, with and without --bypass: a 2xx answer the CLI parser cannot
// read is "did not confirm" and never bypassed; one it can read (a string
// id, a bad timestamp) links; a conflict, a refusal and the invalid_context
// code the CLI reworded as "claim_job_id_rejected: server refused the claim"
// are never bypassed; an outage and a client-side non-confirmation are.
func TestSpawnPathHKClaimCLIHTTPParity(t *testing.T) {
	ctx := context.Background()
	const confirming = `{"id":42,"state":"developing","claimed_by":"worker-a","refs":{"job_id":"job-a"}}`
	cases := []struct {
		name           string
		status         int
		body           string
		cliCode        int
		cliOut, cliErr string
	}{
		{name: "linked", status: http.StatusOK, body: confirming, cliOut: confirming},
		{name: "malformed 2xx", status: http.StatusOK, body: `{"id":42,"state":"developing","claimed_by":`, cliOut: `{"id":42,"state":"developing","claimed_by":`},
		{name: "empty 2xx", status: http.StatusOK, body: ``, cliOut: ``},
		{name: "string id bad timestamp", status: http.StatusOK,
			body:   `{"id":"42","state":"developing","claimed_by":"worker-a","refs":{"job_id":"job-a"},"created_at":"yesterday"}`,
			cliOut: `{"id":"42","state":"developing","claimed_by":"worker-a","refs":{"job_id":"job-a"},"created_at":"yesterday"}`},
		{name: "conflict", status: http.StatusConflict, body: `{"error":"conflict"}`, cliCode: 1, cliErr: "handoffkeep: conflict"},
		{name: "outage", status: http.StatusServiceUnavailable, body: `{"error":"unavailable"}`, cliCode: 1, cliErr: "handoffkeep: unavailable"},
		{name: "invalid_context", status: http.StatusBadRequest, body: `{"error":"invalid_context"}`, cliCode: 1, cliErr: "claim_job_id_rejected: server refused the claim"},
		{name: "claimant not recorded", status: http.StatusOK, body: `{"id":42,"state":"developing","claimed_by":"worker-b","refs":{"job_id":"job-a"}}`,
			cliCode: 1, cliErr: `handoffkeep: claim_claimant_not_recorded: server response has claimed_by="worker-b", want "worker-a"`},
	}
	for _, tc := range cases {
		for _, bypass := range []bool{false, true} {
			t.Run(tc.name+"/bypass="+strconv.FormatBool(bypass), func(t *testing.T) {
				a, hk := hkSiteApp(t)
				hk.SetTask("42", hkTaskJSON(42, "ready", "implement (grade B, T1)", "", "panewire", `{}`)).
					Handle(func(r hkRequest) (int, string, bool) {
						if r.Route() != "POST /v1/tasks/42/claim" {
							return 0, "", false
						}
						return tc.status, tc.body, true
					})
				s := Spawn{Task: "42", Job: "job-a", Label: "worker-a", Bypass: bypass}
				ge := a.taskLink(ctx, s, client.Task{}, bypass)
				events, e := a.ledger().Events(ctx, s.Job)
				if e != nil || len(events) != 1 {
					t.Fatalf("events=%v %v", events, e)
				}
				gotStatus, _ := events[0].Payload["status"].(string)
				gotReason, _ := events[0].Payload["reason"].(string)
				wantStatus, wantReason := cliClaim(tc.cliCode, tc.cliOut, tc.cliErr, s, bypass, false)
				if gotStatus != wantStatus || gotReason != wantReason || hkErrOf(ge) != wantReason {
					t.Fatalf("claim: http status=%q reason=%q err=%v, cli status=%q reason=%q", gotStatus, gotReason, ge, wantStatus, wantReason)
				}
			})
		}
	}
}

// A 2xx answer the CLI parser could not read keeps the CLI path's exit class
// and text and is never bypassed: the precheck is exitConfig "unreadable task
// precheck" under --bypass too, and a rebind records that it did not confirm
// the binding.
func TestSpawnPathHKUnreadableAnswerIsNotBypassed(t *testing.T) {
	ctx := context.Background()
	for _, body := range []string{`{"id":42,"state":"ready"`, `not json`, ``, `{"id":42,"state":"ready","created_at":7}`} {
		for _, bypass := range []bool{false, true} {
			t.Run(fmt.Sprintf("precheck %q bypass=%t", body, bypass), func(t *testing.T) {
				a, hk := hkSiteApp(t)
				hk.SetTask("42", body)
				_, bypassed, e := a.taskPrecheck(ctx, Spawn{Task: "42", Job: "job-a", Label: "worker-a", Bypass: bypass})
				if bypassed || exitCode(e) != exitConfig || hkErrOf(e) != "unreadable task precheck" {
					t.Fatalf("precheck bypassed=%t %d %v, want exitConfig \"unreadable task precheck\"", bypassed, exitCode(e), e)
				}
			})
		}
	}
	t.Run("rebind", func(t *testing.T) {
		fx := newRebindFixture(t, "completed", rebindParent)
		failHKCLI(t, fx.f)
		fx.rebindAnswer = `{"id":42,"state":"claimed","claimed_by":`
		if e := fx.a.Spawn(ctx, fx.s); e != nil {
			t.Fatalf("an unreadable rebind answer failed the landed spawn: %v", e)
		}
		ev := fx.taskRebindEvent(t)
		if ev["status"] != "inherited" || ev["rebind_error"] != "unconfirmed" {
			t.Fatalf("event=%v", ev)
		}
	})
	t.Run("rebind readable with a string id", func(t *testing.T) {
		fx := newRebindFixture(t, "completed", rebindParent)
		failHKCLI(t, fx.f)
		fx.rebindAnswer = `{"id":"42","state":"claimed","claimed_by":"` + fx.s.Label + `","refs":{"job_id":"` + fx.s.Job + `"},"updated_at":"later"}`
		if e := fx.a.Spawn(ctx, fx.s); e != nil {
			t.Fatal(e)
		}
		if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})
}
