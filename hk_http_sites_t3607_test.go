package panewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/hkcred"
)

// hk 3607: `panewire job done|escalate|joined`'s report upload and the quota
// stop manager's policy read, task lookup and note write go over hk HTTP with
// the svc-panewire credential instead of the `handoffkeep` CLI.

const hkSitesSvcToken = "svc-panewire-sites-token"

type hkSitesRequest struct {
	Method, Path, Query, Auth string
	Body                      []byte
}

func (r hkSitesRequest) Route() string { return r.Method + " " + r.Path }

// hkSitesFake is an hk HTTP server reached through the svc-panewire
// credential in the environment; answer gives each request's status and body.
type hkSitesFake struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	reqs   []hkSitesRequest
	answer func(hkSitesRequest) (int, string)
}

func newHKSitesFake(t *testing.T, answer func(hkSitesRequest) (int, string)) *hkSitesFake {
	t.Helper()
	hk := &hkSitesFake{t: t, answer: answer}
	hk.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := hkSitesRequest{Method: r.Method, Path: r.URL.EscapedPath(), Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization"), Body: body}
		hk.mu.Lock()
		hk.reqs = append(hk.reqs, req)
		hk.mu.Unlock()
		if req.Auth != "Bearer "+hkSitesSvcToken {
			t.Errorf("%s: Authorization %q, want the svc-panewire bearer", req.Route(), req.Auth)
		}
		status, out := hk.answer(req)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, out)
	}))
	t.Cleanup(hk.srv.Close)
	t.Setenv(hkcred.URLKey, hk.srv.URL)
	t.Setenv(hkcred.TokenKey, hkSitesSvcToken)
	t.Setenv("HANDOFFKEEP_TOKEN", "hk-cli-config-token")
	return hk
}

func (hk *hkSitesFake) requests() []hkSitesRequest {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return append([]hkSitesRequest(nil), hk.reqs...)
}

func (hk *hkSitesFake) routes() []string {
	var out []string
	for _, r := range hk.requests() {
		out = append(out, r.Route())
	}
	return out
}

// noHKBinary puts a `handoffkeep` on PATH and in HANDOFFKEEP_BIN that leaves
// a mark and fails; ran reports whether anything ran it.
func noHKBinary(t *testing.T) (ran func() bool) {
	t.Helper()
	dir := t.TempDir()
	mark := filepath.Join(dir, "ran")
	bin := filepath.Join(dir, "handoffkeep")
	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$*\" >>%q\necho 'handoffkeep: hk CLI must not run' >&2\nexit 1\n", mark)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HANDOFFKEEP_BIN", bin)
	return func() bool {
		b, err := os.ReadFile(mark)
		if err == nil {
			t.Logf("handoffkeep ran: %s", b)
		}
		return err == nil
	}
}

func unconfiguredHK(t *testing.T) {
	t.Helper()
	t.Setenv(hkcred.URLKey, "")
	t.Setenv(hkcred.TokenKey, "")
	t.Setenv(hkcred.PathEnv, filepath.Join(t.TempDir(), "absent-hk-svc.env"))
}

func hkSitesJSON(t *testing.T, r hkSitesRequest) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(r.Body, &out); err != nil {
		t.Fatalf("%s: body %q: %v", r.Route(), r.Body, err)
	}
	return out
}

// The job command's report upload: PUT /v1/documents/{key} with the key, kind
// report, the job and the report file's bytes the CLI's --file carried, and
// the same suffix and warnings.
func TestJobReportUploadGoesOverHTTP(t *testing.T) {
	report := filepath.Join(t.TempDir(), "report.md")
	content := "first\n" + strings.Repeat("결과 ", 1<<17) + "\nlast line\n"
	if err := os.WriteFile(report, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	key := "reports/jw/report.md"

	t.Run("stored", func(t *testing.T) {
		ran := noHKBinary(t)
		hk := newHKSitesFake(t, func(r hkSitesRequest) (int, string) {
			return http.StatusOK, `{"document":{"id":1,"key":"` + key + `","kind":"report","job":"jw","sha256":"ab","created_by":"svc-panewire","created_at":"2026-11-08T08:00:00Z","updated_at":"2026-11-08T08:00:00Z"},"changed":true}`
		})
		var stderr bytes.Buffer
		c := &jobCLI{stdout: io.Discard, stderr: &stderr, now: time.Now}
		if got := c.uploadReportDocument("jw", report); got != " doc:"+key {
			t.Fatalf("suffix=%q stderr=%q", got, stderr.String())
		}
		if ran() || stderr.Len() != 0 {
			t.Fatalf("ran the hk CLI=%t stderr=%q", ran(), stderr.String())
		}
		reqs := hk.requests()
		if len(reqs) != 1 || reqs[0].Route() != "PUT /v1/documents/"+url.PathEscape(key) {
			t.Fatalf("hk routes=%q", hk.routes())
		}
		put := hkSitesJSON(t, reqs[0])
		if put["key"] != key || put["kind"] != "report" || put["job"] != "jw" || put["body"] != content {
			t.Fatalf("put key=%v kind=%v job=%v body=%d bytes, want the whole report (%d bytes)", put["key"], put["kind"], put["job"], len(fmt.Sprint(put["body"])), len(content))
		}
	})

	t.Run("refused", func(t *testing.T) {
		ran := noHKBinary(t)
		newHKSitesFake(t, func(hkSitesRequest) (int, string) {
			return http.StatusRequestEntityTooLarge, `{"error":"too_large"}`
		})
		var stderr bytes.Buffer
		c := &jobCLI{stdout: io.Discard, stderr: &stderr, now: time.Now}
		if got := c.uploadReportDocument("jw", report); got != "" {
			t.Fatalf("suffix=%q", got)
		}
		if ran() || stderr.String() != "panewire job: warning: handoffkeep document upload failed (rc=1 job=jw); continuing without document link\n" {
			t.Fatalf("ran the hk CLI=%t stderr=%q", ran(), stderr.String())
		}
	})

	t.Run("unconfigured", func(t *testing.T) {
		ran := noHKBinary(t)
		unconfiguredHK(t)
		var stderr bytes.Buffer
		c := &jobCLI{stdout: io.Discard, stderr: &stderr, now: time.Now}
		if got := c.uploadReportDocument("jw", report); got != "" {
			t.Fatalf("suffix=%q", got)
		}
		if ran() || !strings.Contains(stderr.String(), "hk service credential unconfigured") || !strings.Contains(stderr.String(), "report document not uploaded (job=jw)") {
			t.Fatalf("ran the hk CLI=%t stderr=%q, want the unconfigured credential named", ran(), stderr.String())
		}
	})
}

// quotaStopHKFixture answers the quota stop manager's policy read and task
// lookup with fixed hk answers and counts its note writes in notes.
func quotaStopHKFixture(policyAnswer, taskAnswer string, notes *int) quotaStopHK {
	return quotaStopHK{
		policy: func(context.Context) ([]byte, error) { return []byte(policyAnswer), nil },
		task: func(context.Context, string) ([]byte, error) {
			var answer struct {
				Tasks []json.RawMessage `json:"tasks"`
			}
			if json.Unmarshal([]byte(taskAnswer), &answer) != nil || len(answer.Tasks) == 0 {
				return nil, os.ErrNotExist
			}
			return answer.Tasks[0], nil
		},
		tasks: func(context.Context, int64) ([]byte, error) { return []byte(taskAnswer), nil },
		note: func(context.Context, string, string, string) error {
			if notes != nil {
				*notes++
			}
			return nil
		},
	}
}

// quotaStopHKDown is an hk that cannot be reached; calls counts every call.
func quotaStopHKDown(calls *int) quotaStopHK {
	down := func() error {
		if calls != nil {
			*calls++
		}
		return os.ErrNotExist
	}
	return quotaStopHK{
		policy: func(context.Context) ([]byte, error) { return nil, down() },
		task:   func(context.Context, string) ([]byte, error) { return nil, down() },
		tasks:  func(context.Context, int64) ([]byte, error) { return nil, down() },
		note:   func(context.Context, string, string, string) error { return down() },
	}
}

// quotaPolicyAnswer is hk's answer for a policy document whose body is body.
func quotaPolicyAnswer(body string) string {
	b, _ := json.Marshal(map[string]string{"body": body})
	return string(b)
}

const quotaSitesPolicy = "## Values\n- default_expected: 1d\n- urgent_priorities: [1, 2]\n- max_waits: 1\n- resume_text: \"continue\"\n"

func quotaSitesManager(t *testing.T) *quotaStopManager {
	t.Helper()
	store := NewMemoryStore(t)
	t.Cleanup(func() { _ = store.Close() })
	q, err := newQuotaStopManager(store, t.TempDir(), false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// The quota stop manager's three hk calls: the policy read, the task lookup
// (GET /v1/tasks with limit 1000 and stable ID paging) and the note write.
func TestQuotaStopHKCallsGoOverHTTP(t *testing.T) {
	ran := noHKBinary(t)
	policyDoc, _ := json.Marshal(map[string]any{"id": 3, "key": "policy/quota-stop", "kind": "policy", "body": quotaSitesPolicy, "sha256": "ab", "created_by": "operator", "created_at": "2026-11-08T08:00:00Z", "updated_at": "2026-11-08T08:00:00Z"})
	hk := newHKSitesFake(t, func(r hkSitesRequest) (int, string) {
		switch {
		case r.Route() == "GET /v1/documents/policy%2Fquota-stop":
			return http.StatusOK, string(policyDoc)
		case r.Route() == "GET /v1/tasks":
			return http.StatusOK, `{"tasks":[{"id":7,"lane":"workers","title":"t","state":"developing","priority":2,"expected":"4h","refs":{"job_id":"job-active"},"created_at":"2026-11-08T08:00:00Z","updated_at":"2026-11-08T08:00:00Z","project":null},{"id":8,"priority":1,"expected":null,"refs":{"job_id":"job-other"}}]}`
		case r.Method == http.MethodPut && strings.HasPrefix(r.Path, "/v1/documents/quota-stop%2F"):
			return http.StatusOK, `{"document":{"id":4,"key":"k","kind":"note","sha256":"ab","created_by":"svc-panewire","created_at":"2026-11-08T08:00:00Z","updated_at":"2026-11-08T08:00:00Z"},"changed":true}`
		}
		return http.StatusNotFound, `{"error":"not_found"}`
	})
	q := quotaSitesManager(t)
	ctx := context.Background()
	if p, ok := q.loadPolicy(ctx, time.Now()); !ok || p.ResumeText != "continue" {
		t.Fatalf("policy=%+v ok=%t", p, ok)
	}
	if priority, expected, ok := q.task(ctx, HubActiveJob{JobID: "job-active"}, "", ""); !ok || priority != 2 || expected != 4*time.Hour {
		t.Fatalf("task priority=%d expected=%v ok=%t", priority, expected, ok)
	}
	now := time.Now().UTC()
	if _, err := q.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,created) VALUES(?,?,?,?,?,?,?,?,?)`, "wake-1", "job-active", "w:p1", "worker", "codex", "12:00", "wait", "reset_known", now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	q.tick(ctx, now, false)
	var recorded int
	if err := q.store.db.QueryRow(`SELECT hk_recorded FROM quota_stop_decisions WHERE event_id='wake-1'`).Scan(&recorded); err != nil || recorded != 1 {
		t.Fatalf("hk_recorded=%d err=%v", recorded, err)
	}
	if ran() {
		t.Fatal("the quota stop manager ran the hk CLI")
	}
	reqs := hk.requests()
	sum := sha256.Sum256([]byte("wake-1"))
	noteKey := "quota-stop/" + hex.EncodeToString(sum[:])
	want := []string{"GET /v1/documents/policy%2Fquota-stop", "GET /v1/tasks", "PUT /v1/documents/" + url.PathEscape(noteKey)}
	if got := hk.routes(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("hk routes=%q want %q", got, want)
	}
	if q, _ := url.ParseQuery(reqs[1].Query); q.Get("limit") != "1000" || q.Get("after_id") != "0" || q.Get("lane") != "" || q.Get("state") != "" || q.Get("parent_lane") != "" || !q.Has("lane") {
		t.Fatalf("task list query=%q, want the first stable ID page at after_id=0 with limit 1000", reqs[1].Query)
	}
	put := hkSitesJSON(t, reqs[2])
	body := "[quota-stop] job-active w:p1 codex reset=12:00 decision=wait reason=reset_known\n"
	if put["key"] != noteKey || put["kind"] != "note" || put["job"] != "job-active" || put["body"] != body {
		t.Fatalf("note put=%v, want key %s kind note job job-active body %q", put, noteKey, body)
	}
}

// A refused note write backs off a minute, as a failed `doc put` did; with no
// credential nothing is sent and nothing runs.
func TestQuotaStopHKFailuresKeepCLIHandling(t *testing.T) {
	for _, unconfigured := range []bool{false, true} {
		t.Run(map[bool]string{false: "refused", true: "unconfigured"}[unconfigured], func(t *testing.T) {
			ran := noHKBinary(t)
			var hk *hkSitesFake
			if unconfigured {
				unconfiguredHK(t)
			} else {
				hk = newHKSitesFake(t, func(hkSitesRequest) (int, string) { return http.StatusInternalServerError, `{"error":"internal"}` })
			}
			q := quotaSitesManager(t)
			ctx := context.Background()
			if _, ok := q.loadPolicy(ctx, time.Now()); ok {
				t.Fatal("policy read without hk")
			}
			if _, _, ok := q.task(ctx, HubActiveJob{JobID: "job-active"}, "", ""); ok {
				t.Fatal("task found without hk")
			}
			now := time.Now().UTC()
			if _, err := q.store.db.Exec(`INSERT INTO quota_stop_decisions(event_id,job,pane,label,pool,reset,decision,reason,created) VALUES(?,?,?,?,?,?,?,?,?)`, "wake-1", "job-active", "w:p1", "worker", "codex", "12:00", "wait", "reset_known", now.UnixMilli()); err != nil {
				t.Fatal(err)
			}
			q.tick(ctx, now, false)
			var recorded int
			var retry int64
			if err := q.store.db.QueryRow(`SELECT hk_recorded, hk_retry FROM quota_stop_decisions WHERE event_id='wake-1'`).Scan(&recorded, &retry); err != nil || recorded != 0 || retry != now.Add(time.Minute).UnixMilli() {
				t.Fatalf("hk_recorded=%d hk_retry=%d err=%v, want a one-minute backoff", recorded, retry, err)
			}
			if ran() {
				t.Fatal("ran the hk CLI")
			}
			if hk != nil && len(hk.requests()) != 3 {
				t.Fatalf("hk routes=%q", hk.routes())
			}
		})
	}
}

// cliQuotaTask is the quota stop manager's read of `handoffkeep tasks list
// --limit 1000` at ab2e296, copied: the CLI printed hk's GET /v1/tasks answer.
func cliQuotaTask(code int, out string, job, inherited string) (int, time.Duration, bool) {
	if code != 0 {
		return 0, 0, false
	}
	var v struct {
		Tasks []struct {
			Priority any     `json:"priority"`
			Expected *string `json:"expected"`
			Refs     struct {
				Job string `json:"job_id"`
			} `json:"refs"`
		} `json:"tasks"`
	}
	if json.Unmarshal([]byte(out), &v) != nil || len(v.Tasks) >= 1000 {
		return 0, 0, false
	}
	n, priority := 0, 0
	var duration time.Duration
	for _, t := range v.Tasks {
		if t.Refs.Job == job || inherited != "" && t.Refs.Job == inherited {
			n++
			switch value := t.Priority.(type) {
			case nil:
				priority = 0
			case float64:
				if value != float64(int(value)) {
					return 0, 0, false
				}
				priority = int(value)
			case string:
				parsed, err := strconv.Atoi(strings.TrimSpace(value))
				if err != nil {
					return 0, 0, false
				}
				priority = parsed
			default:
				return 0, 0, false
			}
			if t.Expected != nil {
				duration, _ = quotaDuration(*t.Expected)
			}
		}
	}
	return priority, duration, n == 1
}

// cliQuotaPolicy is the manager's read of `handoffkeep doc get
// policy/quota-stop` at ab2e296, copied: ok is false where it fell back to
// its cache.
func cliQuotaPolicy(code int, out string) (quotaPolicy, bool) {
	if code != 0 {
		return quotaPolicy{}, false
	}
	var doc struct {
		Body string `json:"body"`
	}
	if json.Unmarshal([]byte(out), &doc) != nil {
		return quotaPolicy{}, false
	}
	return parseQuotaPolicy(doc.Body)
}

// The task lookup and the policy read take the same hk answer to the same
// result over HTTP as the copied CLI parser on the CLI's output; a policy
// read that fails either way falls back to the last good copy.
func TestQuotaStopCLIHTTPParity(t *testing.T) {
	ctx := context.Background()
	many := make([]string, 1000)
	for i := range many {
		many[i] = fmt.Sprintf(`{"id":%d,"priority":3,"refs":{"job_id":"job-%d"}}`, i+1, i)
	}
	for _, tc := range []struct {
		name     string
		status   int
		answer   string
		priority int
		expected time.Duration
		found    bool
	}{
		{"one match", 200, `{"tasks":[{"priority":2,"expected":"4h","refs":{"job_id":"job-active"}}]}`, 2, 4 * time.Hour, true},
		{"days", 200, `{"tasks":[{"priority":3,"expected":"2d","refs":{"job_id":"job-active"}}]}`, 3, 48 * time.Hour, true},
		{"inherited", 200, `{"tasks":[{"priority":1,"refs":{"job_id":"job-builder"}}]}`, 1, 0, true},
		{"null expected", 200, `{"tasks":[{"priority":1,"expected":null,"refs":{"job_id":"job-active"}}]}`, 1, 0, true},
		{"unparseable expected", 200, `{"tasks":[{"priority":1,"expected":"soon","refs":{"job_id":"job-active"}}]}`, 1, 0, true},
		{"hk duration string", 200, `{"tasks":[{"priority":1,"expected":"24h0m0s","refs":{"job_id":"job-active"}}]}`, 1, 24 * time.Hour, true},
		{"bad timestamp elsewhere", 200, `{"tasks":[{"id":"7","priority":2,"created_at":"yesterday","refs":{"job_id":"job-active","pr":7}}]}`, 2, 0, true},
		{"two matches", 200, `{"tasks":[{"priority":1,"refs":{"job_id":"job-active"}},{"priority":2,"refs":{"job_id":"job-builder"}}]}`, 2, 0, false},
		{"no match", 200, `{"tasks":[{"priority":1,"refs":{"job_id":"job-other"}}]}`, 0, 0, false},
		{"no tasks", 200, `{"tasks":[]}`, 0, 0, false},
		{"string priority", 200, `{"tasks":[{"priority":"2","refs":{"job_id":"job-active"}}]}`, 2, 0, true},
		{"invalid string priority", 200, `{"tasks":[{"priority":"urgent","refs":{"job_id":"job-active"}}]}`, 0, 0, false},
		{"non-integer priority", 200, `{"tasks":[{"priority":1.5,"refs":{"job_id":"job-active"}}]}`, 0, 0, false},
		{"unrelated invalid priority", 200, `{"tasks":[{"priority":"P1","refs":{"job_id":"job-other"}},{"priority":2,"refs":{"job_id":"job-active"}}]}`, 2, 0, true},
		{"numeric expected", 200, `{"tasks":[{"priority":2,"expected":4,"refs":{"job_id":"job-active"}}]}`, 0, 0, false},
		{"non-string job_id", 200, `{"tasks":[{"priority":2,"refs":{"job_id":7}}]}`, 0, 0, false},
		{"a full page", 200, `{"tasks":[` + strings.Join(append(many[:999:999], `{"priority":2,"refs":{"job_id":"job-active"}}`), ",") + `]}`, 0, 0, false},
		{"malformed", 200, `{"tasks":[`, 0, 0, false},
		{"empty 2xx", 200, ``, 0, 0, false},
		{"outage", 500, `{"error":"internal"}`, 0, 0, false},
	} {
		t.Run("task "+tc.name, func(t *testing.T) {
			code := 0
			if tc.status != 200 {
				code = 1
			}
			p, d, ok := cliQuotaTask(code, tc.answer, "job-active", "job-builder")
			if p != tc.priority || d != tc.expected || ok != tc.found {
				t.Fatalf("the CLI parser gives %d %v %t; the table %d %v %t", p, d, ok, tc.priority, tc.expected, tc.found)
			}
			ran := noHKBinary(t)
			newHKSitesFake(t, func(r hkSitesRequest) (int, string) {
				if r.Route() != "GET /v1/tasks" {
					return http.StatusNotFound, `{"error":"not_found"}`
				}
				return tc.status, tc.answer
			})
			q := quotaSitesManager(t)
			p, d, ok = q.task(ctx, HubActiveJob{JobID: "job-active"}, "job-builder", "")
			if p != tc.priority || d != tc.expected || ok != tc.found || ran() {
				t.Fatalf("HTTP gives %d %v %t (hk CLI ran=%t); CLI %d %v %t", p, d, ok, ran(), tc.priority, tc.expected, tc.found)
			}
		})
	}
	good := func(body string) string {
		b, _ := json.Marshal(map[string]any{"id": 3, "key": "policy/quota-stop", "kind": "policy", "body": body, "created_at": "2026-11-08T08:00:00Z"})
		return string(b)
	}
	badTimestamp := func(body string) string {
		b, _ := json.Marshal(map[string]any{"id": "3", "key": "policy/quota-stop", "created_at": "yesterday", "body": body})
		return string(b)
	}
	other := strings.Replace(quotaSitesPolicy, `"continue"`, `"resume now"`, 1)
	for _, tc := range []struct {
		name   string
		status int
		answer string
		ok     bool
	}{
		{"policy", 200, good(other), true},
		{"bad timestamp", 200, badTimestamp(other), true},
		{"not a policy", 200, good("prose"), false},
		{"body not a string", 200, `{"key":"policy/quota-stop","body":{}}`, false},
		{"malformed", 200, `{"body":`, false},
		{"empty 2xx", 200, ``, false},
		{"not found", 404, `{"error":"not_found"}`, false},
		{"outage", 500, `{"error":"internal"}`, false},
	} {
		t.Run("policy "+tc.name, func(t *testing.T) {
			code := 0
			if tc.status != 200 {
				code = 1
			}
			if _, ok := cliQuotaPolicy(code, tc.answer); ok != tc.ok {
				t.Fatalf("the CLI parser reads the policy=%t; the table %t", ok, tc.ok)
			}
			ran := noHKBinary(t)
			newHKSitesFake(t, func(r hkSitesRequest) (int, string) {
				if r.Route() != "GET /v1/documents/policy%2Fquota-stop" {
					return http.StatusNotFound, `{"error":"not_found"}`
				}
				return tc.status, tc.answer
			})
			q := quotaSitesManager(t)
			// The last good copy is the policy with "continue"; a read
			// that fails keeps it.
			if err := os.WriteFile(filepath.Join(q.root, "quota-stop-policy-cache.json"), []byte(quotaSitesPolicy), 0o600); err != nil {
				t.Fatal(err)
			}
			p, ok := q.loadPolicy(ctx, time.Now())
			want := "continue"
			if tc.ok {
				want = "resume now"
			}
			if !ok || p.ResumeText != want || ran() {
				t.Fatalf("HTTP policy=%q ok=%t (hk CLI ran=%t), want %q", p.ResumeText, ok, ran(), want)
			}
			if tc.ok {
				if b, err := os.ReadFile(filepath.Join(q.root, "quota-stop-policy-cache.json")); err != nil || string(b) != other {
					t.Fatalf("cache=%q err=%v, want the read policy kept as the last good copy", b, err)
				}
			}
		})
	}
}
