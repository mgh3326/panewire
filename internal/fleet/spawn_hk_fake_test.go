package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// hkSvcTestToken is the svc-panewire bearer a fake hk expects; hkCLITestToken
// is the hk CLI's own token, which the spawn path must never send.
const (
	hkSvcTestToken = "svc-panewire-test-token"
	hkCLITestToken = "hk-cli-config-token"
)

// hkRequest is one request a fake hk received.
type hkRequest struct {
	Method, Path, Auth string
	// Query is the raw query string, for the list routes.
	Query  string
	Body   []byte
	Header http.Header
}

// Route is "METHOD /escaped/path".
func (r hkRequest) Route() string { return r.Method + " " + r.Path }

// spawnHK is an hk HTTP server for one test: the spawn path reaches it through
// the svc-panewire credential in the environment (HANDOFFKEEP_URL and
// HANDOFFKEEP_TOKEN_SVC_PANEWIRE), exactly as hkcred.Load resolves it, while
// HANDOFFKEEP_TOKEN holds the hk CLI's token so a request that carries it is
// caught. It serves tasks, project owners and documents from its maps; a
// handler set with Handle answers first.
type spawnHK struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	reqs   []hkRequest
	tasks  map[string]string
	owners map[string]string
	docs   map[string]string
	handle []func(hkRequest) (int, string, bool)
}

var spawnHKs sync.Map // *testing.T -> *spawnHK

// useFakeHK returns the test's fake hk, starting it on first use, and points
// the credential environment at it (again: testApp clears it).
func useFakeHK(t *testing.T) *spawnHK {
	t.Helper()
	if hk, ok := spawnHKs.Load(t); ok {
		hk.(*spawnHK).env(t)
		return hk.(*spawnHK)
	}
	hk := &spawnHK{t: t, tasks: map[string]string{}, owners: map[string]string{}, docs: map[string]string{}}
	hk.srv = httptest.NewServer(http.HandlerFunc(hk.serve))
	spawnHKs.Store(t, hk)
	t.Cleanup(func() {
		hk.srv.Close()
		spawnHKs.Delete(t)
	})
	hk.env(t)
	return hk
}

// rearmFakeHK points the credential environment back at the test's fake hk,
// if it has one; testApp calls it after clearing that environment.
func rearmFakeHK(t *testing.T) {
	if hk, ok := spawnHKs.Load(t); ok {
		hk.(*spawnHK).env(t)
	}
}

func (hk *spawnHK) env(t *testing.T) {
	t.Setenv("HANDOFFKEEP_URL", hk.srv.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", hkSvcTestToken)
	t.Setenv("HANDOFFKEEP_TOKEN", hkCLITestToken)
}

// SetTask stores a task answer as GET /v1/tasks/{id} returns it.
func (hk *spawnHK) SetTask(id, body string) *spawnHK {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	hk.tasks[id] = body
	return hk
}

// SetOwner stores a project owner answer.
func (hk *spawnHK) SetOwner(project, body string) *spawnHK {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	hk.owners[project] = body
	return hk
}

// SetDoc stores a document whose body is body.
func (hk *spawnHK) SetDoc(key, body string) *spawnHK {
	b, _ := json.Marshal(map[string]any{"key": key, "kind": "policy", "body": body})
	hk.mu.Lock()
	defer hk.mu.Unlock()
	hk.docs[key] = string(b)
	return hk
}

// Handle answers matching requests before every earlier handler and the maps;
// ok=false passes the request on.
func (hk *spawnHK) Handle(f func(hkRequest) (status int, body string, ok bool)) *spawnHK {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	hk.handle = append([]func(hkRequest) (int, string, bool){f}, hk.handle...)
	return hk
}

// Requests returns every request received so far.
func (hk *spawnHK) Requests() []hkRequest {
	hk.mu.Lock()
	defer hk.mu.Unlock()
	return append([]hkRequest(nil), hk.reqs...)
}

// Routes lists the requests as "METHOD /path".
func (hk *spawnHK) Routes() []string {
	var out []string
	for _, r := range hk.Requests() {
		out = append(out, r.Route())
	}
	return out
}

// Close stops the server, so the next call finds hk unreachable.
func (hk *spawnHK) Close() { hk.srv.Close() }

// hkDocumentKinds is the kind vocabulary hk accepts on a document write: a
// copy of documentKinds at handoffkeep internal/store/store.go:38 (v0.0.130).
// hk answers 400 invalid_context for any other kind, so the fake does too.
var hkDocumentKinds = map[string]bool{"brief": true, "report": true, "answer": true, "handoff": true, "note": true, "other": true, "decision": true, "research": true, "design": true, "policy": true, "evidence": true}

func (hk *spawnHK) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	req := hkRequest{Method: r.Method, Path: r.URL.EscapedPath(), Auth: r.Header.Get("Authorization"), Query: r.URL.RawQuery, Body: body, Header: r.Header.Clone()}
	hk.mu.Lock()
	hk.reqs = append(hk.reqs, req)
	handlers := append([]func(hkRequest) (int, string, bool)(nil), hk.handle...)
	hk.mu.Unlock()
	if req.Auth != "Bearer "+hkSvcTestToken {
		hk.t.Errorf("%s: Authorization %q, want the svc-panewire bearer", req.Route(), req.Auth)
	}
	answer := func(status int, out string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, out)
	}
	for _, h := range handlers {
		if status, out, ok := h(req); ok {
			answer(status, out)
			return
		}
	}
	hk.mu.Lock()
	defer hk.mu.Unlock()
	notFound := `{"error":"not_found"}`
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(req.Path, "/v1/tasks/projects/") && strings.HasSuffix(req.Path, "/owner"):
		project := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/tasks/projects/"), "/owner")
		if out, ok := hk.owners[project]; ok {
			answer(http.StatusOK, out)
			return
		}
	case r.Method == http.MethodGet && strings.HasPrefix(req.Path, "/v1/tasks/") && !strings.Contains(strings.TrimPrefix(req.Path, "/v1/tasks/"), "/"):
		if out, ok := hk.tasks[strings.TrimPrefix(req.Path, "/v1/tasks/")]; ok {
			answer(http.StatusOK, out)
			return
		}
	case r.Method == http.MethodGet && strings.HasPrefix(req.Path, "/v1/documents/"):
		if out, ok := hk.docs[strings.TrimPrefix(r.URL.Path, "/v1/documents/")]; ok {
			answer(http.StatusOK, out)
			return
		}
	case r.Method == http.MethodPut && strings.HasPrefix(req.Path, "/v1/documents/"):
		var doc map[string]any
		if json.Unmarshal(body, &doc) != nil {
			answer(http.StatusBadRequest, `{"error":"invalid_request"}`)
			return
		}
		if kind, _ := doc["kind"].(string); !hkDocumentKinds[kind] {
			answer(http.StatusBadRequest, `{"error":"invalid_context","reason":"invalid kind"}`)
			return
		}
		// hk stores the document under the key in the path with the sha256 of
		// its body, and GET answers with it.
		key := strings.TrimPrefix(r.URL.Path, "/v1/documents/")
		content, _ := doc["body"].(string)
		sum := sha256.Sum256([]byte(content))
		doc["key"], doc["sha256"] = key, hex.EncodeToString(sum[:])
		stored, _ := json.Marshal(doc)
		hk.docs[key] = string(stored)
		out, _ := json.Marshal(map[string]any{"document": doc, "changed": true})
		answer(http.StatusOK, string(out))
		return
	case r.Method == http.MethodPut && req.Path == "/v1/attachments":
		sha := sha256.Sum256(body)
		refKind, refID, _ := strings.Cut(r.Header.Get("X-HK-Ref"), ":")
		attachment, _ := json.Marshal(map[string]any{
			"attachment": map[string]any{
				"sha256": hex.EncodeToString(sha[:]), "size_bytes": len(body),
				"mime": r.Header.Get("Content-Type"), "original_name": r.Header.Get("X-HK-Name"),
				"ref_kind": refKind, "ref_id": refID,
			},
			"created": true,
		})
		answer(http.StatusCreated, string(attachment))
		return
	}
	answer(http.StatusNotFound, notFound)
}

// hkTaskJSON is a task as hk's GET /v1/tasks/{id} encodes store.Task — the
// JSON the hk CLI's `tasks show` printed, since the CLI printed that answer.
// An empty project is null, as hk sends a task with no project.
func hkTaskJSON(id int, state, title, claimedBy, project string, refs string) string {
	p := "null"
	if project != "" {
		p = fmt.Sprintf("%q", project)
	}
	return fmt.Sprintf(`{"id":%d,"lane":"workers","title":%q,"kind":"implement","state":%q,"priority":3,"refs":%s,`+
		`"claimed_by":%q,"created_by":"operator-desk","created_at":"2025-12-31T00:00:00Z","updated_at":"2025-12-31T01:00:00Z",`+
		`"project":%s,"category":"started","awaiting_decision":false}`, id, title, state, refs, claimedBy, p)
}

// hkClaimAnswer answers POST /v1/tasks/{id}/claim as hk does: the task,
// started, claimed by the caller and linked to its job.
func hkClaimAnswer(req hkRequest) (int, string, bool) {
	if req.Method != http.MethodPost || !strings.HasSuffix(req.Path, "/claim") {
		return 0, "", false
	}
	var in struct {
		ClaimedBy string `json:"claimed_by"`
		JobID     string `json:"job_id"`
	}
	_ = json.Unmarshal(req.Body, &in)
	id := strings.TrimSuffix(strings.TrimPrefix(req.Path, "/v1/tasks/"), "/claim")
	b, _ := json.Marshal(map[string]any{"id": json.Number(id), "state": "developing", "claimed_by": in.ClaimedBy, "refs": map[string]any{"job_id": in.JobID}})
	return http.StatusOK, string(b), true
}

// hkFails answers every request matching route ("METHOD /path" prefix) with
// status and an hk error body.
func hkFails(route string, status int, body string) func(hkRequest) (int, string, bool) {
	return func(req hkRequest) (int, string, bool) {
		if !strings.HasPrefix(req.Route(), route) {
			return 0, "", false
		}
		return status, body, true
	}
}

// failHKCLI makes f fail the test when the spawn path runs the hk CLI for a
// call it now makes over HTTP; any other command goes to f's handler.
func failHKCLI(t *testing.T, f *fakeRunner) {
	t.Helper()
	next := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "handoffkeep" {
			args := strings.Join(c.Args, " ")
			brief := strings.HasPrefix(args, "doc put") && strings.Contains(args, "--kind brief")
			for _, sub := range []string{"tasks show", "tasks claim", "tasks rebind", "tasks owner", "doc get " + routingDocKey} {
				if strings.HasPrefix(args, sub) || brief {
					t.Errorf("spawn path ran the hk CLI: handoffkeep %s", args)
					return Result{Code: 1, Err: "hk CLI must not run"}
				}
			}
		}
		if next != nil {
			return next(c)
		}
		return Result{}
	}
}

// hkClaimed reports whether the test's fake hk, if it has one, received a
// task claim.
func hkClaimed(t *testing.T) bool {
	hk, ok := spawnHKs.Load(t)
	if !ok {
		return false
	}
	for _, r := range hk.(*spawnHK).Requests() {
		if r.Method == http.MethodPost && strings.HasSuffix(r.Path, "/claim") {
			return true
		}
	}
	return false
}
