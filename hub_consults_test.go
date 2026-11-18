package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mgh3326/panewire/internal/fleet"
)

func consultAuthorRequest() fleet.Submission {
	return fleet.Submission{
		RequestID: uuid.NewString(),
		Args:      []string{"-c", "/work/project", "-p", "/work/brief.md", "-l", "author-job", "--job", "author-job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer", "--host", "auto"},
		Brief:     "Implement the task",
		Repository: fleet.Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work", Worktree: "author-checkout",
			Checkout: &fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/task", Sha: strings.Repeat("a", 40), Dir: "author-checkout"}},
		OriginMachine: "machine-a", DeadlineSeconds: 600,
	}
}

func consultPlanFixture(job string) fleet.ConsultPlan {
	request := fleet.Submission{
		RequestID: uuid.NewString(),
		Args: []string{"-c", "/work/project", "-p", "/consult-brief", "-m", "codex-astra", "--effort", "high", "-l", job, "--job", job, "--t", "T1",
			"--task", "42", "--owner", "parent", "--role", "consult", "--host", "auto", "--override-reason", "consult seat A",
			"--containment", "hk:42", "--author-family", "anthropic", "--report-relative", job + "/report.md"},
		Brief: "consult brief",
		Repository: fleet.Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work", Worktree: job + "-checkout",
			Checkout: &fleet.HubCheckout{Origin: "https://github.com/example/project.git", Mode: "detached", Sha: strings.Repeat("b", 40), Fetch: "pull/7/head", Dir: job + "-checkout"}},
		OriginMachine: "hub", DeadlineSeconds: 600,
	}
	return fleet.ConsultPlan{
		Seat:    fleet.SeatChoice{Profile: "codex-astra", Effort: "high", Family: "openai", Grade: "SS"},
		Job:     job,
		Request: request,
	}
}

type consultFixture struct {
	hub    *HubServer
	jobs   *quotaJobs
	calls  int
	plans  []fleet.ConsultPlan
	inputs []fleet.ConsultPlanInput
	err    error
}

func newConsultFixture(t *testing.T) *consultFixture {
	t.Helper()
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{
		Tokens:       map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token-b", "svc-consult": "svc-token", "svc-other": "other-token"},
		PRStatusHost: "machine-a",
		Scopes:       map[string]string{"svc-consult": "consult", "svc-other": "inbox.ack_state"},
		// InboxStorePath is the durable store the consult rows live in.
		InboxStorePath:  filepath.Join(t.TempDir(), "inbox.sqlite3"),
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"},"panewire-design":{"machine":"machine-b","pane":"w2:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, relay, stopRelay := newFakeHandoffkeep(t)
	t.Cleanup(stopRelay)
	h.handoffkeep = relay
	t.Cleanup(func() { h.Close() })
	f := &consultFixture{hub: h, jobs: &quotaJobs{records: map[string]map[string]any{}}}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), consultAuthorRequest(), "author-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: f.jobs, Out: out, Err: errout}, nil
	}
	// The task is bound to the fixture's author job: an omitted author job
	// takes its repository evidence from there.
	h.consultTaskBinding = func(context.Context, string) (string, error) { return "author-job", nil }
	h.consultPlan = func(_ context.Context, in fleet.ConsultPlanInput) (fleet.ConsultPlan, error) {
		f.inputs = append(f.inputs, in)
		f.calls++
		if f.err != nil {
			return fleet.ConsultPlan{}, f.err
		}
		if len(f.plans) != 0 {
			plan := f.plans[0]
			f.plans = f.plans[1:]
			return plan, nil
		}
		return consultPlanFixture(fmt.Sprintf("consult-42-%d", f.calls)), nil
	}
	return f
}

func consultRequestBody(task, pr, head string) map[string]string {
	return map[string]string{
		"task_id": task, "requester": "parent", "question_doc": "hk:doc/design/q",
		"author_job": "author-job", "pr": pr, "head": head,
	}
}

func (f *consultFixture) post(t *testing.T, machine, token string, body map[string]string) (int, map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest("POST", "/v1/consults", bytes.NewReader(raw))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if machine != "" {
		r.Header.Set("X-Panewire-Machine-ID", machine)
	}
	w := httptest.NewRecorder()
	f.hub.Handler().ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func (f *consultFixture) get(t *testing.T, id string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("GET", "/v1/consults/"+id, nil)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	f.hub.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s = %d %s", id, w.Code, w.Body)
	}
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out
}

func (f *consultFixture) setSpawnStatus(t *testing.T, requestID, status string) {
	t.Helper()
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE spawn_operations SET status=? WHERE request_id=?`, status, requestID); err != nil {
		t.Fatal(err)
	}
}

func TestConsultRoutesAdmitNodeAndScopedService(t *testing.T) {
	f := newConsultFixture(t)
	head := strings.Repeat("a", 40)
	body := consultRequestBody("42", "example/project#7", head)

	if code, _ := f.post(t, "", "", body); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated = %d, want 401", code)
	}
	if code, _ := f.post(t, "svc-other", "other-token", body); code != http.StatusForbidden {
		t.Fatalf("unscoped service = %d, want 403", code)
	}
	if code, _ := f.post(t, "machine-b", "node-token-b", body); code != http.StatusForbidden {
		t.Fatalf("node outside the pr-status host = %d, want 403", code)
	}
	if code, out := f.post(t, "machine-a", "node-token", body); code != http.StatusCreated || out["state"] != consultQueued {
		t.Fatalf("pr-status node credential = %d %v, want 201 queued", code, out)
	}
	other := consultRequestBody("42", "example/project#7", strings.Repeat("b", 40))
	if code, out := f.post(t, "svc-consult", "svc-token", other); code != http.StatusCreated || out["state"] != consultQueued {
		t.Fatalf("scoped service = %d %v, want 201 queued", code, out)
	}
	operator := consultRequestBody("42", "example/project#7", strings.Repeat("c", 40))
	if code, out := f.post(t, "", t14OperatorToken, operator); code != http.StatusCreated {
		t.Fatalf("operator = %d %v, want 201", code, out)
	}
}

func TestConsultSameKeyReturnsExistingConsult(t *testing.T) {
	f := newConsultFixture(t)
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	code, first := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated {
		t.Fatalf("create = %d %v", code, first)
	}
	code, second := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusOK || second["id"] != first["id"] {
		t.Fatalf("replay = %d %v, want the row %v", code, second, first["id"])
	}
	var rows int
	if err := f.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM consult_requests`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows = %d err=%v, want 1", rows, err)
	}
	changed := consultRequestBody("42", "example/project#7", strings.Repeat("d", 40))
	if code, next := f.post(t, "", t14OperatorToken, changed); code != http.StatusCreated || next["id"] == first["id"] {
		t.Fatalf("new head = %d %v, want a new consult", code, next)
	}
}

func TestConsultNoSeatStoresRefusedAndSpawnsNothing(t *testing.T) {
	f := newConsultFixture(t)
	f.err = failConsultSeat()
	code, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	if code != http.StatusCreated || out["state"] != consultRefused {
		t.Fatalf("no seat = %d %v, want 201 refused", code, out)
	}
	if !strings.Contains(fmt.Sprint(out["reason"]), "NO_ROLE_CANDIDATE") {
		t.Fatalf("reason = %v", out["reason"])
	}
	var operations int
	if err := f.hub.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations`).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	// Only the author job's operation exists; the refused consult added none.
	if operations != 1 {
		t.Fatalf("spawn operations = %d, want only the author's", operations)
	}
}

func failConsultSeat() error {
	return fmt.Errorf("NO_ROLE_CANDIDATE: no consult seat passes the floor")
}

// consultAnswerHK serves the stored report document.
func consultAnswerHK(t *testing.T, body string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1/documents/") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not_found"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"key": strings.TrimPrefix(r.URL.Path, "/v1/documents/"), "kind": "report", "body": body})
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "svc-panewire-consult-token")
}

const consultValidAnswer = "## Verdict\napply\n\n## Root cause\nOne wrong point.\n\n## Design\nOne design.\n\n## Findings\n| finding id | action | reason |\n| --- | --- | --- |\n| F1 | fix | real |\n"

func TestConsultAnsweredRecordsAnswerDocument(t *testing.T) {
	f := newConsultFixture(t)
	code, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	if code != http.StatusCreated {
		t.Fatalf("create = %d %v", code, out)
	}
	id := fmt.Sprint(out["id"])
	row, err := f.hub.inbox.store.consultByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, row.RequestID, "succeeded")
	doc := "reports/" + row.Job + "/sha256/" + strings.Repeat("e", 64) + ".md"
	f.jobs.records[row.Job] = map[string]any{"state": "completed", "report_doc": doc}
	consultAnswerHK(t, consultValidAnswer)

	f.hub.reconcileConsults(t.Context())

	row, err = f.hub.inbox.store.consultByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != consultAnswered || row.AnswerDoc != doc || row.Answered.IsZero() {
		t.Fatalf("row = %+v, want answered with %s", row, doc)
	}
	items, err := f.hub.inbox.store.list("parent", true)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range items {
		if item.Event.NotifyType == "ready" && strings.Contains(item.Event.Text, doc) {
			found = true
		}
	}
	if !found {
		t.Fatalf("lane event = %+v, want one ready event naming %s", items, doc)
	}
	// The answer is saved before its event; a hub that stopped in between
	// retries from the answered row.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE consult_requests SET notified=0 WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcileConsults(t.Context())
	row, err = f.hub.inbox.store.consultByID(t.Context(), id)
	if err != nil || !row.Notified {
		t.Fatalf("row after notification retry = %+v err=%v, want notified", row, err)
	}
}

func TestConsultReportMissingSectionFails(t *testing.T) {
	f := newConsultFixture(t)
	_, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, row.RequestID, "succeeded")
	f.jobs.records[row.Job] = map[string]any{"state": "completed", "report_doc": "reports/" + row.Job + "/sha256/x.md"}
	consultAnswerHK(t, "## Verdict\napply\n\n## Design\nx\n\n## Findings\n| a | b |\n")

	f.hub.reconcileConsults(t.Context())
	row, err = f.hub.inbox.store.consultByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != consultFailed || !strings.Contains(row.Reason, "## Root cause") {
		t.Fatalf("row = %+v, want failed naming the missing section", row)
	}
}

func TestConsultLostJobRespawnsOnceThenFails(t *testing.T) {
	f := newConsultFixture(t)
	_, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	first, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, first.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())

	second, err := f.hub.inbox.store.consultByID(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Respawns != 1 || second.Job == first.Job || second.RequestID == first.RequestID || second.State != consultQueued {
		t.Fatalf("respawn = %+v, want a second job queued", second)
	}
	f.setSpawnStatus(t, second.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())
	second, err = f.hub.inbox.store.consultByID(t.Context(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if second.State != consultFailed || second.Respawns != 1 {
		t.Fatalf("after the second failure = %+v, want failed with one respawn", second)
	}
}

func TestConsultRowsSurviveRestart(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	open := func() *HubServer {
		h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken}, InboxStorePath: path, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	first := open()
	now := time.Now().UTC()
	want := hubConsult{ID: uuid.NewString(), IdemKey: "42|r#1|abc", Task: "42", PR: "r#1", Head: "abc", Requester: "parent", QuestionDoc: "hk:doc/q", State: consultQueued, Created: now, Deadline: now.Add(time.Hour)}
	if _, _, err := first.inbox.store.insertConsult(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := open()
	defer func() { _ = second.Close() }()
	active, err := second.inbox.store.activeConsults(t.Context())
	if err != nil || len(active) != 1 || active[0].ID != want.ID || active[0].State != consultQueued {
		t.Fatalf("active after restart = %+v err=%v", active, err)
	}
	r := httptest.NewRequest("GET", "/v1/consults/"+want.ID, nil)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	second.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"state":"queued"`) {
		t.Fatalf("GET after restart = %d %s", w.Code, w.Body)
	}
}

func TestConsultGetUnknownIsNotFound(t *testing.T) {
	f := newConsultFixture(t)
	r := httptest.NewRequest("GET", "/v1/consults/"+uuid.NewString(), nil)
	r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
	w := httptest.NewRecorder()
	f.hub.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown consult = %d, want 404", w.Code)
	}
}

// A tool requester with no author job owns the spawn through the tool lane,
// never the tool id: completion notices go to a lane that exists.
func TestConsultToolRequesterOwnsOperatorDesk(t *testing.T) {
	f := newConsultFixture(t)
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	body["requester"] = "mergewake/example/project#7"
	delete(body, "author_job")
	code, out := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated {
		t.Fatalf("create = %d %v", code, out)
	}
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	task, err := f.hub.inbox.store.spawnTask(t.Context(), row.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Owner != consultToolLane {
		t.Fatalf("spawn owner = %q, want %q", task.Owner, consultToolLane)
	}
}

// A refusal is a retry: the same key re-plans instead of pinning the dead row.
func TestConsultRefusedRowCanBeRetried(t *testing.T) {
	f := newConsultFixture(t)
	f.err = failConsultSeat()
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	code, first := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated || first["state"] != consultRefused {
		t.Fatalf("first = %d %v, want refused", code, first)
	}
	f.err = nil
	code, second := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated || second["state"] != consultQueued || second["id"] != first["id"] {
		t.Fatalf("retry = %d %v, want queued under the same id", code, second)
	}
	if second["reason"] != nil && second["reason"] != "" {
		t.Fatalf("retry kept the refusal reason: %v", second["reason"])
	}
}

// A failed row is a retry too: a new attempt gets a fresh spawn.
func TestConsultFailedRowCanBeRetried(t *testing.T) {
	f := newConsultFixture(t)
	body := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	_, out := f.post(t, "", t14OperatorToken, body)
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, row.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())
	row, err = f.hub.inbox.store.consultByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, row.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())
	row, err = f.hub.inbox.store.consultByID(t.Context(), row.ID)
	if err != nil || row.State != consultFailed {
		t.Fatalf("failed row = %+v err=%v", row, err)
	}
	code, retry := f.post(t, "", t14OperatorToken, body)
	if code != http.StatusCreated || retry["state"] != consultQueued || retry["id"] != out["id"] {
		t.Fatalf("retry = %d %v, want queued", code, retry)
	}
	row, err = f.hub.inbox.store.consultByID(t.Context(), row.ID)
	if err != nil || row.Respawns != 0 || row.Job == "" {
		t.Fatalf("retried row = %+v err=%v, want a fresh attempt", row, err)
	}
}

// A queued row whose spawn operation is missing (a crash between the row and
// the claim) is started by the coordinator, not left until the deadline.
func TestConsultQueuedRowWithoutSpawnRecovers(t *testing.T) {
	f := newConsultFixture(t)
	_, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.db.Exec(`DELETE FROM spawn_operations WHERE request_id=?`, row.RequestID); err != nil {
		t.Fatal(err)
	}
	// Past the request's pre-claim lease.
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE consult_requests SET claim_until_ms=0 WHERE id=?`, row.ID); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcileConsults(t.Context())
	row, err = f.hub.inbox.store.consultByID(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != consultQueued || row.RequestID == "" {
		t.Fatalf("row = %+v, want a recovered queued attempt", row)
	}
	if _, err := f.hub.inbox.store.spawnTask(t.Context(), row.RequestID); err != nil {
		t.Fatalf("recovered row has no spawn operation: %v", err)
	}
}

// A retry stores the new request body: the question, requester and author
// evidence the plan and the respawn path will read.
func TestConsultRetryStoresNewRequest(t *testing.T) {
	f := newConsultFixture(t)
	f.err = failConsultSeat()
	first := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	if code, out := f.post(t, "", t14OperatorToken, first); code != http.StatusCreated || out["state"] != consultRefused {
		t.Fatalf("first = %d %v", code, out)
	}
	f.err = nil
	second := consultRequestBody("42", "example/project#7", strings.Repeat("a", 40))
	second["question_doc"] = "hk:doc/design/other"
	second["requester"] = "other-lane"
	code, out := f.post(t, "", t14OperatorToken, second)
	if code != http.StatusCreated {
		t.Fatalf("retry = %d %v", code, out)
	}
	row, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	if row.QuestionDoc != "hk:doc/design/other" || row.Requester != "other-lane" {
		t.Fatalf("row = requester %q doc %q, want the retry's body", row.Requester, row.QuestionDoc)
	}
}

// A retry only replaces a terminal row; a second replacement loses the CAS.
func TestConsultReplaceIsCompareAndSet(t *testing.T) {
	store, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if err = store.ensureConsults(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	row := hubConsult{ID: uuid.NewString(), IdemKey: "42|r#1|abc", Task: "42", PR: "r#1", Head: "abc", Requester: "parent", QuestionDoc: "hk:doc/q", State: consultRefused, Created: now, Deadline: now.Add(time.Hour)}
	if _, _, err := store.insertConsult(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	first := row
	first.Job, first.State = "job-1", consultQueued
	if ok, err := store.replaceConsult(t.Context(), first); err != nil || !ok {
		t.Fatalf("first replace = %v err=%v", ok, err)
	}
	second := row
	second.Job, second.State = "job-2", consultQueued
	if ok, err := store.replaceConsult(t.Context(), second); err != nil || ok {
		t.Fatalf("second replace = %v err=%v, want the CAS to refuse", ok, err)
	}
	stored, err := store.consultByID(t.Context(), row.ID)
	if err != nil || stored.Job != "job-1" {
		t.Fatalf("row = %+v err=%v, want job-1", stored, err)
	}
}

func TestConsultLeaseIsBoundedByRemainingTime(t *testing.T) {
	now := time.Now()
	if got := consultLease(now, now.Add(20*time.Second)); got != 10*time.Second {
		t.Fatalf("lease for 20s = %v, want 10s", got)
	}
	if got := consultLease(now, now.Add(2*time.Hour)); got != consultRecoverGrace {
		t.Fatalf("lease for 2h = %v, want %v", got, consultRecoverGrace)
	}
	if got := consultLease(now, now.Add(-time.Second)); got != 0 {
		t.Fatalf("lease past deadline = %v, want 0", got)
	}
}

// A respawn intent saved by a crash keeps excluding the seat that failed, and
// recovery starts from that persisted exclusion.
func TestConsultRespawnRecoveryKeepsExcludedSeat(t *testing.T) {
	f := newConsultFixture(t)
	_, out := f.post(t, "", t14OperatorToken, consultRequestBody("42", "example/project#7", strings.Repeat("a", 40)))
	first, err := f.hub.inbox.store.consultByID(t.Context(), fmt.Sprint(out["id"]))
	if err != nil {
		t.Fatal(err)
	}
	f.setSpawnStatus(t, first.RequestID, "failed")
	f.hub.reconcileConsults(t.Context())
	second, err := f.hub.inbox.store.consultByID(t.Context(), first.ID)
	if err != nil || second.Respawns != 1 {
		t.Fatalf("respawn = %+v err=%v", second, err)
	}
	if !slices.Contains(second.ExcludeProfiles, first.SeatProfile) {
		t.Fatalf("row excludes %v, want %q persisted", second.ExcludeProfiles, first.SeatProfile)
	}
	if _, err := f.hub.inbox.store.db.Exec(`DELETE FROM spawn_operations WHERE request_id=?`, second.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.db.Exec(`UPDATE consult_requests SET claim_until_ms=0 WHERE id=?`, second.ID); err != nil {
		t.Fatal(err)
	}
	f.hub.reconcileConsults(t.Context())
	last := f.inputs[len(f.inputs)-1]
	if !slices.Contains(last.ExcludeProfiles, first.SeatProfile) {
		t.Fatalf("recovery plan excludes %v, want %q", last.ExcludeProfiles, first.SeatProfile)
	}
}
