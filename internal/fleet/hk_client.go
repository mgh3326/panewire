package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/panewire/internal/hkcred"
)

// newHKClient builds the hk public client panewire calls hk through in place
// of the `handoffkeep` CLI (hk 3526 §5): the spawn path's task precheck,
// claim, rebind and link check, owner lookup, routing policy read and hub
// brief upload (hk 3591), and `panewire work`'s task read, the artifact
// records and sweep, the lifecycle report upload and task comment, the job
// command's report upload and the quota stop manager's calls (hk 3607). The
// bearer is panewire's svc-panewire credential from hkcred.Load — the
// environment, else ~/.config/panewire/hk-svc.env — and never the hk CLI's
// config.env. The HTTP settings are those of panewire's other hk clients (catalog.go,
// fleet.go): the catalogTimeout per call (hkCallTimeout), no redirect
// followed, HTTPS off loopback and the tailnet, and over plain HTTP no proxy,
// so an HTTP_PROXY never receives the bearer. An unconfigured credential is
// the *hkcred.UnconfiguredError Load returns; no error names the URL or a
// value.
func newHKClient() (client.Client, error) { return newHKClientWithin(hkCallTimeout) }

// The bounds of one hk call. An upload — the hub brief, a report, an
// artifact record (hkPutDocument) — has its own, longer one: the hk CLI's
// `doc put` it replaces had no timeout, and a large file over a slow link
// must not fail at a task read's bound. Variables only so a test can shrink
// them.
var (
	hkCallTimeout   = catalogTimeout
	hkUploadTimeout = 60 * time.Second
)

// newHKClientWithin is newHKClient with timeout as each call's bound.
func newHKClientWithin(timeout time.Duration) (client.Client, error) {
	creds, err := hkcred.Load()
	if err != nil {
		return client.Client{}, err
	}
	u, err := url.Parse(creds.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return client.Client{}, errors.New("invalid hk endpoint")
	}
	plain := u.Scheme == "http"
	if u.Scheme != "https" && !(plain && (isLoopbackHost(u.Hostname()) || isTailnetHost(u.Hostname()))) {
		return client.Client{}, errors.New("hk endpoint requires HTTPS off loopback and the tailnet")
	}
	h := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if plain {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil
		h.Transport = t
	}
	return client.Client{URL: strings.TrimRight(creds.URL, "/"), Token: creds.Token, CFAccessClientID: creds.CFID, CFAccessClientSecret: creds.CFSecret, HTTP: h}, nil
}

// hkCLIErrorExit is the status the hk CLI exited with on every hk error. The
// TASK_LINK_FAILED reason still names it ("task claim exited with status 1"),
// because owner lanes and their tests read that text.
const hkCLIErrorExit = 1

// hkErrText renders an hk call error as the hk CLI printed it on stderr, so a
// spawn-path message that carried the CLI's stderr reads the same over HTTP.
func hkErrText(err error) string { return "handoffkeep: " + err.Error() }

// errHKNotFound is the hk CLI's error for a task or document hk does not
// have.
var errHKNotFound = errors.New("not_found")

// hkTaskID parses a task id as the hk CLI did.
func hkTaskID(task string) (int64, error) {
	id, err := strconv.ParseInt(task, 10, 64)
	if err != nil || id < 1 {
		return 0, errors.New("task id must be positive")
	}
	return id, nil
}

// hkTaskIDIs reports whether an answered task is the requested one. The id
// is cliTaskAnswer's: the requested id when hk's answer named it or named
// none, 0 when it named another — an explicit 0 included.
func hkTaskIDIs(t client.Task, task string) bool {
	return t.ID != 0 && strconv.FormatInt(t.ID, 10) == task
}

// errHKUnreadable is a 2xx hk answer the hk CLI path's parser could not read.
// It is not an hk error: hk answered, so --bypass, which covers an hk that
// cannot be reached or refuses, never covers it. Each site reports it as the
// CLI path reported unparseable `handoffkeep` output.
var errHKUnreadable = errors.New("unreadable hk answer")

// hkAnswerLimit bounds the 2xx body kept for cliTaskAnswer. A longer body is
// not kept at all: its first hkAnswerLimit bytes can be complete JSON ahead of
// a truncated or malformed rest, so the capture reads one byte past the limit
// and keeps nothing on overflow, which does not parse and reads as
// errHKUnreadable.
const hkAnswerLimit = 4 << 20

// answerCapture keeps the body of a 2xx answer — hk's client decodes it into
// its types and drops it — so the spawn path can read it as the CLI path did.
// It reads the body whole inside the round trip, under the call's bound; a
// failure to read it is a transport error, as before.
type answerCapture struct {
	next     http.RoundTripper
	body     []byte
	answered bool
}

func (c *answerCapture) RoundTrip(r *http.Request) (*http.Response, error) {
	c.body, c.answered = nil, false
	resp, err := c.next.RoundTrip(r)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, hkAnswerLimit+1))
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if len(body) > hkAnswerLimit {
		body = nil
	}
	c.body, c.answered = body, true
	return resp, nil
}

// hkAnswer runs call on an hk client that keeps the 2xx answer's body.
// answered is false when hk sent none — a transport failure, a non-2xx or
// HTML answer (*client.HTTPError), a refused redirect — and call's error then
// stands as it is: those keep their --bypass semantics.
func hkAnswer(call func(client.Client) error) (body []byte, answered bool, err error) {
	c, err := newHKClient()
	if err != nil {
		return nil, false, err
	}
	h := *c.HTTP
	capture := &answerCapture{next: h.Transport}
	if capture.next == nil {
		capture.next = http.DefaultTransport
	}
	h.Transport = capture
	c.HTTP = &h
	err = call(c)
	var he *client.HTTPError
	if errors.As(err, &he) || !capture.answered {
		return nil, false, err
	}
	return capture.body, true, err
}

// hkTaskCall runs a task call (read, claim, rebind) and reads hk's answer
// with the hk CLI path's parser (cliTaskAnswer), so the spawn path sees the
// fields it saw through `handoffkeep`: a string id or priority, a non-string
// job_id and a timestamp that does not parse read as they did, where hk's
// typed client fails to decode. An answer that parser cannot read is
// errHKUnreadable. Any other error is call's own: an hk error or outage, or
// the client's check of an answer it decoded (claim_claimant_not_recorded).
func hkTaskCall(task string, call func(client.Client, int64) (client.Task, error)) (client.Task, error) {
	id, err := hkTaskID(task)
	if err != nil {
		return client.Task{}, err
	}
	body, answered, err := hkAnswer(func(c client.Client) error {
		_, e := call(c, id)
		return e
	})
	if !answered {
		return client.Task{}, err
	}
	if !utf8.Valid(body) {
		return client.Task{}, errHKUnreadable
	}
	t, ok := cliTaskAnswer(body, task)
	if !ok {
		return client.Task{}, errHKUnreadable
	}
	if json.NewDecoder(bytes.NewReader(body)).Decode(new(client.Task)) != nil {
		// The client's error is its decode of an answer the CLI parser
		// reads, so its checks of the answer never ran; the spawn path
		// makes its own, as it did on the CLI's output.
		err = nil
	}
	return t, err
}

// cliTaskAnswer reads a task answer as panewire read `handoffkeep tasks
// show|claim|rebind` output (the Task type), and gives it as a client.Task.
// The id is the requested one when the answer names it — a number or a
// string — or names none (absent or null), as that parser accepted; a task
// naming another id, 0 included, has id 0, which hkTaskIDIs refuses. A
// created_at that does not parse is zero, so the task's wait reads 0; a
// priority is a whole number or a numeric string, else 0; a non-string
// refs.job_id reads "".
func cliTaskAnswer(body []byte, task string) (client.Task, bool) {
	var t Task
	if json.Unmarshal(body, &t) != nil {
		return client.Task{}, false
	}
	out := client.Task{Title: t.Title, State: t.State, ClaimedBy: t.ClaimedBy, Refs: client.TaskRefs{JobID: str(t.Refs, "job_id")}}
	if t.ID == nil || scalar(t.ID) == task {
		out.ID, _ = strconv.ParseInt(task, 10, 64)
	}
	if t.Project != "" {
		out.Project = &t.Project
	}
	if ts, e := time.Parse(time.RFC3339Nano, t.CreatedAt); e == nil {
		out.CreatedAt = ts
	}
	switch n := t.Priority.(type) {
	case float64:
		if n == float64(int(n)) {
			out.Priority = int(n)
		}
	case string:
		out.Priority, _ = strconv.Atoi(strings.TrimSpace(n))
	}
	return out, true
}

// hkGetTask reads one task; a task hk does not have is errHKNotFound.
func hkGetTask(ctx context.Context, task string) (client.Task, error) {
	return hkTaskCall(task, func(c client.Client, id int64) (client.Task, error) {
		t, found, err := c.GetTask(ctx, id)
		if err == nil && !found {
			err = errHKNotFound
		}
		return t, err
	})
}

// hkTaskProject is the task's project, "" when it has none.
func hkTaskProject(t client.Task) string {
	if t.Project == nil {
		return ""
	}
	return *t.Project
}

// hkGetDocument reads one document; a key hk does not have is errHKNotFound.
func hkGetDocument(ctx context.Context, key string) (client.Document, error) {
	c, err := newHKClient()
	if err != nil {
		return client.Document{}, err
	}
	doc, found, err := c.GetDocument(ctx, key)
	if err == nil && !found {
		err = errHKNotFound
	}
	return doc, err
}

// hkUploadKinds are the document kinds that carry a file's bytes, which
// hkPutDocument sends under the upload bound.
var hkUploadKinds = map[string]bool{"brief": true, "report": true, "evidence": true}

// hkPutDocument writes doc over hk HTTP, as `handoffkeep doc put --key
// --kind [--job] --file` did: the key, kind, job and the file's bytes as the
// body. A brief, report or artifact (evidence) record gets the upload bound
// (hkUploadTimeout), any other document the call bound. An answer hk's
// client cannot decode is an error: hk's outcome is unknown, and the CLI
// failed on it too.
func hkPutDocument(ctx context.Context, doc client.Document) error {
	timeout := hkCallTimeout
	if hkUploadKinds[doc.Kind] {
		timeout = hkUploadTimeout
	}
	c, err := newHKClientWithin(timeout)
	if err != nil {
		return err
	}
	_, _, err = c.PutDocument(ctx, doc)
	return err
}

// hkAnswerBody runs one hk read and returns hk's 2xx answer as hk sent it,
// for a site that reads it with the parser it used on the hk CLI's output
// (the CLI printed hk's answer): an answer that parser read is read as it
// was, and one it could not read takes the branch it took for unparseable
// output, never success. Any other outcome — hk unreachable, an hk error,
// the credential unconfigured — is the call's error.
func hkAnswerBody(call func(client.Client) error) ([]byte, error) {
	body, answered, err := hkAnswer(call)
	if !answered {
		return nil, err
	}
	return body, nil
}

// hkTaskBody is GET /v1/tasks/{id}, the read behind `handoffkeep tasks show
// ID`; a task hk does not have is errHKNotFound.
func hkTaskBody(ctx context.Context, task string) ([]byte, error) {
	id, err := hkTaskID(task)
	if err != nil {
		return nil, err
	}
	return hkAnswerBody(func(c client.Client) error {
		_, found, e := c.GetTask(ctx, id)
		if e == nil && !found {
			e = errHKNotFound
		}
		return e
	})
}

// hkDocumentBody is GET /v1/documents/{key}, the read behind `handoffkeep
// doc get KEY`; a key hk does not have is errHKNotFound.
func hkDocumentBody(ctx context.Context, key string) ([]byte, error) {
	return hkAnswerBody(func(c client.Client) error {
		_, found, e := c.GetDocument(ctx, key)
		if e == nil && !found {
			e = errHKNotFound
		}
		return e
	})
}

// hkDocListLimit is the hk CLI's `doc list` page.
const hkDocListLimit = 100

// hkDocumentListBody is GET /v1/documents?prefix=, the read behind
// `handoffkeep doc list --prefix PREFIX`, with the CLI's limit. Its answer
// is {"documents": [...]}, where the CLI printed the array; the prefix is a
// LIKE pattern, so the caller still checks each key against it.
func hkDocumentListBody(ctx context.Context, prefix string) ([]byte, error) {
	return hkAnswerBody(func(c client.Client) error {
		_, e := c.ListDocuments(ctx, prefix, hkDocListLimit)
		return e
	})
}

// hkCommentTask appends a comment to the task over hk HTTP, as `handoffkeep
// tasks comment ID --body` did, under the call bound. hk checks the body as
// the CLI checked it before sending (non-empty, at most hk's comment limit,
// UTF-8, no NUL, no secret-like content).
func hkCommentTask(ctx context.Context, task, body string) error {
	id, err := hkTaskID(task)
	if err != nil {
		return err
	}
	c, err := newHKClient()
	if err != nil {
		return err
	}
	_, err = c.CommentTask(ctx, id, body)
	return err
}

// hkCheck is check(r, what) for an hk call that failed: the CLI's exit
// status and, where check printed the CLI's stderr, hk's error as the CLI
// printed it.
func hkCheck(err error, what string) error {
	if err == nil {
		return nil
	}
	return fail(hkCLIErrorExit, "%s: %s", what, hkErrText(err))
}

// PutHKDocument writes one document over hk HTTP with the svc-panewire
// credential (hkPutDocument): the job command's report upload and the quota
// stop manager's note, which ran `handoffkeep doc put`.
func PutHKDocument(ctx context.Context, key, kind, job, body string) error {
	return hkPutDocument(ctx, client.Document{Key: key, Kind: kind, Job: job, Body: body})
}

// HKDocumentAnswer is hk's 2xx answer to GET /v1/documents/{key}
// (hkDocumentBody), for the quota stop manager's policy read, which parses it
// as it parsed `handoffkeep doc get` output.
func HKDocumentAnswer(ctx context.Context, key string) ([]byte, error) {
	return hkDocumentBody(ctx, key)
}

// HKTaskListAnswer is hk's 2xx answer to GET /v1/tasks with no filter and
// limit tasks, the read behind `handoffkeep tasks list --limit N`, for the
// quota stop manager's task lookup, which parses it as it parsed that
// output.
func HKTaskListAnswer(ctx context.Context, limit int) ([]byte, error) {
	return hkAnswerBody(func(c client.Client) error {
		_, e := c.ListTasks(ctx, client.ListTasksInput{Limit: limit})
		return e
	})
}

// HKTaskAnswer is hk's 2xx answer to GET /v1/tasks/{id}, for callers that
// already know the task id and need the same answer body as `tasks show`.
func HKTaskAnswer(ctx context.Context, task string) ([]byte, error) {
	return hkTaskBody(ctx, task)
}
