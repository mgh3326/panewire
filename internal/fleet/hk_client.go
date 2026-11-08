package fleet

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/mgh3326/handoffkeep/client"

	"github.com/mgh3326/panewire/internal/hkcred"
)

// newHKClient builds the hk public client the spawn path calls hk through
// (hk 3526 §5, hk 3591): the task precheck, claim, rebind and link check, the
// owner lookup, the routing policy read and the hub brief upload. The bearer
// is panewire's svc-panewire credential from hkcred.Load — the environment,
// else ~/.config/panewire/hk-svc.env — and never the hk CLI's config.env. The
// HTTP settings are those of panewire's other hk clients (catalog.go,
// fleet.go): the catalogTimeout per call, no redirect followed, HTTPS off
// loopback and the tailnet, and over plain HTTP no proxy, so an HTTP_PROXY
// never receives the bearer. An unconfigured credential is the
// *hkcred.UnconfiguredError Load returns; no error names the URL or a value.
func newHKClient() (client.Client, error) {
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
	h := &http.Client{Timeout: catalogTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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

// hkTaskIDIs reports whether an answered task id is the requested one; an
// answer without an id (0) is not checked, as the CLI path did not.
func hkTaskIDIs(t client.Task, task string) bool {
	return t.ID == 0 || strconv.FormatInt(t.ID, 10) == task
}

// hkGetTask reads one task; a task hk does not have is errHKNotFound.
func hkGetTask(ctx context.Context, task string) (client.Task, error) {
	id, err := hkTaskID(task)
	if err != nil {
		return client.Task{}, err
	}
	c, err := newHKClient()
	if err != nil {
		return client.Task{}, err
	}
	t, found, err := c.GetTask(ctx, id)
	if err == nil && !found {
		err = errHKNotFound
	}
	return t, err
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
