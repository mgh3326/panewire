package panewire

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

// hk 3617: a pane.capture that writes the screen also uploads it to hk as the
// evidence document screens/<job>/pane-screen.txt, linked to the job and its
// task, and answers the key in an additive `doc` field. A failed upload keeps
// the local screen and the `screen` outcome, with no doc.

const captureDocKey = "screens/job-a/pane-screen.txt"

// captureDocHK is a fake hk that stores every document PUT as hk does, or
// answers status/body when status is not 0.
func captureDocHK(t *testing.T, status int, body string) *hkSitesFake {
	t.Helper()
	return newHKSitesFake(t, func(r hkSitesRequest) (int, string) {
		if status != 0 {
			return status, body
		}
		var doc map[string]any
		if r.Method != http.MethodPut || json.Unmarshal(r.Body, &doc) != nil {
			return http.StatusNotFound, `{"error":"not_found"}`
		}
		out, _ := json.Marshal(map[string]any{"document": doc, "changed": true})
		return http.StatusOK, string(out)
	})
}

// captureScreen runs a writing pane.capture for job-a, whose job dir holds
// events, and returns the result and the saved screen path.
func captureScreen(t *testing.T, events ...map[string]any) (map[string]any, string) {
	t.Helper()
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
		t.Fatal(err)
	}
	markLostJobEvents(t, inbox, "job-a", events...)
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "job-a", "done", "/work/a"))}, read: "last screen\n결과\n"}
	args := map[string]any{"pane_id": "w1:p1", "name": "job-a", "cwd": "/work/a", "merged_after": 0, "write": true}
	result, code := nodeRun(t, &HubClient{jobsInboxRoot: inbox, nodeHerdr: fake.run}, nodeCommandPaneCapture, args)
	screen := filepath.Join(inbox, "jobs", "job-a", nodeCaptureScreenTxt)
	if code != "" || result["outcome"] != nodeCaptureScreen || result["saved"] != screen {
		t.Fatalf("result=%v code=%q, want outcome screen saved at %s", result, code, screen)
	}
	contents, err := os.ReadFile(screen)
	if err != nil || string(contents) != "last screen\n결과\n" {
		t.Fatalf("local screen=%q err=%v", contents, err)
	}
	return result, screen
}

func captureDocPut(t *testing.T, hk *hkSitesFake) map[string]any {
	t.Helper()
	for _, r := range hk.requests() {
		if r.Route() == http.MethodPut+" /v1/documents/"+url.PathEscape(captureDocKey) {
			return hkSitesJSON(t, r)
		}
	}
	t.Fatalf("hk routes=%q, want the screen document PUT", hk.routes())
	return nil
}

func TestNodePaneCaptureUploadsTheScreenDoc(t *testing.T) {
	claim := func(task any) map[string]any {
		return map[string]any{"kind": "job.claim", "payload": map[string]any{"task_id": task, "role": "worker"}}
	}
	spawned := map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w1:p1", "label": "job-a"}}
	for name, test := range map[string]struct {
		events []map[string]any
		task   any // the task_id the PUT carries; nil is none
	}{
		"string task":      {events: []map[string]any{claim("42"), spawned}, task: float64(42)},
		"number task":      {events: []map[string]any{claim(7), spawned}, task: float64(7)},
		"non-numeric task": {events: []map[string]any{claim("hk-abc"), spawned}},
		"no events":        {},
	} {
		t.Run(name, func(t *testing.T) {
			hk := captureDocHK(t, 0, "")
			result, screen := captureScreen(t, test.events...)
			if result["doc"] != captureDocKey {
				t.Fatalf("result=%v, want doc %s", result, captureDocKey)
			}
			put := captureDocPut(t, hk)
			contents, _ := os.ReadFile(screen)
			if put["key"] != captureDocKey || put["kind"] != "evidence" || put["job"] != "job-a" || put["origin_class"] != "job" || put["body"] != string(contents) {
				t.Fatalf("screen put=%v, want key %s, kind evidence, job job-a, origin_class job and the local screen as body", put, captureDocKey)
			}
			if got, linked := put["task_id"]; got != test.task || (test.task == nil && linked) {
				t.Fatalf("screen put task_id=%v (present %v), want %v", got, linked, test.task)
			}
		})
	}
}

func TestNodePaneCaptureKeepsTheScreenWhenTheUploadFails(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T){
		"hk refuses": func(t *testing.T) {
			captureDocHK(t, http.StatusBadRequest, `{"error":"invalid_context","reason":"secret-like content"}`)
		},
		"hk fails":        func(t *testing.T) { captureDocHK(t, http.StatusInternalServerError, `{"error":"internal_error"}`) },
		"hk unconfigured": unconfiguredHK,
	} {
		t.Run(name, func(t *testing.T) {
			setup(t)
			result, _ := captureScreen(t)
			if doc, _ := result["doc"].(string); doc != "" {
				t.Fatalf("result=%v, want no doc after a failed upload", result)
			}
		})
	}
}

// The hub re-encodes a node's pane.capture result from its documented
// shape: it carries `doc` for a written screen, and refuses a doc anywhere
// else or one that is not a screen document key.
func TestHubCarriesThePaneCaptureDoc(t *testing.T) {
	saved := "/inbox/jobs/job-a/pane-screen.txt"
	accepted := `{"type":"node.command.result","request_id":"cmd-1","op":"pane.capture","result":{"outcome":"screen","saved":"` + saved + `","doc":"` + captureDocKey + `"}}`
	result, ok := parseHubNodeCommandResult([]byte(accepted))
	if !ok {
		t.Fatalf("hub refused %s", accepted)
	}
	var carried map[string]any
	if err := json.Unmarshal(result.Result, &carried); err != nil || carried["doc"] != captureDocKey || carried["saved"] != saved || carried["outcome"] != nodeCaptureScreen {
		t.Fatalf("hub result=%s err=%v, want the doc carried", result.Result, err)
	}
	plain := `{"type":"node.command.result","request_id":"cmd-1","op":"pane.capture","result":{"outcome":"screen","saved":"` + saved + `"}}`
	if result, ok := parseHubNodeCommandResult([]byte(plain)); !ok || string(result.Result) != `{"outcome":"screen","saved":"`+saved+`"}` {
		t.Fatalf("hub result=%s ok=%v, want a result without doc unchanged", result.Result, ok)
	}
	for _, refused := range []string{
		`{"outcome":"report","saved":"/inbox/jobs/job-a/report.md","doc":"` + captureDocKey + `"}`,
		`{"outcome":"would-screen","saved":"` + saved + `","doc":"` + captureDocKey + `"}`,
		`{"outcome":"not-a-job","doc":"` + captureDocKey + `"}`,
		`{"outcome":"screen","saved":"` + saved + `","doc":"reports/job-a/report.md"}`,
		`{"outcome":"screen","saved":"` + saved + `","doc":"screens/../pane-screen.txt"}`,
		`{"outcome":"screen","saved":"` + saved + `","doc":"screens/job-b/pane-screen.txt"}`,
		`{"outcome":"screen","saved":"` + saved + `","doc":7}`,
	} {
		payload := `{"type":"node.command.result","request_id":"cmd-1","op":"pane.capture","result":` + refused + `}`
		if _, ok := parseHubNodeCommandResult([]byte(payload)); ok {
			t.Fatalf("hub accepted %s", payload)
		}
	}
}
