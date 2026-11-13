package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func catalogTestEnv(t *testing.T, url string) {
	t.Helper()
	t.Setenv("HANDOFFKEEP_URL", url)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
}

type catalogRecorder struct {
	mu     sync.Mutex
	path   string
	method string
	body   []byte
	auth   string
}

func (r *catalogRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.path, r.method, r.body, r.auth = req.URL.RequestURI(), req.Method, body, req.Header.Get("Authorization")
	r.mu.Unlock()
	_, _ = io.WriteString(w, `{"upserted":1}`)
}

func catalogCatalogServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/bench/catalog" || r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func writeCatalogFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The push fixture from pacelever's bench_test.go: the hk request is byte for
// byte the payload pacelever sends, and local fields never reach it.
func TestCatalogPushMatchesPaceleverPayload(t *testing.T) {
	recorder := &catalogRecorder{}
	server := httptest.NewServer(recorder)
	t.Cleanup(server.Close)
	catalogTestEnv(t, server.URL)
	path := writeCatalogFile(t, `{"catalog":[{"profile":"example","effort":"high","model_id":"example-model","pool":"claude","grade":"A","gate":"default","placed":true,"family":"anthropic","billing":"subscription","group":"gemini","score":null,"gate_reason":null,"retired_at":null,"boundary_version":"","deviation_ref":"hk:task/1","decided_at":"2026-10-28T00:00:00Z","decided_by":"operator"}]}`)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"push", path}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	want := `{"catalog":[{"boundary_version":"","decided_at":"2026-10-28T00:00:00Z","decided_by":"operator","deviation_ref":"hk:task/1","effort":"high","gate":"default","gate_reason":null,"grade":"A","model_id":"example-model","pool":"claude","profile":"example","retired_at":null,"score":null}]}`
	if recorder.method != http.MethodPut || recorder.path != "/v1/bench/catalog" || recorder.auth != "Bearer test-token" {
		t.Fatalf("request=%s %s auth=%q", recorder.method, recorder.path, recorder.auth)
	}
	if string(recorder.body) != want {
		t.Fatalf("payload differs:\n got %s\nwant %s", recorder.body, want)
	}
	if out.Len() != 0 {
		t.Fatalf("push printed %q", out.String())
	}
}

func TestGradeSSCatalogPushDryRun(t *testing.T) {
	path := writeCatalogFile(t, `{"catalog":[{"profile":"example","effort":"high","model_id":"example-model","pool":"claude","grade":"SS","gate":"default","deviation_ref":"hk:task/4044","decided_at":"2026-10-28T00:00:00Z","decided_by":"operator"}]}`)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"push", path, "--dry-run"}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	want := `{
  "catalog": [
    {
      "boundary_version": "",
      "decided_at": "2026-10-28T00:00:00Z",
      "decided_by": "operator",
      "deviation_ref": "hk:task/4044",
      "effort": "high",
      "gate": "default",
      "gate_reason": null,
      "grade": "SS",
      "model_id": "example-model",
      "pool": "claude",
      "profile": "example",
      "retired_at": null,
      "score": null
    }
  ]
}
`
	if out.String() != want {
		t.Fatalf("dry-run output differs:\n%s", out.String())
	}
}

func TestCatalogPushRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"empty", `{"catalog":[]}`, "non-empty catalog required"},
		{"grade", `{"catalog":[{"profile":"p","effort":"high","model_id":"m","pool":"claude","grade":"Z","gate":"default","deviation_ref":"hk:task/1","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator"}]}`, `invalid catalog grade "Z"`},
		{"provenance", `{"catalog":[{"profile":"p","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default","decided_at":"2026-11-01T00:00:00Z"}]}`, "catalog provenance required"},
		{"decided_at", `{"catalog":[{"profile":"p","effort":"high","model_id":"m","pool":"claude","grade":"A","gate":"default","deviation_ref":"hk:task/1","decided_at":"2026-11-01 00:00:00","decided_by":"operator"}]}`, "catalog decided_at must include timezone"},
		{"monotonic", `{"catalog":[{"profile":"p","effort":"low","model_id":"m","pool":"claude","grade":"A","gate":"default","deviation_ref":"hk:task/1","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator"},{"profile":"p","effort":"high","model_id":"m","pool":"claude","grade":"C","gate":"default","deviation_ref":"hk:task/1","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator"}]}`, "not monotonic for p"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeCatalogFile(t, tc.body)
			var out, errs bytes.Buffer
			if code := runCatalogCLI([]string{"push", path}, &out, &errs); code != ExitConditionInvalid || !strings.Contains(errs.String(), tc.want) {
				t.Fatalf("exit=%d stderr=%q want %q", code, errs.String(), tc.want)
			}
		})
	}
}

func TestCatalogListTextMatchesPaceleverFormat(t *testing.T) {
	body := `{"catalog":[
 {"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S+","gate":"default"},
 {"profile":"codex-luna","effort":"low","model_id":"gpt-5.6-luna","pool":"codex","grade":"C","gate":"default","benchmark_source":"artificialanalysis:v2:2026-11-02","benchmark_annotation":"{\"intelligence_index\":null,\"coding_index\":null,\"price_1m_input_tokens\":0.2,\"price_1m_output_tokens\":1.2,\"median_output_tokens_per_second\":null,\"aa_name\":\"GPT-5.6 Luna (Low)\",\"fetched_at\":\"2026-11-02T05:00:00Z\"}"}
]}`
	server := catalogCatalogServer(t, body)
	catalogTestEnv(t, server.URL)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"list"}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	want := "S+  opus@high model=claude-opus-5-5 pool=claude gate=default\n" +
		"C   codex-luna@low model=gpt-5.6-luna pool=codex gate=default benchmark_source=artificialanalysis:v2:2026-11-02 aa_name=\"GPT-5.6 Luna (Low)\" intelligence_index=n/a coding_index=n/a price_1m_input_tokens=0.2 price_1m_output_tokens=1.2 output_tok_s=n/a fetched_at=2026-11-02T05:00:00Z\n" +
		"catalog=server stale=false\n"
	if out.String() != want {
		t.Fatalf("list output differs:\n got %q\nwant %q", out.String(), want)
	}
}

func TestCatalogListJSON(t *testing.T) {
	body := `{"catalog":[{"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S+","gate":"default","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator","deviation_ref":"hk:task/1"}]}`
	server := catalogCatalogServer(t, body)
	catalogTestEnv(t, server.URL)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"list", "--json"}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	var doc struct {
		Catalog []map[string]any `json:"catalog"`
		Source  map[string]any   `json:"source"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || len(doc.Catalog) != 1 {
		t.Fatalf("list JSON: %v\n%s", err, out.String())
	}
	if doc.Source["source"] != "server" || doc.Source["stale"] != false {
		t.Fatalf("source = %v", doc.Source)
	}
	for _, local := range []string{"placed", "family", "billing", "group"} {
		if _, ok := doc.Catalog[0][local]; ok {
			t.Fatalf("local field %s reached list output", local)
		}
	}
}

func TestCatalogListWarnsUnknownPool(t *testing.T) {
	body := `{"catalog":[
 {"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"S+","gate":"default"},
 {"profile":"mystery","effort":"high","model_id":"m","pool":"upstage","grade":"A","gate":"default"}
]}`
	server := catalogCatalogServer(t, body)
	catalogTestEnv(t, server.URL)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"list"}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	if !strings.Contains(errs.String(), "warning: unknown pool [upstage] ignored") {
		t.Fatalf("warning missing: %q", errs.String())
	}
	if strings.Contains(out.String(), "mystery") {
		t.Fatalf("unknown pool row listed: %s", out.String())
	}
}

// The svc-panewire file: PANEWIRE_HK_ENV names the mode-0600 env file (hk 3570).
func TestCatalogCredentialsFromConfigFile(t *testing.T) {
	server := catalogCatalogServer(t, `{"catalog":[{"profile":"opus","effort":"high","model_id":"m","pool":"claude","grade":"S+","gate":"default"}]}`)
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	env := writeMode0600(t, t.TempDir(), "hk.env", "HANDOFFKEEP_URL="+server.URL+"\nHANDOFFKEEP_TOKEN_SVC_PANEWIRE=test-token\n")
	t.Setenv("PANEWIRE_HK_ENV", env)
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"list"}, &out, &errs); code != ExitOK {
		t.Fatalf("exit=%d stderr=%s", code, errs.String())
	}
	if strings.Contains(out.String()+errs.String(), "test-token") {
		t.Fatal("credential reached output")
	}
	if !strings.Contains(out.String(), "catalog=server stale=false") {
		t.Fatalf("list output: %s", out.String())
	}
}

func TestCatalogGradeCommandRemoved(t *testing.T) {
	for _, args := range [][]string{
		{"grade"},
		{"grade", "set", "--profile", "example", "--grade", "A", "--deviation-ref", "hk:task/1"},
	} {
		var out, errs bytes.Buffer
		if code := runCatalogCLI(args, &out, &errs); code != ExitUsage || !strings.Contains(errs.String(), "usage:") {
			t.Fatalf("args=%v exit=%d stderr=%q", args, code, errs.String())
		}
		if out.Len() != 0 {
			t.Fatalf("args=%v wrote output: %q", args, out.String())
		}
	}
}

func TestCatalogUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"nope"}, {"grade"}, {"grade", "list"}, {"push"}} {
		var out, errs bytes.Buffer
		if code := runCatalogCLI(args, &out, &errs); code != ExitUsage {
			t.Fatalf("%v: exit=%d", args, code)
		}
	}
}
