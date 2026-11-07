package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func aaFixture(t *testing.T) aaPage {
	t.Helper()
	b, e := os.ReadFile("testdata/aa_models_v2.json")
	if e != nil {
		t.Fatal(e)
	}
	var p aaPage
	if e = json.Unmarshal(b, &p); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestAASyncMappingsAndGrades(t *testing.T) {
	p := aaFixture(t)
	seen := map[string]bool{}
	for _, m := range p.Data {
		if seen[m.ID] {
			t.Fatalf("duplicate fixture UUID %s", m.ID)
		}
		seen[m.ID] = true
	}
	var rows []map[string]any
	for k, id := range aaMappings {
		if !seen[id] {
			t.Errorf("missing mapped UUID %s", id)
		}
		rows = append(rows, map[string]any{"profile": k.Profile, "model_id": k.ModelID, "effort": k.Effort, "grade": "B", "score": 7.5, "gate": "default"})
	}
	at := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	result, n, missing, changed := aaAnnotate(rows, p.Data, 5, at)
	if n != len(rows) || len(missing) != 0 || changed != len(rows) {
		t.Fatalf("annotated=%d missing=%v changed=%d", n, missing, changed)
	}
	for i, row := range result {
		delete(row, "benchmark_source")
		delete(row, "benchmark_annotation")
		if !reflect.DeepEqual(row, rows[i]) {
			t.Fatal("non-benchmark fields changed")
		}
	}
	result, _, _, _ = aaAnnotate(rows, p.Data, 5, at)
	_, _, _, changed = aaAnnotate(result, p.Data, 5, at.Add(time.Minute))
	if changed != 0 {
		t.Fatal("timestamp counted as change")
	}
	_, _, _, changed = aaAnnotate(result, p.Data, 6, at)
	if changed != len(rows) {
		t.Fatal("version change missed")
	}
}
func TestAASyncHTTP(t *testing.T) {
	p := aaFixture(t)
	calls := 0
	secret := "fixture-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.URL.String(), secret) || r.Header.Get("x-api-key") != secret || r.Header.Get("Accept") != "application/json" || r.Header.Get("User-Agent") != "panewire" {
			t.Error("incorrect request")
		}
		page := r.URL.Query().Get("page")
		part := p
		if page == "1" {
			part.Data = p.Data[:1]
			more := true
			part.Pagination.HasMore = &more
		} else {
			part.Pagination.Page = 2
			part.Data = p.Data[1:]
			more := false
			part.Pagination.HasMore = &more
		}
		_ = json.NewEncoder(w).Encode(part)
	}))
	defer server.Close()
	a := aaSync{endpoint: server.URL, key: secret}
	models, version, e := a.fetch(context.Background())
	if e != nil || calls != 2 || len(models) != len(p.Data) || version != 5 || models[0].Performance.Speed == nil {
		t.Fatalf("fetch: calls=%d models=%d version=%d err=%v", calls, len(models), version, e)
	}
}
func TestAASyncHTTPErrors(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{{403, "tier does not allow this route"}, {429, "Retry-After 42"}, {302, "HTTP 302"}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			calls := 0
			secret := "fixture-key"
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Retry-After", "42")
				w.Header().Set("Location", "/other")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, secret)
			}))
			defer s.Close()
			_, _, e := (aaSync{endpoint: s.URL, key: secret}).fetch(context.Background())
			if e == nil || !strings.Contains(e.Error(), tc.want) || strings.Contains(e.Error(), secret) || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, e)
			}
		})
	}
}
func TestAASyncAnnotationGolden(t *testing.T) {
	p := aaFixture(t)
	rows := []map[string]any{{"profile": "codex-sol", "model_id": "gpt-6.1-sol", "effort": "low", "grade": "B"}, {"profile": "unknown", "model_id": "x", "effort": ""}}
	result, n, missing, _ := aaAnnotate(rows, p.Data, 5, time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC))
	if n != 1 || len(missing) != 1 {
		t.Fatal("unexpected mapping")
	}
	b, e := json.MarshalIndent(map[string]any{"catalog": result}, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	b = append(b, '\n')
	want, e := os.ReadFile("testdata/aa_annotation.golden")
	if e != nil {
		t.Fatal(e)
	}
	if string(b) != string(want) {
		t.Fatalf("artifact differs:\n%s", b)
	}
}

type aaTestTransport func(*http.Request) (*http.Response, error)

func (f aaTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestAASyncCommandArtifact(t *testing.T) {
	dir := t.TempDir()
	catalog := `{"catalog":[{"profile":"codex-sol","model_id":"gpt-6.1-sol","effort":"low","pool":"codex","grade":"B","gate":"default","decided_at":"2026-11-01T00:00:00Z","decided_by":"operator","decided_reason":"a & b <c> \u0061","extra": { "nested" : [ 1, 2 ] },"score":7.50,"benchmark_source":"old","benchmark_annotation":"old"},{"profile":"unknown","model_id":"x","effort":"","pool":"claude","grade":"A","gate":"escalation","decided_at":"2026-11-02T00:00:00Z","decided_by":"operator"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/bench/catalog" || r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		fmt.Fprint(w, catalog)
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CONFIG", filepath.Join(dir, "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv(aaKeyName, "fixture-key")
	fixture, e := os.ReadFile("testdata/aa_models_v2.json")
	if e != nil {
		t.Fatal(e)
	}
	old := http.DefaultTransport
	hkHost := strings.TrimPrefix(server.URL, "http://")
	http.DefaultTransport = aaTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == hkHost {
			return old.RoundTrip(r)
		}
		if r.Header.Get("x-api-key") != "fixture-key" || strings.Contains(r.URL.String(), "fixture-key") {
			t.Error("key not confined to header")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(fixture)), Request: r}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = old })
	path := filepath.Join(dir, "catalog.json")
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"aa-sync", "--out", path}, &out, &errs); code != 0 {
		t.Fatalf("code=%d error=%s", code, errs.String())
	}
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(b), "benchmark_annotation") || !strings.Contains(string(b), `"grade": "B"`) || strings.Contains(out.String()+errs.String()+string(b), "fixture-key") {
		t.Fatal("artifact or output incorrect")
	}
	var before, after struct {
		Catalog []map[string]json.RawMessage `json:"catalog"`
	}
	if json.Unmarshal([]byte(catalog), &before) != nil || json.Unmarshal(b, &after) != nil || len(before.Catalog) != len(after.Catalog) {
		t.Fatal("invalid catalog artifact")
	}
	for i, row := range before.Catalog {
		for field, value := range row {
			if field == "benchmark_source" || field == "benchmark_annotation" {
				continue
			}
			if !bytes.Equal(value, after.Catalog[i][field]) {
				t.Errorf("row %d field %s changed: %s -> %s", i, field, value, after.Catalog[i][field])
			}
		}
		for field := range after.Catalog[i] {
			if field != "benchmark_source" && field != "benchmark_annotation" {
				if _, ok := row[field]; !ok {
					t.Errorf("row %d added field %s", i, field)
				}
			}
		}
	}
	if bytes.Equal(before.Catalog[0]["benchmark_source"], after.Catalog[0]["benchmark_source"]) || bytes.Equal(before.Catalog[0]["benchmark_annotation"], after.Catalog[0]["benchmark_annotation"]) {
		t.Fatal("mapped benchmark fields did not change")
	}
	if !reflect.DeepEqual(before.Catalog[1], after.Catalog[1]) {
		t.Fatal("unmapped row changed")
	}
	if !strings.Contains(out.String(), "https://artificialanalysis.ai/") {
		t.Fatal("attribution missing")
	}
	os.Remove(path)
	out.Reset()
	errs.Reset()
	if code := runCatalogCLI([]string{"aa-sync", "--out", path, "--dry-run"}, &out, &errs); code != 0 {
		t.Fatalf("dry-run: %d %s", code, errs.String())
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("dry-run wrote artifact")
	}
}
func TestAASyncAmbiguity(t *testing.T) {
	p := aaFixture(t)
	row := map[string]any{"profile": "codex-sol", "model_id": "gpt-6.1-sol", "effort": "low", "benchmark_source": "old"}
	for _, m := range p.Data {
		if m.ID == aaMappings[aaRowKey{"codex-sol", "gpt-6.1-sol", "low"}] {
			p.Data = append(p.Data, m)
			break
		}
	}
	result, n, unmapped, _ := aaAnnotate([]map[string]any{row}, p.Data, 5, time.Now())
	if n != 0 || len(unmapped) != 1 || !reflect.DeepEqual(row, result[0]) {
		t.Fatal("ambiguous model was annotated")
	}
}

func TestAASyncOutRejectsFlag(t *testing.T) {
	t.Setenv(aaKeyName, "fixture-key")
	var out, errs bytes.Buffer
	if code := runCatalogCLI([]string{"aa-sync", "--out", "--dry-run"}, &out, &errs); code != ExitUsage {
		t.Fatalf("--out --dry-run: got exit %d, want usage exit %d", code, ExitUsage)
	}
}

func TestAASyncRejectsTrailingCatalogDataBeforeFetch(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"catalog":[{"profile":"p"}]}`+"\nwarning\n")
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CONFIG", filepath.Join(dir, "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv(aaKeyName, "fixture-key")
	old := http.DefaultTransport
	hkHost := strings.TrimPrefix(server.URL, "http://")
	aaCalls := 0
	http.DefaultTransport = aaTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == hkHost {
			return old.RoundTrip(r)
		}
		aaCalls++
		return nil, fmt.Errorf("unexpected request")
	})
	t.Cleanup(func() { http.DefaultTransport = old })
	var out, errs bytes.Buffer
	code := runCatalogCLI([]string{"aa-sync", "--out", filepath.Join(dir, "catalog.json")}, &out, &errs)
	if code != ExitInternal || aaCalls != 0 {
		t.Fatalf("exit=%d AA requests=%d; want internal error before fetch", code, aaCalls)
	}
}
