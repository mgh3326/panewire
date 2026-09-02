package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func r15ContextHub(t *testing.T) (*ContextStore, *httptest.Server) {
	t.Helper()
	store, err := OpenContextStore(r15TestDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken}, ContextStore: store})
	if err != nil {
		t.Fatal(err)
	}
	return store, httptest.NewServer(hub.Handler())
}

func r15TestDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("PANEWIRE_TEST_DB_URL")
	if url == "" {
		t.Skip("PANEWIRE_TEST_DB_URL is required for PostgreSQL context integration tests")
	}
	return url
}
func r15Request(t *testing.T, client *http.Client, method, url, token string, value any) *http.Response {
	t.Helper()
	var body *bytes.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
func TestContextR15APIAuthPersistenceAndLimits(t *testing.T) {
	store, server := r15ContextHub(t)
	defer server.Close()
	defer store.Close()
	response := r15Request(t, server.Client(), http.MethodPost, server.URL+"/v1/context/checkpoints", "", map[string]any{"session": "s", "kind": "checkpoint", "title": "x", "body": "x"})
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauth status=%d", response.StatusCode)
	}
	response.Body.Close()
	response = r15Request(t, server.Client(), http.MethodPost, server.URL+"/v1/context/checkpoints", r6NodeAToken, map[string]any{"session": "s", "kind": "checkpoint", "title": "first", "body": "body"})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("machine write status=%d", response.StatusCode)
	}
	response.Body.Close()
	response = r15Request(t, server.Client(), http.MethodGet, server.URL+"/v1/context/checkpoints?session=s&limit=3", r6OperatorToken, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("operator read status=%d", response.StatusCode)
	}
	var result struct {
		Checkpoints []ContextCheckpoint `json:"checkpoints"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(result.Checkpoints) != 1 || result.Checkpoints[0].CreatedBy != "node-a" {
		t.Fatalf("checkpoint=%+v", result.Checkpoints)
	}
	for i := 0; i < contextCheckpointKeep+2; i++ {
		_, err := store.CreateCheckpoint(t.Context(), ContextCheckpoint{Session: "keep", Kind: "checkpoint", Title: "x", Body: "x", CreatedBy: "node-a"})
		if err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.RecentCheckpoints(t.Context(), "keep", "", 1000)
	if err != nil || len(items) != contextCheckpointKeep {
		t.Fatalf("checkpoint cap got=%d err=%v", len(items), err)
	}
	for i := 0; i < contextMemoryKeep+2; i++ {
		_, err := store.PutMemory(t.Context(), ContextMemory{Agent: "agent", Name: fmtName(i), MemoryType: "project", Content: "x", UpdatedBy: "node-a"})
		if err != nil {
			t.Fatal(err)
		}
	}
	memory, err := store.ListMemory(t.Context(), "agent", true)
	if err != nil || len(memory) != contextMemoryKeep {
		t.Fatalf("memory cap got=%d err=%v", len(memory), err)
	}
}
func fmtName(i int) string { return "m" + strconv.Itoa(i) }
func TestContextR15SecretGuardAndTailnetValidation(t *testing.T) {
	store, server := r15ContextHub(t)
	defer server.Close()
	defer store.Close()
	for _, content := range []string{"sk-abcdefghijk", "ghp_abcdefghijk", "xoxb-abcdefghijk", "AKIAABCDEFGH", "-----BEGIN PRIVATE", "Bearer abcdefghijklmnopqrstuvwxyz", "MY_TOKEN=abcdefghijklm", "github_pat_abcdefghijk"} {
		response := r15Request(t, server.Client(), http.MethodPost, server.URL+"/v1/context/checkpoints", r6NodeAToken, map[string]any{"session": "s", "kind": "checkpoint", "title": "x", "body": content})
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("secret %q status=%d", content, response.StatusCode)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if !strings.Contains(string(body), "secret_like_content") || strings.Contains(string(body), content) {
			t.Fatalf("secret response leaked content: %s", body)
		}
	}
	for _, address := range []string{"0.0.0.0:9377", "8.8.8.8:9377", "[::]:9377"} {
		if _, err := hubTailnetListenAddress(address); err == nil {
			t.Fatalf("accepted unsafe address %s", address)
		}
	}
	if _, err := hubTailnetListenAddress("100.122.100.56:9377"); err != nil {
		t.Fatal(err)
	}
}
func TestContextR15MemoryPushPullRoundTrip(t *testing.T) {
	store, server := r15ContextHub(t)
	defer server.Close()
	defer store.Close()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	raw := "---\nname: one\ndescription: retained description\nmetadata:\n  type: reference\n---\ncontent\n"
	if err := os.WriteFile(filepath.Join(source, "one.md"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	env := filepath.Join(root, "node.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := runMemoryPush([]string{"--hub-url", server.URL, "--hub-token-env", env, "--agent", "agent", "--dir", source, "--apply"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK {
		t.Fatalf("push code=%d stderr=%s", code, errOut.String())
	}
	destination := filepath.Join(root, "destination")
	out.Reset()
	errOut.Reset()
	if code := runMemoryPull([]string{"--hub-url", server.URL, "--hub-token-env", env, "--agent", "agent", "--dir", destination, "--apply"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK {
		t.Fatalf("pull code=%d stderr=%s", code, errOut.String())
	}
	got, err := os.ReadFile(filepath.Join(destination, "one.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != raw {
		t.Fatalf("round trip got %q want %q", got, raw)
	}
}

func TestContextR15StoreSurvivesRestart(t *testing.T) {
	first, err := OpenContextStore(r15TestDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.CreateCheckpoint(t.Context(), ContextCheckpoint{Session: "restart", Kind: "handoff", Title: "persist", Body: "body", CreatedBy: "node-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.PutMemory(t.Context(), ContextMemory{Agent: "agent", Name: "persist", MemoryType: "project", Content: "content", UpdatedBy: "node-a"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenContextStore(r15TestDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	checkpoints, err := second.RecentCheckpoints(t.Context(), "restart", "", 3)
	if err != nil || len(checkpoints) != 1 {
		t.Fatalf("checkpoint persistence: count=%d err=%v", len(checkpoints), err)
	}
	memory, found, err := second.GetMemory(t.Context(), "agent", "persist")
	if err != nil || !found || memory.Content != "content" {
		t.Fatalf("memory persistence: found=%v item=%+v err=%v", found, memory, err)
	}
}

func TestContextR15SearchDocumentsAndImport(t *testing.T) {
	store, server := r15ContextHub(t)
	defer server.Close()
	defer store.Close()
	var extension string
	if err := store.db.QueryRowContext(t.Context(), `SELECT extname FROM pg_extension WHERE extname='pg_trgm'`).Scan(&extension); err != nil || extension != "pg_trgm" {
		t.Fatalf("pg_trgm extension=%q err=%v", extension, err)
	}
	response := r15Request(t, server.Client(), http.MethodPost, server.URL+"/v1/context/checkpoints", r6NodeAToken, map[string]any{"session": "search-session", "kind": "decision", "title": "한국어 배포 결정", "body": "트라이그램 검색 검증 본문"})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("checkpoint status=%d", response.StatusCode)
	}
	response.Body.Close()
	response = r15Request(t, server.Client(), http.MethodPut, server.URL+"/v1/context/docs/jobs/search-job/brief.md", r6NodeAToken, map[string]any{"kind": "brief", "session": "search-session", "job": "search-job", "body": "한국어 문서 검색 본문"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("document put status=%d", response.StatusCode)
	}
	response.Body.Close()
	response = r15Request(t, server.Client(), http.MethodGet, server.URL+"/v1/context/search?q=%ED%95%9C%EA%B5%AD%EC%96%B4&scope=all&session=search-session", r6OperatorToken, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("search status=%d", response.StatusCode)
	}
	var search struct {
		Results []ContextSearchResult `json:"results"`
	}
	if err := json.NewDecoder(response.Body).Decode(&search); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(search.Results) < 2 {
		t.Fatalf("search results=%+v", search.Results)
	}
	response = r15Request(t, server.Client(), http.MethodPut, server.URL+"/v1/context/docs/jobs/search-job/secret.md", r6NodeAToken, map[string]any{"kind": "note", "body": "sk-abcdefghijk"})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("document secret status=%d", response.StatusCode)
	}
	response.Body.Close()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "jobs", "import-job"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "jobs", "import-job", "brief.md"), []byte("brief body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "jobs", "import-job", "report.md"), []byte("report body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "jobs", "import-job", "answer-1.md"), []byte("answer body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "jobs", "import-job", "bad.md"), []byte("ghp_abcdefghijk"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := runDocumentImport([]string{"--dir", root}, &out, &errOut, hubCLIDeps{}); code != ExitOK || !strings.Contains(out.String(), "files=4") || !strings.Contains(out.String(), "rejected jobs/import-job/bad.md") {
		t.Fatalf("dry import code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	env := filepath.Join(root, "node.env")
	if err := os.WriteFile(env, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := runDocumentImport([]string{"--dir", root, "--hub-url", server.URL, "--hub-token-env", env, "--apply"}, &out, &errOut, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK || !strings.Contains(out.String(), "stored=3") || !strings.Contains(out.String(), "rejected=1") {
		t.Fatalf("apply import code=%d out=%q err=%q", code, out.String(), errOut.String())
	}
	response = r15Request(t, server.Client(), http.MethodGet, server.URL+"/v1/context/docs?prefix=jobs/import-job/", r6OperatorToken, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("document list status=%d", response.StatusCode)
	}
	var listed struct {
		Documents []ContextDocument `json:"documents"`
	}
	if err := json.NewDecoder(response.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(listed.Documents) != 3 {
		t.Fatalf("listed documents=%+v", listed.Documents)
	}
}
