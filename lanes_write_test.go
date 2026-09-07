package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func lanesWriteHub(t *testing.T, path string) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "fixture-operator-token", "machine-a": "fixture-a-token", "machine-b": "fixture-b-token"},
		ReportRelayPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func lanesWriteRequest(t *testing.T, hub *HubServer, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func lanesWriteSubscriber(t *testing.T, hub *HubServer) *hubEventSubscriber {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 16)}
	hub.mu.Lock()
	hub.subscribers[subscriber] = struct{}{}
	hub.mu.Unlock()
	t.Cleanup(func() {
		cancel()
		hub.mu.Lock()
		delete(hub.subscribers, subscriber)
		hub.mu.Unlock()
	})
	return subscriber
}

func lanesWriteEvent(t *testing.T, subscriber *hubEventSubscriber) hubEvent {
	t.Helper()
	select {
	case message := <-subscriber.messages:
		if message.event == nil {
			t.Fatalf("received non-event subscription message: %+v", message)
		}
		return *message.event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lanes event")
		return hubEvent{}
	}
}

func decodeLaneProjection(t *testing.T, body []byte) hubLaneProjection {
	t.Helper()
	var result hubLaneProjection
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("response=%q err=%v", body, err)
	}
	return result
}

func TestLanesWriteCreateUpdateDeleteAndEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-parent":{"machine":"machine-a","pane":"w1:p2"}}}`)
	hub := lanesWriteHub(t, path)
	subscriber := lanesWriteSubscriber(t, hub)

	create := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1","parent":"lane-parent"}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	if got := decodeLaneProjection(t, create.Body.Bytes()); got != (hubLaneProjection{Lane: "lane-a", Machine: "machine-a", Pane: "w1:p1", Parent: "lane-parent"}) {
		t.Fatalf("create projection=%+v", got)
	}
	if strings.Contains(create.Body.String(), "protected") || strings.Contains(create.Body.String(), "deliver") {
		t.Fatalf("write response exposed internal fields: %s", create.Body.String())
	}

	update := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p3"}`)
	if update.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", update.Code, update.Body.String())
	}
	if got := decodeLaneProjection(t, update.Body.Bytes()); got != (hubLaneProjection{Lane: "lane-a", Machine: "machine-a", Pane: "w1:p3"}) {
		t.Fatalf("update projection=%+v", got)
	}

	remove := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-a", "fixture-operator-token", "")
	if remove.Code != http.StatusOK || remove.Body.String() != `{"lane":"lane-a","removed":true}`+"\n" {
		t.Fatalf("remove status=%d body=%q", remove.Code, remove.Body.String())
	}
	if got := lanesProjectionResponse(t, lanesProjectionGet(hub, "fixture-operator-token")); len(got) != 1 || got[0]["lane"] != "lane-parent" {
		t.Fatalf("GET after remove=%v", got)
	}

	for _, want := range []struct {
		lane string
		op   string
	}{
		{lane: "lane-a", op: "add"},
		{lane: "lane-a", op: "update"},
		{lane: "lane-a", op: "remove"},
	} {
		event := lanesWriteEvent(t, subscriber)
		if event.Kind != "lanes.changed" {
			t.Fatalf("event kind=%q", event.Kind)
		}
		var payload map[string]string
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(payload, map[string]string{"lane": want.lane, "op": want.op}) {
			t.Fatalf("event payload=%v want lane=%s op=%s", payload, want.lane, want.op)
		}
	}
}

func TestLanesWriteSinkNormalizationAndStrictValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	hub := lanesWriteHub(t, path)
	createSink := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-sink", "fixture-operator-token", `{"sink":true}`)
	if createSink.Code != http.StatusCreated {
		t.Fatalf("sink status=%d body=%s", createSink.Code, createSink.Body.String())
	}
	if got := decodeLaneProjection(t, createSink.Body.Bytes()); got != (hubLaneProjection{Lane: "lane-sink", Sink: true}) {
		t.Fatalf("sink projection=%+v", got)
	}

	valid := `{"machine":"machine-a","pane":"w1:p1"}`
	for _, test := range []struct {
		name string
		path string
		body string
	}{
		{name: "bad lane", path: "/v1/lanes/Lane-a", body: valid},
		{name: "bad machine", path: "/v1/lanes/lane-b", body: `{"machine":"unknown-machine","pane":"w1:p1"}`},
		{name: "operator machine", path: "/v1/lanes/lane-c", body: `{"machine":"operator","pane":"w1:p1"}`},
		{name: "bad pane", path: "/v1/lanes/lane-d", body: `{"machine":"machine-a","pane":"pane-a"}`},
		{name: "missing parent", path: "/v1/lanes/lane-e", body: `{"machine":"machine-a","pane":"w1:p1","parent":"missing"}`},
		{name: "self parent", path: "/v1/lanes/lane-f", body: `{"machine":"machine-a","pane":"w1:p1","parent":"lane-f"}`},
		{name: "unknown field", path: "/v1/lanes/lane-g", body: `{"machine":"machine-a","pane":"w1:p1","secret":"nope"}`},
		{name: "trailing body", path: "/v1/lanes/lane-h", body: valid + ` {}`},
		{name: "null parent", path: "/v1/lanes/lane-i", body: `{"machine":"machine-a","pane":"w1:p1","parent":null}`},
		{name: "oversized body", path: "/v1/lanes/lane-j", body: `{"machine":"machine-a","pane":"` + strings.Repeat("a", lanesWriteRequestMaxBytes) + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := lanesWriteRequest(t, hub, http.MethodPut, test.path, "fixture-operator-token", test.body)
			if writer.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
		})
	}
	oversizedPane := `{"machine":"machine-a","pane":"` + strings.Repeat("a", 129) + `"}`
	if writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-long", "fixture-operator-token", oversizedPane); writer.Code != http.StatusBadRequest {
		t.Fatalf("oversized pane status=%d body=%s", writer.Code, writer.Body.String())
	}
}

func TestLanesWriteAuthenticationAndProtectedDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	original := `{"lanes":{"lane-protected":{"machine":"machine-a","pane":"w1:p1","protected":true}}}`
	lanesProjectionWrite(t, path, original)
	hub := lanesWriteHub(t, path)
	for _, token := range []string{"", "wrong-token", "fixture-a-token"} {
		writer := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-protected", token, "")
		if writer.Code != http.StatusUnauthorized || writer.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("token=%q status=%d auth=%q body=%s", token, writer.Code, writer.Header().Get("WWW-Authenticate"), writer.Body.String())
		}
	}
	subscriber := lanesWriteSubscriber(t, hub)
	writer := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-protected", "fixture-operator-token", "")
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_protected"}`+"\n" {
		t.Fatalf("protected status=%d body=%q", writer.Code, writer.Body.String())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != original {
		t.Fatalf("protected delete changed original from %q to %q", original, contents)
	}
	select {
	case message := <-subscriber.messages:
		t.Fatalf("protected delete broadcast event: %+v", message)
	case <-time.After(50 * time.Millisecond):
	}

	missing := lanesWriteRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-missing", "fixture-operator-token", "")
	if missing.Code != http.StatusNotFound || missing.Body.String() != `{"error":"lane_not_found"}`+"\n" {
		t.Fatalf("missing delete status=%d body=%q", missing.Code, missing.Body.String())
	}
}

func TestLanesWriteConcurrentAddsPreserveBothRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{}}`)
	hub := lanesWriteHub(t, path)
	handler := hub.Handler()
	results := make(chan int, 2)
	var wait sync.WaitGroup
	for _, lane := range []string{"lane-a", "lane-b"} {
		lane := lane
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPut, "/v1/lanes/"+lane, strings.NewReader(`{"machine":"machine-a","pane":"w1:p1"}`))
			request.Header.Set(hubAuthorizationHeader, "Bearer fixture-operator-token")
			writer := httptest.NewRecorder()
			handler.ServeHTTP(writer, request)
			results <- writer.Code
		}()
	}
	wait.Wait()
	close(results)
	for code := range results {
		if code != http.StatusCreated {
			t.Fatalf("concurrent add status=%d", code)
		}
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var routes reportRelayRoutes
	if err := json.Unmarshal(contents, &routes); err != nil {
		t.Fatalf("final lanes JSON=%q err=%v", contents, err)
	}
	if len(routes.Lanes) != 2 || routes.Lanes["lane-a"].Pane != "w1:p1" || routes.Lanes["lane-b"].Pane != "w1:p1" {
		t.Fatalf("final routes=%+v", routes.Lanes)
	}
}

func TestLanesWriteWaitsForTheOSFileLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{}}`)
	hub := lanesWriteHub(t, path)
	handler := hub.Handler()
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPut, "/v1/lanes/lane-a", strings.NewReader(`{"machine":"machine-a","pane":"w1:p1"}`))
		request.Header.Set(hubAuthorizationHeader, "Bearer fixture-operator-token")
		writer := httptest.NewRecorder()
		handler.ServeHTTP(writer, request)
		done <- writer
	}()
	select {
	case writer := <-done:
		t.Fatalf("write completed while OS lock was held: status=%d body=%s", writer.Code, writer.Body.String())
	case <-time.After(100 * time.Millisecond):
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case writer := <-done:
		if writer.Code != http.StatusCreated {
			t.Fatalf("unlocked write status=%d body=%s", writer.Code, writer.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("unlocked write did not complete")
	}
}

func TestLanesWriteBackupsKeepTenNewestAndPreserveBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	original := []byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 12; index++ {
		name := filepath.Join(filepath.Dir(path), filepath.Base(path)+fmt.Sprintf(".bak-20200101T000000.000000000Z-%02d", index))
		if err := os.WriteFile(name, []byte("old"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	hub := lanesWriteHub(t, path)
	hub.now = func() time.Time { return time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC) }
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p2"}`)
	if writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	backups, err := filepath.Glob(path + ".bak-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != lanesBackupsKept {
		t.Fatalf("backup count=%d names=%v", len(backups), backups)
	}
	foundOriginal := false
	for _, backup := range backups {
		contents, err := os.ReadFile(backup)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(contents, original) {
			foundOriginal = true
		}
	}
	if !foundOriginal {
		t.Fatalf("no backup preserved original bytes")
	}
}

func TestLanesWriteMalformedAndOversizedOriginalRemainByteIdentical(t *testing.T) {
	for _, test := range []struct {
		name string
		body []byte
	}{
		{name: "malformed", body: []byte(`{"lanes":`)},
		{name: "oversized", body: []byte(strings.Repeat("x", lanesFileMaxBytes+1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			if err := os.WriteFile(path, test.body, 0600); err != nil {
				t.Fatal(err)
			}
			hub := lanesWriteHub(t, path)
			for _, method := range []string{http.MethodPut, http.MethodDelete} {
				var writer *httptest.ResponseRecorder
				if method == http.MethodPut {
					writer = lanesWriteRequest(t, hub, method, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1"}`)
				} else {
					writer = lanesWriteRequest(t, hub, method, "/v1/lanes/lane-a", "fixture-operator-token", "")
				}
				if writer.Code != http.StatusInternalServerError {
					t.Fatalf("method=%s status=%d body=%s", method, writer.Code, writer.Body.String())
				}
				contents, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(contents, test.body) {
					t.Fatalf("method=%s changed original bytes", method)
				}
			}
		})
	}
}

func TestLanesWriteLegacyRoutesAndProtectedProjectionRemainCompatible(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"routes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","deliver":"idle","protected":true}}}`)
	hub := lanesWriteHub(t, path)
	get := lanesProjectionGet(hub, "fixture-operator-token")
	if get.Code != http.StatusOK || strings.Contains(get.Body.String(), "protected") || strings.Contains(get.Body.String(), "deliver") {
		t.Fatalf("legacy GET status=%d body=%s", get.Code, get.Body.String())
	}
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p2"}`)
	if writer.Code != http.StatusCreated {
		t.Fatalf("legacy update status=%d body=%s", writer.Code, writer.Body.String())
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(updated), `"protected": true`) || !strings.Contains(string(updated), `"deliver": "idle"`) {
		t.Fatalf("internal route fields were not preserved: %s", updated)
	}
}

func TestLanesWriteCLIParsesLaneBeforeFlagsAndUsesOperatorAuth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	hub := lanesWriteHub(t, path)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	envPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN=fixture-operator-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	deps := hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}
	var stdout, stderr strings.Builder
	if code := runLanesCLI([]string{"add", "lane-a", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps); code != ExitOK || stderr.Len() != 0 {
		t.Fatalf("add code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := decodeLaneProjection(t, []byte(stdout.String())); got.Lane != "lane-a" || got.Machine != "machine-a" || got.Pane != "w1:p1" {
		t.Fatalf("CLI add output=%+v", got)
	}
	stdout.Reset()
	if code := runLanesCLI([]string{"ls", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps); code != ExitOK || !strings.Contains(stdout.String(), `"lane":"lane-a"`) {
		t.Fatalf("ls code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if code := runLanesCLI([]string{"rm", "lane-a", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, deps); code != ExitOK || stdout.String() != `{"lane":"lane-a","removed":true}`+"\n" {
		t.Fatalf("rm code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "fixture-operator-token") {
		t.Fatal("CLI output exposed operator token")
	}

	cfEnvPath := filepath.Join(t.TempDir(), "cf.env")
	if err := os.WriteFile(cfEnvPath, []byte("CF_ACCESS_CLIENT_ID=fixture-client\nCF_ACCESS_CLIENT_SECRET=fixture-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var sawCFHeaders bool
	cfServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		sawCFHeaders = request.Header.Get("CF-Access-Client-Id") == "fixture-client" && request.Header.Get("CF-Access-Client-Secret") == "fixture-secret"
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"lanes":[]}`))
	}))
	defer cfServer.Close()
	stdout.Reset()
	stderr.Reset()
	if code := runLanesCLI([]string{"ls", "--hub-url", cfServer.URL, "--hub-token-env", envPath, "--hub-cf-env", cfEnvPath}, &stdout, &stderr, deps); code != ExitOK || !sawCFHeaders {
		t.Fatalf("CF ls code=%d headers=%t stdout=%q stderr=%q", code, sawCFHeaders, stdout.String(), stderr.String())
	}
	if code := runLanesCLI([]string{"ls", "--hub-url", cfServer.URL, "--hub-token-env", envPath}, &stdout, &stderr, hubCLIDeps{HTTPClient: cfServer.Client()}); code != ExitConditionInvalid {
		t.Fatalf("insecure URL code=%d want=%d", code, ExitConditionInvalid)
	}
}

func TestLanesWriteTempFailureDoesNotChangeOriginal(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "lanes.json")
	original := []byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	blockedTemp := filepath.Join(directory, "blocked-temp")
	if err := os.WriteFile(blockedTemp, nil, 0600); err != nil {
		t.Fatal(err)
	}
	hub := lanesWriteHub(t, path)
	removed := false
	hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error { return nil }
	hub.lanesWriteOps.createTemp = func(string, string) (*os.File, error) {
		return os.Open(blockedTemp)
	}
	hub.lanesWriteOps.remove = func(name string) error {
		if name == blockedTemp {
			removed = true
		}
		return os.Remove(name)
	}
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p2"}`)
	if writer.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if !removed {
		t.Fatal("failed temp file was not cleaned up")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contents, original) {
		t.Fatalf("failed write changed original")
	}
}

func TestLanesWriteBackupFailureDoesNotChangeOriginal(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "lanes.json")
	original := []byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	hub := lanesWriteHub(t, path)
	hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error {
		return errors.New("injected backup failure")
	}
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p2"}`)
	if writer.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contents, original) {
		t.Fatalf("backup failure changed original")
	}
}

func TestLanesWriteRenameFailureDoesNotChangeOriginal(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "lanes.json")
	original := []byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	hub := lanesWriteHub(t, path)
	hub.lanesWriteOps.createBackup = func(string, []byte, time.Time) error { return nil }
	hub.lanesWriteOps.rename = func(string, string) error { return errors.New("injected rename failure") }
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p2"}`)
	if writer.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(contents, original) {
		t.Fatalf("rename failure changed original")
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".lanes.json.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("rename failure left temp files: %v", temporary)
	}
}

func TestLanesWriteProjectionJSONKeysStayExact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	hub := lanesWriteHub(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1"}`)
	var fields map[string]any
	if err := json.Unmarshal(writer.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, []string{"lane", "machine", "pane", "parent", "sink"}) {
		t.Fatalf("projection keys=%v", keys)
	}
}
