package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	lanesProjectionOperatorToken = "fixture-operator-token"
	lanesProjectionNodeToken     = "fixture-node-token"
)

func lanesProjectionHub(t *testing.T, path string) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": lanesProjectionOperatorToken, "machine-a": lanesProjectionNodeToken},
		ReportRelayPath: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func lanesProjectionGet(hub *HubServer, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/v1/lanes", nil)
	if token != "" {
		request.Header.Set(hubAuthorizationHeader, "Bearer "+token)
	}
	writer := httptest.NewRecorder()
	hub.Handler().ServeHTTP(writer, request)
	return writer
}

func lanesProjectionWrite(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func lanesProjectionResponse(t *testing.T, writer *httptest.ResponseRecorder) []map[string]any {
	t.Helper()
	var response struct {
		Lanes []map[string]any `json:"lanes"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatalf("response=%q err=%v", writer.Body.String(), err)
	}
	return response.Lanes
}

func TestLanesProjectionT1ProjectionSortingAndNoExtraFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-b":{"machine":"machine-a","pane":"w1:p2","parent":"lane-a","token":"fixture-not-a-real-secret"},"lane-a":{"machine":"machine-a","pane":"w1:p1"},"lane-sink":{"sink":true}}}`)

	writer := lanesProjectionGet(lanesProjectionHub(t, path), lanesProjectionOperatorToken)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type=%q", writer.Header().Get("Content-Type"))
	}
	if strings.Contains(writer.Body.String(), "token") {
		t.Fatalf("response exposed fixture-only field: %s", writer.Body.String())
	}
	got := lanesProjectionResponse(t, writer)
	wantKeys := map[string]struct{}{"lane": {}, "machine": {}, "pane": {}, "parent": {}, "sink": {}}
	for _, lane := range got {
		keys := make(map[string]struct{}, len(lane))
		for key := range lane {
			keys[key] = struct{}{}
		}
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("keys=%v want=%v", keys, wantKeys)
		}
	}
	want := []map[string]any{
		{"lane": "lane-a", "machine": "machine-a", "pane": "w1:p1", "parent": "", "sink": false},
		{"lane": "lane-b", "machine": "machine-a", "pane": "w1:p2", "parent": "lane-a", "sink": false},
		{"lane": "lane-sink", "machine": "", "pane": "", "parent": "", "sink": true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lanes=%v want=%v", got, want)
	}
}

func TestLanesProjectionT2Authentication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	hub := lanesProjectionHub(t, path)

	for _, test := range []struct {
		name  string
		token string
	}{
		{name: "missing token"},
		{name: "wrong token", token: "fixture-wrong-token"},
		{name: "node token", token: lanesProjectionNodeToken},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := lanesProjectionGet(hub, test.token)
			if writer.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			if writer.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("www-authenticate=%q", writer.Header().Get("WWW-Authenticate"))
			}
			if strings.Contains(writer.Body.String(), "lane-a") {
				t.Fatalf("unauthorized response exposed lanes: %s", writer.Body.String())
			}
		})
	}
}

func TestLanesProjectionT3InvalidLanes(t *testing.T) {
	for _, test := range []struct {
		name     string
		contents string
	}{
		{name: "malformed JSON", contents: `{"lanes":`},
		{name: "over 64 KiB", contents: strings.Repeat(" ", (64<<10)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lanes.json")
			lanesProjectionWrite(t, path, test.contents)
			writer := lanesProjectionGet(lanesProjectionHub(t, path), lanesProjectionOperatorToken)
			if writer.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
			var got map[string]string
			if err := json.Unmarshal(writer.Body.Bytes(), &got); err != nil {
				t.Fatalf("response=%q err=%v", writer.Body.String(), err)
			}
			if !reflect.DeepEqual(got, map[string]string{"error": "lanes_invalid"}) {
				t.Fatalf("response=%v", got)
			}
		})
	}
}

func TestLanesProjectionT4HotReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	hub := lanesProjectionHub(t, path)
	if got := lanesProjectionResponse(t, lanesProjectionGet(hub, lanesProjectionOperatorToken)); len(got) != 1 || got[0]["lane"] != "lane-a" {
		t.Fatalf("before reload=%v", got)
	}
	lanesProjectionWrite(t, path, `{"lanes":{"lane-b":{"machine":"machine-a","pane":"w1:p2"}}}`)
	if got := lanesProjectionResponse(t, lanesProjectionGet(hub, lanesProjectionOperatorToken)); len(got) != 1 || got[0]["lane"] != "lane-b" {
		t.Fatalf("after reload=%v", got)
	}
}

func TestLanesProjectionT5AbsentLanes(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-lanes.json")
	for _, test := range []struct {
		name string
		path string
	}{
		{name: "empty path"},
		{name: "missing path", path: missing},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := lanesProjectionGet(lanesProjectionHub(t, test.path), lanesProjectionOperatorToken)
			if writer.Code != http.StatusOK || !strings.Contains(writer.Body.String(), `"lanes":[]`) || strings.Contains(writer.Body.String(), "null") {
				t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
			}
		})
	}
}
