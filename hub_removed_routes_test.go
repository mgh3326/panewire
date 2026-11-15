package panewire

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The burst, node-wake, held-relay edit and operator chat routes were removed
// with their features, so each one answers 404 like any unknown path.
func TestRemovedRoutesReturnNotFound(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op"}})
	if err != nil {
		t.Fatal(err)
	}
	handler := hub.Handler()
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/v1/burst"},
		{http.MethodPost, "/v1/burst/request"},
		{http.MethodPost, "/v1/burst/release"},
		{http.MethodGet, "/v1/burst/holds"},
		{http.MethodPost, "/v1/nodes/wake"},
		{http.MethodGet, "/v1/relay/held"},
		{http.MethodPatch, "/v1/relay/events/7"},
		{http.MethodDelete, "/v1/relay/events/7"},
		{http.MethodGet, "/chat"},
		{http.MethodGet, "/chat/data"},
		{http.MethodPost, "/chat/messages"},
		{http.MethodPost, "/chat/messages/1/retry"},
		{http.MethodPost, "/chat/messages/1/cancel"},
		{http.MethodPost, "/chat/questions/Q-20261113-01/transition"},
		{http.MethodPost, "/v1/chat/questions"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(route.method, route.path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("%s %s: status %d, want 404", route.method, route.path, recorder.Code)
		}
	}
}
