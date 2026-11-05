package claude

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newLoopbackServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.URL
}
