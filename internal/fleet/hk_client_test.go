package fleet

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/hkcred"
)

// newHKClient takes the svc-panewire credential only: unconfigured is the
// typed *hkcred.UnconfiguredError even when the hk CLI's own token and
// config.env are present, and a configured credential gives the client the
// HTTP settings of panewire's other hk clients.
func TestNewHKClientUsesSvcCredentialOnly(t *testing.T) {
	a := testApp(t)
	cli := filepath.Join(a.Home, ".config", "handoffkeep", "config.env")
	if e := os.MkdirAll(filepath.Dir(cli), 0o700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(cli, []byte("HANDOFFKEEP_URL=https://hk.example\nHANDOFFKEEP_TOKEN="+hkCLITestToken+"\n"), 0o600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("HOME", a.Home)
	t.Setenv("HANDOFFKEEP_TOKEN", hkCLITestToken)
	t.Setenv("HANDOFFKEEP_URL", "https://hk.example")

	_, err := newHKClient()
	var unconfigured *hkcred.UnconfiguredError
	if !errors.As(err, &unconfigured) {
		t.Fatalf("newHKClient without the svc token = %v, want *hkcred.UnconfiguredError", err)
	}
	if strings.Contains(err.Error(), hkCLITestToken) {
		t.Fatalf("error leaked the CLI token: %v", err)
	}

	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", hkSvcTestToken)
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "cf-id")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "cf-secret")
	c, err := newHKClient()
	if err != nil {
		t.Fatal(err)
	}
	if c.URL != "https://hk.example" || c.Token != hkSvcTestToken || c.CFAccessClientID != "cf-id" || c.CFAccessClientSecret != "cf-secret" {
		t.Fatalf("client = %q %q %q %q", c.URL, c.Token, c.CFAccessClientID, c.CFAccessClientSecret)
	}
	if c.HTTP == nil || c.HTTP.Timeout != catalogTimeout || c.HTTP.CheckRedirect == nil || c.HTTP.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatalf("http client settings: %+v", c.HTTP)
	}
	if c.HTTP.Transport != nil {
		t.Fatal("an https endpoint keeps the default transport")
	}

	// Plain HTTP only to loopback or the tailnet, and then with no proxy.
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")
	t.Setenv("HANDOFFKEEP_URL", "http://127.0.0.1:9")
	c, err = newHKClient()
	if err != nil {
		t.Fatal(err)
	}
	if tr, ok := c.HTTP.Transport.(*http.Transport); !ok || tr.Proxy != nil {
		t.Fatalf("plain HTTP transport must not use a proxy: %#v", c.HTTP.Transport)
	}
	for _, bad := range []string{"http://hk.example", "https://user@hk.example", "ftp://hk.example", "https://hk.example/?q=1"} {
		t.Setenv("HANDOFFKEEP_URL", bad)
		if _, err := newHKClient(); err == nil || strings.Contains(err.Error(), bad) {
			t.Fatalf("endpoint %q: err=%v", bad, err)
		}
	}
}

func TestHKTaskListPageAnswerUsesAfterID(t *testing.T) {
	const answer = `{"tasks":[{"id":1002,"priority":3,"refs":{"job_id":"job-active"}}]}`
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(answer))
	}))
	defer server.Close()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_ID", "")
	t.Setenv("HANDOFFKEEP_CF_ACCESS_CLIENT_SECRET", "")

	body, err := HKTaskListPageAnswer(context.Background(), 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != answer {
		t.Fatalf("page answer=%q, want %q", body, answer)
	}
	values, err := url.ParseQuery(query)
	if err != nil || values.Get("after_id") != "1000" || values.Get("limit") != "1000" {
		t.Fatalf("query=%q err=%v, want after_id=1000 and limit=1000", query, err)
	}
}
