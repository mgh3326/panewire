package devin

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// token and apiKey may appear only in the web request's Authorization header;
// no error, observation or log carries them.
const (
	token  = "devin-bearer-token-must-not-leak"
	apiKey = "devin-api-key-must-not-leak"
	org    = "org-fixture"
)

// fixtureFP is hex(sha256("devin-account|org-fixture"))[:16], computed with
// pacelever@f7a1825 (fingerprint("devin-account", "org-fixture")).
const fixtureFP = "9651d01012552948"

var testNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type doFunc func(*http.Request) (*http.Response, error)

func (f doFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func reply(status int, body string, header http.Header) doFunc {
	return func(*http.Request) (*http.Response, error) {
		if header == nil {
			header = http.Header{}
		}
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

// loginFiles is a devin login under /home/u: the CLI's credentials file and
// selected organization, and pacelever's config naming the same organization.
func loginFiles() map[string]string {
	return map[string]string{
		"/home/u/.local/share/devin/credentials.toml": "windsurf_api_key = '" + apiKey + "'\n",
		"/home/u/.config/devin/config.json":           `{"devin":{"org_id":"` + org + `"}}`,
		"/home/u/.config/pacelever/config.toml":       "[pools.devin]\norg_id = '" + org + "'\n",
	}
}

func without(files map[string]string, paths ...string) map[string]string {
	out := map[string]string{}
	for k, v := range files {
		out[k] = v
	}
	for _, p := range paths {
		delete(out, p)
	}
	return out
}

// config is a measurement over fakes: the login files, the environment, no
// devin binary on PATH.
func config(files map[string]string, env map[string]string) Config {
	return Config{
		Now: func() time.Time { return testNow }, Home: "/home/u",
		LookupEnv: func(name string) (string, bool) { v, ok := env[name]; return v, ok },
		LookPath:  func(string) (string, error) { return "", os.ErrNotExist },
		ReadFile: func(path string) ([]byte, error) {
			if body, ok := files[path]; ok {
				return []byte(body), nil
			}
			return nil, os.ErrNotExist
		},
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
	}
}

func withToken() map[string]string { return map[string]string{"DEVIN_BEARER_TOKEN": token} }

type wantBucket struct {
	window string
	used   float64
	reset  string
}

func checkBuckets(t *testing.T, got []quota.Bucket, want []wantBucket) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("buckets %+v, want %+v", got, want)
	}
	for i, b := range got {
		if b.Window != want[i].window || b.UsedPct == nil || *b.UsedPct != want[i].used || b.ResetsAt != want[i].reset || b.Scope.Kind != quota.ScopeAccount {
			t.Fatalf("bucket %d = %+v used=%v, want %+v", i, b, b.UsedPct, want[i])
		}
	}
}
