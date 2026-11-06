package devin

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"strconv"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

const cacheFile = "/home/u/.cache/pacelever/devin-free-limit.json"

// hostUserID is pacelever's limitIdentity for a snapshot without an account:
// "host-user-" + hex(sha256(host + NUL + uid))[:32].
func hostUserID(host string, uid int) string {
	sum := sha256.Sum256([]byte(host + "\x00" + strconv.Itoa(uid)))
	return "host-user-" + hex.EncodeToString(sum[:16])
}

func accountID(fp string) string {
	sum := sha256.Sum256([]byte("account\x00" + fp))
	return "account-" + hex.EncodeToString(sum[:16])
}

func limits(records string) map[string]string {
	files := loginFiles()
	files[cacheFile] = `{"schema":"pacelever.devin-free-limit.v1","records":{` + records + `}}`
	return files
}

func record(id, reset, observed string) string {
	return `"` + id + `":{"reset_at":"` + reset + `","observed_at":"` + observed + `","rung":"swe-2-high","source":"job:x","unparsed":false}`
}

func TestLocalFree(t *testing.T) {
	future := testNow.Add(3 * time.Hour).Format(time.RFC3339)
	later := testNow.Add(5 * time.Hour).Format(time.RFC3339)
	past := testNow.Add(-time.Hour).Format(time.RFC3339)
	observed := testNow.Add(-10 * time.Minute).Format(time.RFC3339)
	host := hostUserID("host-a", 1000)
	for _, test := range []struct {
		name  string
		files map[string]string
		state string
		reset *time.Time
	}{
		{"no record file", loginFiles(), shared.FreeAvailable, nil},
		{"truncated record file", func() map[string]string { f := loginFiles(); f[cacheFile] = `{"records":{"x":`; return f }(), shared.FreeUnknown, nil},
		{"record file of the wrong shape", func() map[string]string { f := loginFiles(); f[cacheFile] = `[1]`; return f }(), shared.FreeUnknown, nil},
		{"empty record file", func() map[string]string { f := loginFiles(); f[cacheFile] = ``; return f }(), shared.FreeUnknown, nil},
		{"no records yet", func() map[string]string { f := loginFiles(); f[cacheFile] = `{"records":{}}`; return f }(), shared.FreeAvailable, nil},
		{"no record for this host", limits(record(hostUserID("other", 1000), future, observed)), shared.FreeAvailable, nil},
		{"active exceed", limits(record(host, future, observed)), shared.FreeExhausted, ptr(testNow.Add(3 * time.Hour))},
		{"exceed filed under the account", limits(record(accountID(fixtureFP), future, observed)), shared.FreeExhausted, ptr(testNow.Add(3 * time.Hour))},
		{"the later of two holds", limits(record(host, future, observed) + "," + record(accountID(fixtureFP), later, observed)), shared.FreeExhausted, ptr(testNow.Add(5 * time.Hour))},
		{"ended exceed", limits(record(host, past, observed)), shared.FreeAvailable, nil},
		{"unreadable reset holds for an hour after the observation", limits(record(host, "soon", observed)), shared.FreeExhausted, ptr(testNow.Add(50 * time.Minute))},
		{"unreadable reset, hold over", limits(record(host, "soon", testNow.Add(-2*time.Hour).Format(time.RFC3339))), shared.FreeAvailable, nil},
		{"no usable time at all is damaged", limits(record(host, "soon", "never")), shared.FreeUnknown, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := LocalFree(config(test.files, nil))(testNow)
			if got.State != test.state || (test.reset == nil) != (got.ResetsAt == nil) || test.reset != nil && !got.ResetsAt.Equal(*test.reset) {
				t.Fatalf("free bucket %+v (reset %v), want %s %v", got, got.ResetsAt, test.state, test.reset)
			}
		})
	}
}

func TestPaceleverCacheEnvOverride(t *testing.T) {
	files := loginFiles()
	files["/elsewhere/devin-free-limit.json"] = `{"records":{` + record(hostUserID("host-a", 1000), testNow.Add(time.Hour).Format(time.RFC3339), testNow.Format(time.RFC3339)) + `}}`
	got := LocalFree(config(files, map[string]string{"PACELEVER_CACHE": "/elsewhere/snapshots.json"}))(testNow)
	if got.State != shared.FreeExhausted {
		t.Fatalf("PACELEVER_CACHE not followed: %+v", got)
	}
}

func ptr(t time.Time) *time.Time { return &t }

// A record file that is there but cannot be read (permissions, a directory in
// its place) is not an absent one: it is unknown, never permission to spend the
// free tier.
func TestLocalFreeUnreadableRecordFileIsUnknown(t *testing.T) {
	cfg := config(loginFiles(), nil)
	cfg.ReadFile = func(path string) ([]byte, error) {
		if path == cacheFile {
			return nil, fs.ErrPermission
		}
		return config(loginFiles(), nil).ReadFile(path)
	}
	if got := LocalFree(cfg)(testNow); got.State != shared.FreeUnknown {
		t.Fatalf("free bucket %+v", got)
	}
	cfg.ReadFile = func(path string) ([]byte, error) {
		if path == cacheFile {
			return make([]byte, authFileLimit+1), nil
		}
		return config(loginFiles(), nil).ReadFile(path)
	}
	if got := LocalFree(cfg)(testNow); got.State != shared.FreeUnknown {
		t.Fatalf("oversized record file: %+v", got)
	}
}
