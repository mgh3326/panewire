package devin

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
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
		{"empty object", func() map[string]string { f := loginFiles(); f[cacheFile] = `{}`; return f }(), shared.FreeUnknown, nil},
		{"records null", func() map[string]string { f := loginFiles(); f[cacheFile] = `{"schema":"s","records":null}`; return f }(), shared.FreeUnknown, nil},
		{"schema only", func() map[string]string {
			f := loginFiles()
			f[cacheFile] = `{"schema":"pacelever.devin-free-limit.v1"}`
			return f
		}(), shared.FreeUnknown, nil},
		{"records an array", func() map[string]string { f := loginFiles(); f[cacheFile] = `{"records":[]}`; return f }(), shared.FreeUnknown, nil},
		{"a record of the wrong type", func() map[string]string { f := loginFiles(); f[cacheFile] = `{"records":{"x":"text"}}`; return f }(), shared.FreeUnknown, nil},
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

func TestPanewireCacheOverrideWinsAndReadsLegacyFallback(t *testing.T) {
	files := loginFiles()
	files["/new/devin-free-limit.json"] = `{"records":{` + record(hostUserID("host-a", 1000), testNow.Add(time.Hour).Format(time.RFC3339), testNow.Format(time.RFC3339)) + `}}`
	files["/old/devin-free-limit.json"] = `{"records":{}}`
	got := LocalFree(config(files, map[string]string{
		"PANEWIRE_CACHE":  "/new/cache.json",
		"PACELEVER_CACHE": "/old/cache.json",
	}))(testNow)
	if got.State != shared.FreeExhausted {
		t.Fatalf("PANEWIRE_CACHE precedence not followed: %+v", got)
	}
	legacy := LocalFree(config(files, map[string]string{"PACELEVER_CACHE": "/old/cache.json"}))(testNow)
	if legacy.State != shared.FreeAvailable {
		t.Fatalf("legacy cache fallback not read: %+v", legacy)
	}
}

func TestObserveMergesLegacyHoldBeforeWritingPanewireRecord(t *testing.T) {
	now := testNow.UTC()
	cache := t.TempDir()
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	legacyPath := filepath.Join(cache, "pacelever", "devin-free-limit.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	hostID := hostUserID("host-a", 1000)
	legacy := `{"schema":"pacelever.devin-free-limit.v1","records":{` + record(hostID, now.Add(2*time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)) + `}}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ObserveFreeLimit(cfg, now.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:new-account", "devin:0123456789abcdef", false, false); err != nil {
		t.Fatal(err)
	}
	got := LocalFree(cfg)(now)
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("legacy hold lost after new write: %+v", got)
	}
}

func TestObserveDoesNotDeleteNewCacheWhenCandidatesDeduplicate(t *testing.T) {
	now := testNow.UTC()
	cache := t.TempDir()
	path := filepath.Join(cache, "devin-free-limit.json")
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
	}
	cfg.LookupEnv = func(name string) (string, bool) {
		switch name {
		case "PANEWIRE_CACHE":
			return filepath.Join(cache, "panewire-placeholder.json"), true
		case "PACELEVER_CACHE":
			return filepath.Join(cache, "pacelever-placeholder.json"), true
		case "XDG_CACHE_HOME":
			return cache, true
		default:
			return "", false
		}
	}
	if got := cfg.cachePaths(); len(got) != 1 || got[0] != path {
		t.Fatalf("deduplicated cache paths = %v", got)
	}
	if err := ObserveFreeLimit(cfg, now.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:test", "", false, false); err != nil {
		t.Fatal(err)
	}
	if got := LocalFree(cfg)(now); got.State != shared.FreeExhausted {
		t.Fatalf("observation disappeared after deduplicated write: %+v", got)
	}
}

func TestObserveFreeLimitClearRemovesLegacyOnlyHold(t *testing.T) {
	now := testNow.UTC()
	cache := t.TempDir()
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	legacyPath := filepath.Join(cache, "pacelever", "devin-free-limit.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	hostID := hostUserID("host-a", 1000)
	legacy := `{"schema":"pacelever.devin-free-limit.v1","records":{` + record(hostID, now.Add(time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)) + `}}`
	if err := os.WriteFile(legacyPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ObserveFreeLimit(cfg, "", "", "", "", "", false, true); err != nil {
		t.Fatal(err)
	}
	if got := LocalFree(cfg)(now); got.State != shared.FreeAvailable {
		t.Fatalf("legacy hold remained after clear: %+v", got)
	}
}

func TestLocalFreeKeepsLongestHoldAcrossBothCacheFiles(t *testing.T) {
	now := testNow.UTC()
	cache := t.TempDir()
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	id := hostUserID("host-a", 1000)
	for path, reset := range map[string]time.Time{
		filepath.Join(cache, "panewire", "devin-free-limit.json"):  now.Add(time.Hour),
		filepath.Join(cache, "pacelever", "devin-free-limit.json"): now.Add(3 * time.Hour),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		data := `{"records":{` + record(id, reset.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)) + `}}`
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got := LocalFree(cfg)(now)
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(3*time.Hour)) {
		t.Fatalf("merged cache hold = %+v", got)
	}
}

func TestObservePreservesKnownHoldAndDamagedCopyAcrossCacheMigration(t *testing.T) {
	for _, damaged := range []string{`{}`, `"not a record"`} {
		t.Run(damaged, func(t *testing.T) {
			now := testNow.UTC()
			cache := t.TempDir()
			cfg := Config{
				Home:     t.TempDir(),
				Now:      func() time.Time { return now },
				Hostname: func() (string, error) { return "host-a", nil },
				UID:      func() int { return 1000 },
				LookupEnv: func(name string) (string, bool) {
					if name == "XDG_CACHE_HOME" {
						return cache, true
					}
					return "", false
				},
			}
			id := hostUserID("host-a", 1000)
			primaryPath := filepath.Join(cache, "panewire", "devin-free-limit.json")
			legacyPath := filepath.Join(cache, "pacelever", "devin-free-limit.json")
			for path, raw := range map[string]string{
				primaryPath: `{"records":{"` + id + `":` + damaged + `}}`,
				legacyPath:  `{"records":{` + record(id, now.Add(2*time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)) + `}}`,
			} {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			got := LocalFree(cfg)(now)
			if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(2*time.Hour)) {
				t.Fatalf("known hold hidden by damaged cache copy: %+v", got)
			}
			if err := ObserveFreeLimit(cfg, now.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:new-account", "devin:0123456789abcdef", false, false); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(legacyPath); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("legacy cache still exists after migration: %v", err)
			}
			got = LocalFree(cfg)(now)
			if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(2*time.Hour)) {
				t.Fatalf("known hold lost after migration write: %+v", got)
			}
			now = now.Add(3 * time.Hour)
			if got = LocalFree(cfg)(now); got.State != shared.FreeUnknown {
				t.Fatalf("damaged copy stopped holding after known reset: %+v", got)
			}
		})
	}
}

func TestObserveKeepsLegacyWhenPanewireWriteOrReadbackFails(t *testing.T) {
	for _, failAt := range []string{"write", "readback"} {
		t.Run(failAt, func(t *testing.T) {
			now := testNow.UTC()
			cache := t.TempDir()
			legacyPath := filepath.Join(cache, "pacelever", "devin-free-limit.json")
			primaryPath := filepath.Join(cache, "panewire", "devin-free-limit.json")
			if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
				t.Fatal(err)
			}
			id := hostUserID("host-a", 1000)
			legacy := []byte(`{"schema":"pacelever.devin-free-limit.v1","records":{` + record(id, now.Add(2*time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)) + `}}`)
			if err := os.WriteFile(legacyPath, legacy, 0o600); err != nil {
				t.Fatal(err)
			}
			cfg := Config{
				Home:     t.TempDir(),
				Now:      func() time.Time { return now },
				Hostname: func() (string, error) { return "host-a", nil },
				UID:      func() int { return 1000 },
				LookupEnv: func(name string) (string, bool) {
					if name == "XDG_CACHE_HOME" {
						return cache, true
					}
					return "", false
				},
			}
			if failAt == "write" {
				cfg.ReplaceFile = func(string, string, []byte, os.FileMode) error { return errors.New("injected write failure") }
			} else {
				cfg.ReadFile = func(path string) ([]byte, error) {
					if path == primaryPath {
						if _, err := os.Stat(path); err == nil {
							return nil, fs.ErrPermission
						}
					}
					return os.ReadFile(path)
				}
			}

			err := ObserveFreeLimit(cfg, now.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:new-account", "devin:0123456789abcdef", false, false)
			if err == nil {
				t.Fatal("observe succeeded despite injected panewire write/readback failure")
			}
			got, err := os.ReadFile(legacyPath)
			if err != nil || !bytes.Equal(got, legacy) {
				t.Fatalf("legacy file changed or disappeared after %s failure: %s, %v", failAt, got, err)
			}
		})
	}
}

func TestObserveDoesNotRemoveLegacyPathAliasedToPanewireFile(t *testing.T) {
	now := testNow.UTC()
	root := t.TempDir()
	primaryDir := filepath.Join(root, "panewire")
	legacyAlias := filepath.Join(root, "pacelever")
	if err := os.MkdirAll(primaryDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(primaryDir, legacyAlias); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			switch name {
			case "PANEWIRE_CACHE":
				return filepath.Join(primaryDir, "cache.json"), true
			case "PACELEVER_CACHE":
				return filepath.Join(legacyAlias, "cache.json"), true
			default:
				return "", false
			}
		},
	}
	paths := cfg.cachePaths()
	if len(paths) != 2 || paths[0] == paths[1] {
		t.Fatalf("cache candidates = %v, want two distinct aliased paths", paths)
	}
	if err := ObserveFreeLimit(cfg, now.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:test", "", false, false); err != nil {
		t.Fatal(err)
	}
	if got := LocalFree(cfg)(now); got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("aliased legacy cleanup deleted the primary hold: %+v", got)
	}
}

func TestObserveFreeLimitWritesAndClearsPrivateRecord(t *testing.T) {
	now := testNow.UTC()
	cache := filepath.Join(t.TempDir(), "cache")
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return now },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	reset := now.Add(2 * time.Hour).Format(time.RFC3339Nano)
	if err := ObserveFreeLimit(cfg, reset, "", "swe-2-high", "job:test", "", false, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cache, "panewire", "devin-free-limit.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("record mode/info = %v, %v; want 0600", info, err)
	}
	var doc struct {
		Schema  string                     `json:"schema"`
		Records map[string]json.RawMessage `json:"records"`
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &doc) != nil || doc.Schema != limitRecordSchema || len(doc.Records) != 1 {
		t.Fatalf("record document = %s, %v", data, err)
	}
	if got := LocalFree(cfg)(now); got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("observed free limit = %+v", got)
	}
	if err := ObserveFreeLimit(cfg, "", "", "", "", "", false, true); err != nil {
		t.Fatal(err)
	}
	if got := LocalFree(cfg)(now); got.State != shared.FreeAvailable {
		t.Fatalf("cleared free limit = %+v", got)
	}
}

func TestObserveFreeLimitUnknownResetUsesConservativeHold(t *testing.T) {
	now := testNow.UTC()
	cfg := Config{Home: t.TempDir(), Now: func() time.Time { return now }, Hostname: func() (string, error) { return "host-a", nil }, UID: func() int { return 1000 }}
	if err := ObserveFreeLimit(cfg, "", "", "swe-2-high", "job:test", "", true, false); err != nil {
		t.Fatal(err)
	}
	got := LocalFree(cfg)(now)
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(unknownResetHold)) {
		t.Fatalf("unparsed reset = %+v", got)
	}
}

func TestUnparsedObserveDoesNotShortenAnActiveKnownHold(t *testing.T) {
	now := testNow.UTC()
	cfg := Config{Home: t.TempDir(), Now: func() time.Time { return now }, Hostname: func() (string, error) { return "host-a", nil }, UID: func() int { return 1000 }}
	knownReset := now.Add(5 * time.Hour)
	if err := ObserveFreeLimit(cfg, knownReset.Format(time.RFC3339Nano), "", "swe-2-high", "job:first", "", false, false); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	if err := ObserveFreeLimit(cfg, "", "", "swe-2-high", "job:later", "", true, false); err != nil {
		t.Fatal(err)
	}
	got := LocalFree(cfg)(now)
	if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(knownReset) {
		t.Fatalf("unparsed observation shortened active known hold: %+v", got)
	}
}

func TestUnparsedObserveReplacesDamagedPriorEntry(t *testing.T) {
	for _, previous := range []json.RawMessage{json.RawMessage(`"not a record"`), json.RawMessage(`{"reset_at":"bad","observed_at":"bad"}`)} {
		t.Run(string(previous), func(t *testing.T) {
			now := testNow.UTC()
			cache := filepath.Join(t.TempDir(), "cache")
			cfg := Config{
				Home:     t.TempDir(),
				Now:      func() time.Time { return now },
				Hostname: func() (string, error) { return "host-a", nil },
				UID:      func() int { return 1000 },
				LookupEnv: func(name string) (string, bool) {
					if name == "XDG_CACHE_HOME" {
						return cache, true
					}
					return "", false
				},
			}
			path := filepath.Join(cache, "pacelever", "devin-free-limit.json")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			id := hostUserID("host-a", 1000)
			var records map[string]json.RawMessage
			if err := json.Unmarshal([]byte(`{}`), &records); err != nil {
				t.Fatal(err)
			}
			records[id] = previous
			encoded, err := json.Marshal(map[string]any{"schema": limitRecordSchema, "records": records})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ObserveFreeLimit(cfg, "", "", "swe-2-high", "job:recovered", "", true, false); err != nil {
				t.Fatal(err)
			}
			got := LocalFree(cfg)(now)
			if got.State != shared.FreeExhausted || got.ResetsAt == nil || !got.ResetsAt.Equal(now.Add(unknownResetHold)) {
				t.Fatalf("unparsed observation did not replace damaged prior entry: %+v", got)
			}
		})
	}
}

func TestConcurrentObserveFreeLimitPreservesBothAccounts(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "cache")
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return testNow.UTC() },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, fp := range []string{"0123456789abcdef", "fedcba9876543210"} {
		go func(fp string) {
			<-start
			errs <- ObserveFreeLimit(cfg, testNow.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:test", "devin:"+fp, false, false)
		}(fp)
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(cache, "panewire", "devin-free-limit.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Records map[string]json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(data, &doc); err != nil || len(doc.Records) != 2 {
		t.Fatalf("concurrent account records = %v (err %v)", doc.Records, err)
	}
}

func TestObserveFreeLimitSerializesReadModifyWrite(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "cache")
	cfg := Config{
		Home:     t.TempDir(),
		Now:      func() time.Time { return testNow.UTC() },
		Hostname: func() (string, error) { return "host-a", nil },
		UID:      func() int { return 1000 },
		LookupEnv: func(name string) (string, bool) {
			if name == "XDG_CACHE_HOME" {
				return cache, true
			}
			return "", false
		},
	}
	path := filepath.Join(cache, "panewire", "devin-free-limit.json.lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- ObserveFreeLimit(cfg, testNow.Add(time.Hour).Format(time.RFC3339Nano), "", "swe-2-high", "job:test", "", false, false)
	}()
	<-started
	// This wait proves the writer cannot pass its advisory lock while another
	// writer owns it; unlock below then allows the write to complete.
	select {
	case err := <-done:
		t.Fatalf("writer passed a held advisory lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not continue after lock release")
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
