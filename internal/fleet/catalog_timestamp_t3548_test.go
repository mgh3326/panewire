package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A catalog cache entry whose fetched_at is missing or unreadable has no
// age, so it is never fresh, whatever the TTL: Read asks handoffkeep, and
// when handoffkeep is down the entry is served stale. pacelever reads such an
// entry as fetched at the zero time and refreshes it the same way.

// writeUndatedCatalogCache stores rows as the client's cache entry with
// fetchedAt as its timestamp.
func writeUndatedCatalogCache(t *testing.T, client *CatalogClient, rows []CatalogRow, fetchedAt string) {
	t.Helper()
	raw := make([]json.RawMessage, len(rows))
	for i, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		raw[i] = b
	}
	data, err := json.Marshal(catalogCache{FetchedAt: fetchedAt, EndpointID: client.endpointID(), Rows: raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(client.cachePath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(client.cachePath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

const longTTLConfig = "[bench]\ncatalog_ttl_s = 172800\n"

// TestCatalogUndatedCacheIsNeverFresh: with catalog_ttl_s = 172800 (two
// days, longer than the age an unreadable timestamp used to stand for), an
// undated cache entry is not served as fresh: the catalog client refreshes
// it, ReadCached does not return it, and with handoffkeep down it is served
// stale.
func TestCatalogUndatedCacheIsNeverFresh(t *testing.T) {
	long := CatalogFreshness{TTLS: 172800}
	for _, fetchedAt := range []string{"not a time", ""} {
		t.Run("fetched_at="+fetchedAt, func(t *testing.T) {
			state := &catalogServer{body: `{"catalog":[{"profile":"opus","effort":"high","model_id":"claude-opus-5-1","pool":"claude","grade":"S","gate":"default"}]}`}
			env := catalogEnv(t, state)
			client, err := NewCatalogClient(CatalogClientConfig{Now: func() time.Time { return launchNow }, Freshness: long})
			if err != nil {
				t.Fatal(err)
			}
			writeUndatedCatalogCache(t, env, launchRulesCatalog(), fetchedAt)

			if _, ok := client.ReadCached(); ok {
				t.Fatal("ReadCached served an undated cache entry as fresh")
			}
			data, err := client.Read(context.Background(), false)
			if err != nil || data.Info.Source != catalogSourceServer || state.count() != 1 {
				t.Fatalf("an undated cache entry must be refreshed: source=%q err=%v reads=%d", data.Info.Source, err, state.count())
			}

			writeUndatedCatalogCache(t, env, launchRulesCatalog(), fetchedAt)
			state.setFail(true)
			data, err = client.Read(context.Background(), false)
			if err != nil || data.Info.Source != catalogSourceCacheStale || !data.Info.Stale {
				t.Fatalf("an undated cache entry served without a refresh must be stale: %+v %v", data.Info, err)
			}
		})
	}
}

// TestLaunchNativeRefreshesUndatedCache: end to end, native launch on a host
// with catalog_ttl_s = 172800 refreshes an undated cache from handoffkeep and
// resolves against handoffkeep's answer.
func TestLaunchNativeRefreshesUndatedCache(t *testing.T) {
	state := &catalogServer{body: `{"catalog":[{"profile":"opus","effort":"high","model_id":"claude-opus-5-1","pool":"claude","grade":"S","gate":"default"}]}`}
	client := catalogEnv(t, state)
	a, _, _ := launchApp(t, "native", nil, nil)
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(longTTLConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", path)
	writeUndatedCatalogCache(t, client, launchRulesCatalog(), "not a time")
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "implementer"})
	if err != nil || got.ModelID != "claude-opus-5-1" || state.count() != 1 {
		t.Fatalf("an undated cache must be refreshed, not resolved as fresh: %+v %v reads=%d", got, err, state.count())
	}
}
