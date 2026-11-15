package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditFPForTest is pacelever's configured anthropic-api account fingerprint
// (config.toml [quota.accounts]); the shared store holds its presence under
// this key.
const creditFPForTest = "5984150754efdf68"

var creditKeyForTest = "anthropic-api:" + creditFPForTest

var creditTestNow = time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)

// creditHK is a handoffkeep shared quota store for the credit pools: each
// account's latest and manual entries, the presence records, and the
// lease/observation pair a native pool's refresh needs.
type creditHK struct {
	t        *testing.T
	server   *httptest.Server
	mu       sync.Mutex
	calls    []string
	latest   map[string]shared.Latest
	presence []shared.Presence
	// failLatest makes every latest read fail with 500.
	failLatest bool
}

func newCreditHK(t *testing.T) *creditHK {
	h := &creditHK{t: t, latest: map[string]shared.Latest{}}
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.calls = append(h.calls, r.Method+" "+r.URL.Path)
		key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/quota/accounts/"), "/latest")
		switch {
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/v1/quota/accounts/") && strings.HasSuffix(r.URL.Path, "/latest"):
			if h.failLatest {
				w.WriteHeader(500)
				return
			}
			_ = json.NewEncoder(w).Encode(h.latest[key])
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/lease"):
			var body struct {
				Holder string `json:"holder"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(shared.Lease{Holder: body.Holder, ExpiresAt: time.Now().Add(time.Minute), Acquired: true})
		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/observation"):
			var body struct {
				Observation shared.Observation `json:"observation"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			h.latest[key] = shared.Latest{Observation: &body.Observation}
			w.WriteHeader(204)
		case r.Method == "DELETE" && strings.HasSuffix(r.URL.Path, "/lease"):
			w.WriteHeader(204)
		case r.Method == "PUT" && r.URL.Path == "/v1/quota/presence":
			var record shared.Presence
			_ = json.NewDecoder(r.Body).Decode(&record)
			h.presence = append(h.presence, record)
			w.WriteHeader(204)
		case r.Method == "GET" && r.URL.Path == "/v1/quota/presence":
			_ = json.NewEncoder(w).Encode(h.presence)
		default:
			h.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(h.server.Close)
	return h
}

func (h *creditHK) records() []shared.Presence {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]shared.Presence(nil), h.presence...)
}

// creditHostConfig gives this host no panewire credit account and pacelever's
// config with the two accounts the pacelever era published.
func creditHostConfig(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts.toml")
	if err := os.WriteFile(hosts, []byte("[quota]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	pacelever := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(pacelever, []byte("[quota.accounts]\nanthropic-api = \""+creditFPForTest+"\"\nopenai-api = \"ac91f07b57208a36\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", pacelever)
}

// creditLiveManual is the store's answer for an account whose operator credit
// entry is live at now.
func creditLiveManual(now time.Time) shared.Latest {
	expires := now.Add(48 * time.Hour)
	return shared.Latest{Manual: []shared.Observation{{
		ID: 11, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-time.Hour), ExpiresAt: &expires,
		Buckets: []shared.Bucket{{Window: "credit", UsedPct: 25, Scope: "account"}},
	}}}
}

// creditTestSource is a credit presence publisher over fakes: the harness is
// installed, the pool is not excluded and the account is the fixture's.
func creditTestSource(spawn string, now func() time.Time, pools ...string) *creditPresenceSource {
	if len(pools) == 0 {
		pools = []string{"anthropic-api"}
	}
	return &creditPresenceSource{
		HostID: "node-a", SpawnConfigPath: spawn, Now: now, Pools: pools,
		Installed: func(name string) bool { return name == "omp" },
		Account: func(pool string) (string, bool) {
			switch pool {
			case "anthropic-api":
				return creditKeyForTest, true
			case "openai-api":
				return "openai-api:ac91f07b57208a36", true
			}
			return "", false
		},
		Excluded: func(string, time.Time) bool { return false },
	}
}

// hk 3998 (a): omp on the host, a live manual credit entry for the pool and no
// exclude class publish one presence record, under the account key pacelever
// published and the node's own host id. internal/fleet's
// TestCreditPresenceBindsOMPRungOnThePublishingHost shows a plan binds the
// pool's omp rung on that host.
func TestCreditPresencePublishesForLiveCredit(t *testing.T) {
	creditHostConfig(t)
	hk := newCreditHK(t)
	hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
	source := creditTestSource(claudeStore(t, hk.server.URL), func() time.Time { return creditTestNow })
	source.Publish(t.Context())
	records := hk.records()
	if len(records) != 1 {
		t.Fatalf("presence = %+v, want one record", records)
	}
	record := records[0]
	if record.HostID != "node-a" || record.AccountKey != creditKeyForTest || !record.LastSeen.Equal(creditTestNow) {
		t.Fatalf("record = %+v, want node-a's presence for %s", record, creditKeyForTest)
	}
}

// hk 3998 review: a record carries the id placement reads the local host under,
// `[local].presence_id` when the fleet config sets one, not the node's machine
// id; without one the machine id is the id.
func TestCreditPresenceHostIDFollowsPresenceID(t *testing.T) {
	hosts := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(hosts, []byte("[local]\npresence_id = \"node-p\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	client := quotaClient(t)
	client.machineID = "node-a"
	if id := client.creditPresencePublisher().HostID; id != "node-p" {
		t.Fatalf("publisher host id = %q, want the presence id placement reads", id)
	}
	if id := quotaCLICreditSource().HostID; id != "node-p" {
		t.Fatalf("status host id = %q, want the presence id placement reads", id)
	}
	if err := os.WriteFile(hosts, []byte("[local]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client.creditPresence = nil
	if id := client.creditPresencePublisher().HostID; id != "node-a" {
		t.Fatalf("publisher host id = %q, want the machine id when no presence id is set", id)
	}
}

// hk 3998 (a): the node's quota loop is the publisher. It announces the credit
// pool even when the node measures no native pool itself, and only once per
// presence interval across rounds.
func TestCreditPresenceRunsInTheQuotaLoopWithoutNativePools(t *testing.T) {
	hk := newCreditHK(t)
	hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
	spawn := claudeStore(t, hk.server.URL)
	client := quotaClient(t)
	client.spawnConfigPath = spawn
	client.statePublish = true
	client.stateQuotaInterval = 20 * time.Millisecond
	client.creditPresence = creditTestSource(spawn, func() time.Time { return creditTestNow })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.runFleetQuota(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	eventually(t, 3*time.Second, "credit presence from the quota loop", func() bool { return len(hk.records()) == 1 })
	// A window that proves nothing more is published: several rounds pass
	// inside the presence interval.
	time.Sleep(60 * time.Millisecond)
	if records := hk.records(); len(records) != 1 {
		t.Fatalf("presence = %d, want one record across rounds", len(records))
	}
}

// hk 3998 (b): each condition pacelever required — the harness installed, the
// pool not excluded, a live manual credit entry — is necessary. Without one,
// the host claims nothing.
func TestCreditPresenceNeedsEveryCondition(t *testing.T) {
	expired := creditTestNow.Add(-time.Hour)
	otherWindow := creditTestNow.Add(48 * time.Hour)
	for _, tc := range []struct {
		name      string
		noHarness bool
		excluded  bool
		latest    *shared.Latest
		live      bool
		failRead  bool
		noAccount bool
	}{
		{name: "no harness", noHarness: true, live: true},
		{name: "excluded pool", excluded: true, live: true},
		{name: "no credit entry"},
		{name: "expired credit entry", latest: &shared.Latest{Manual: []shared.Observation{{Kind: "manual", Window: "credit", MeasuredAt: creditTestNow.Add(-2 * time.Hour), ExpiresAt: &expired, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 25, Scope: "account"}}}}}},
		{name: "credit entry for another window", latest: &shared.Latest{Manual: []shared.Observation{{Kind: "manual", Window: "7d", MeasuredAt: creditTestNow.Add(-time.Hour), ExpiresAt: &otherWindow, Buckets: []shared.Bucket{{Window: "7d", UsedPct: 25, Scope: "account"}}}}}},
		{name: "no account key", noAccount: true, live: true},
		{name: "store read fails", failRead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := newCreditHK(t)
			hk.failLatest = tc.failRead
			if tc.latest != nil {
				hk.latest[creditKeyForTest] = *tc.latest
			} else if tc.live {
				hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
			}
			source := creditTestSource(claudeStore(t, hk.server.URL), func() time.Time { return creditTestNow })
			source.Installed = func(string) bool { return !tc.noHarness }
			source.Excluded = func(string, time.Time) bool { return tc.excluded }
			if tc.noAccount {
				source.Account = func(string) (string, bool) { return "", false }
			}
			source.Publish(t.Context())
			if records := hk.records(); len(records) != 0 {
				t.Fatalf("presence = %+v, want none", records)
			}
		})
	}
}

// hk 3998 (c): presence follows the interval — a second pass inside it writes
// nothing, and the pass after it announces again.
func TestCreditPresenceFollowsThePresenceInterval(t *testing.T) {
	now := creditTestNow
	hk := newCreditHK(t)
	hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
	source := creditTestSource(claudeStore(t, hk.server.URL), func() time.Time { return now })
	source.Publish(t.Context())
	source.Publish(t.Context())
	if records := hk.records(); len(records) != 1 {
		t.Fatalf("presence within the interval = %d, want 1", len(records))
	}
	now = now.Add(shared.DefaultPresenceEvery + time.Second)
	source.Publish(t.Context())
	if records := hk.records(); len(records) != 2 {
		t.Fatalf("presence after the interval = %d, want 2", len(records))
	}
}

// hk 3998 (d): the native pools' presence is unchanged — the claude refresh
// still announces once, before its lease, under the node's host id, and the
// credit pass beside it adds only the credit pool's record.
func TestCreditPresenceLeavesNativePresenceUnchanged(t *testing.T) {
	hk := newCreditHK(t)
	hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
	spawn := claudeStore(t, hk.server.URL)
	client := quotaClient(t)
	client.spawnConfigPath = spawn
	client.warn = func(string) {}
	client.nativeQuota = map[string]*nativeQuotaSource{"claude": claudeTestSource(spawn, claudeLoginFiles(claudeSecret, claudeTestNow.Add(time.Hour)), claudeAPIReply(200, claudeAPIBody))}
	client.refreshFleetQuota(t.Context(), "claude", map[string]fleetQuotaSent{})
	client.creditPresence = creditTestSource(spawn, func() time.Time { return creditTestNow })
	client.creditPresencePublisher().Publish(t.Context())
	byKey := map[string][]shared.Presence{}
	for _, record := range hk.records() {
		byKey[record.AccountKey] = append(byKey[record.AccountKey], record)
	}
	claudeRecords := byKey[claudeKeyForTest]
	if len(claudeRecords) != 1 || claudeRecords[0].HostID != "node-a" || !claudeRecords[0].LastSeen.Equal(claudeTestNow) {
		t.Fatalf("claude presence = %+v, want the one announcement the refresh made", claudeRecords)
	}
	if creditRecords := byKey[creditKeyForTest]; len(creditRecords) != 1 {
		t.Fatalf("credit presence = %+v, want the credit pool's one record", creditRecords)
	}
	for key := range byKey {
		if key != claudeKeyForTest && key != creditKeyForTest {
			t.Fatalf("unexpected presence key %q", key)
		}
	}
}

// hk 3998 (2): `quota status` shows each credit pool on a host that publishes
// it, with the class, the manual credit entry's state and this host's presence,
// so an operator can see why a credit rung is or is not placeable.
func TestQuotaStatusShowsCreditPools(t *testing.T) {
	hk := newCreditHK(t)
	hk.latest[creditKeyForTest] = creditLiveManual(creditTestNow)
	hk.presence = append(hk.presence, shared.Presence{HostID: "node-a", AccountKey: creditKeyForTest, LastSeen: creditTestNow})
	spawn := claudeStore(t, hk.server.URL)
	previousNative, previousCredit := quotaCLISource, quotaCLICreditSource
	t.Cleanup(func() { quotaCLISource, quotaCLICreditSource = previousNative, previousCredit })
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return &nativeQuotaSource{Pool: pool, Now: func() time.Time { return creditTestNow }}
	}
	quotaCLICreditSource = func() *creditPresenceSource {
		return creditTestSource(spawn, func() time.Time { return creditTestNow }, "anthropic-api", "openai-api")
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"status", "--json"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	var doc struct {
		Hosts []struct {
			Readings []quotaReading `json:"readings"`
		} `json:"hosts"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil || len(doc.Hosts) != 1 {
		t.Fatalf("status JSON: %v %s", err, out.String())
	}
	byPool := map[string]quotaReading{}
	for _, reading := range doc.Hosts[0].Readings {
		byPool[reading.Pool] = reading
	}
	anthropic := byPool["anthropic-api"]
	if anthropic.Account != creditFPForTest || anthropic.Class != "spend" || anthropic.Credit != "live" || anthropic.Presence != "present" || anthropic.Stale || anthropic.AgeSeconds == nil || *anthropic.AgeSeconds != 3600 {
		t.Fatalf("anthropic-api line = %+v", anthropic)
	}
	openai := byPool["openai-api"]
	if openai.Account != "ac91f07b57208a36" || openai.Credit != "absent" || openai.Presence != "absent" || !openai.Stale {
		t.Fatalf("openai-api line = %+v", openai)
	}
	out.Reset()
	errOut.Reset()
	if code := runQuotaCLI([]string{"status"}, &out, &errOut); code != 0 {
		t.Fatalf("text status %d: %q", code, errOut.String())
	}
	if row := statusLineWith(out.String(), "anthropic-api"); !strings.Contains(row, creditFPForTest) || !strings.HasSuffix(strings.Join(strings.Fields(row), " "), "fresh credit") {
		t.Fatalf("anthropic-api table row = %q", row)
	}
}

// A store that cannot be read keeps the line's shape: the class and both states
// are printed as unknown rather than dropped, so the operator sees the cause.
func TestQuotaStatusShowsAnUnreadableCreditStore(t *testing.T) {
	hk := newCreditHK(t)
	hk.failLatest = true
	spawn := claudeStore(t, hk.server.URL)
	previousNative, previousCredit := quotaCLISource, quotaCLICreditSource
	t.Cleanup(func() { quotaCLISource, quotaCLICreditSource = previousNative, previousCredit })
	quotaCLISource = func(pool string) *nativeQuotaSource {
		return &nativeQuotaSource{Pool: pool, Now: func() time.Time { return creditTestNow }}
	}
	quotaCLICreditSource = func() *creditPresenceSource {
		return creditTestSource(spawn, func() time.Time { return creditTestNow })
	}
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"status"}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if row := statusLineWith(out.String(), "anthropic-api"); !strings.Contains(row, creditFPForTest) || !strings.HasSuffix(strings.Join(strings.Fields(row), " "), "stale credit") {
		t.Fatalf("unreadable anthropic-api table row = %q", row)
	}
}

// A host without the harness publishes nothing and has no credit line: it can
// never hold the account.
func TestQuotaStatusOmitsCreditPoolsWithoutTheHarness(t *testing.T) {
	source := creditTestSource("", func() time.Time { return creditTestNow })
	source.Installed = func(string) bool { return false }
	if _, ok := source.Status(context.Background(), "anthropic-api"); ok {
		t.Fatal("a host without the harness must not list a credit pool")
	}
}

// statusLineWith is the first `quota status` table line naming pool, so a test
// asserts one row's columns without pinning the whole table.
func statusLineWith(table, pool string) string {
	for _, line := range strings.Split(table, "\n") {
		if strings.Contains(line, pool) {
			return line
		}
	}
	return ""
}
