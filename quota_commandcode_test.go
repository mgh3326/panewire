package panewire

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/quota"
	"github.com/mgh3326/panewire/internal/quota/commandcode"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// The commandcode pool is read from `omp usage --json` (hk 4707). The fixtures are
// recorded omp output (omp 18.14.2, 2026-11-15) with the ids replaced; no test
// calls omp.

// commandcodeFetched is the fetchedAt of account A in the fixture; account B's is
// 30 s older.
var commandcodeFetched = time.UnixMilli(1794696766070).UTC()

const (
	commandcodeFPA = "7d279b7dcc317cda" // user_fixture_a
	commandcodeFPB = "a1fd7fc5ae227218" // user_fixture_b
)

var (
	commandcodeKeyA = "commandcode:" + commandcodeFPA
	commandcodeKeyB = "commandcode:" + commandcodeFPB
)

func commandcodeFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("internal", "quota", "commandcode", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// ompFake answers `omp usage --json` with a fixed document and counts the calls.
type ompFake struct {
	mu     sync.Mutex
	output []byte
	err    error
	reads  int
}

func (o *ompFake) run(_ context.Context, args ...string) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return nil, o.err
	}
	if len(args) > 1 && args[1] == "invalidate" {
		return []byte("ok\n"), nil
	}
	o.reads++
	return o.output, nil
}

func (o *ompFake) readCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reads
}

// commandcodeTestSource is a commandcode measurement over a fake omp, read 90 s
// after omp fetched account A.
func commandcodeTestSource(spawnConfig string, o *ompFake) *nativeQuotaSource {
	now := commandcodeFetched.Add(90 * time.Second)
	return commandcodeTestSourceAt(spawnConfig, o, &now)
}

// commandcodeTestSourceAt reads the clock from *now, so a test can move it.
func commandcodeTestSourceAt(spawnConfig string, o *ompFake, now *time.Time) *nativeQuotaSource {
	return &nativeQuotaSource{
		Pool: "commandcode", SpawnConfigPath: spawnConfig, HostID: "node-a", Now: func() time.Time { return *now }, Wait: 5 * time.Millisecond,
		Commandcode: commandcode.Config{Run: o.run},
	}
}

// accountStore is a handoffkeep that keeps one reading per account key, with
// leases always granted, and records presence.
type accountStore struct {
	server   *httptest.Server
	mu       sync.Mutex
	latest   map[string]json.RawMessage
	pubs     map[string]int
	presence []shared.Presence
	calls    []string
}

func newAccountStore(t *testing.T) *accountStore {
	s := &accountStore{latest: map[string]json.RawMessage{}, pubs: map[string]int{}}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.calls = append(s.calls, r.Method+" "+r.URL.Path)
		var body struct {
			Holder      string          `json:"holder"`
			Observation json.RawMessage `json:"observation"`
			shared.Presence
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		switch {
		case r.Method == "GET" && len(parts) == 5 && parts[4] == "latest":
			if o := s.latest[parts[3]]; o != nil {
				_, _ = w.Write([]byte(`{"observation":` + string(o) + `}`))
				return
			}
			_, _ = w.Write([]byte(`{"observation":null}`))
		case r.Method == "POST" && len(parts) == 5 && parts[4] == "lease":
			_ = json.NewEncoder(w).Encode(shared.Lease{Holder: body.Holder, ExpiresAt: time.Now().Add(time.Minute), Acquired: true})
		case r.Method == "PUT" && len(parts) == 5 && parts[4] == "observation":
			s.latest[parts[3]] = body.Observation
			s.pubs[parts[3]]++
			w.WriteHeader(204)
		case r.Method == "PUT" && r.URL.Path == "/v1/quota/presence":
			s.presence = append(s.presence, body.Presence)
			w.WriteHeader(204)
		case r.Method == "GET" && r.URL.Path == "/v1/quota/presence":
			_ = json.NewEncoder(w).Encode(s.presence)
		default:
			w.WriteHeader(204)
		}
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *accountStore) published(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pubs[key]
}

func (s *accountStore) observation(t *testing.T, key string) (observation struct {
	Kind       string    `json:"kind"`
	MeasuredAt time.Time `json:"measured_at"`
	Buckets    []struct {
		Window   string  `json:"window"`
		UsedPct  float64 `json:"used_pct"`
		ResetsAt string  `json:"resets_at"`
		Scope    string  `json:"scope"`
	} `json:"buckets"`
}) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := json.Unmarshal(s.latest[key], &observation); err != nil {
		t.Fatalf("no observation stored under %s: %v", key, err)
	}
	return observation
}

// restamp makes every stored reading current as of at, as another host's refresh does.
func (s *accountStore) restamp(t *testing.T, at time.Time) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, raw := range s.latest {
		var o map[string]any
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Fatal(err)
		}
		o["measured_at"] = at
		s.latest[key], _ = json.Marshal(o)
	}
}

// restampKey makes one account's stored reading current as of at.
func (s *accountStore) restampKey(t *testing.T, key string, at time.Time) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var o map[string]any
	if err := json.Unmarshal(s.latest[key], &o); err != nil {
		t.Fatal(err)
	}
	o["measured_at"] = at
	s.latest[key], _ = json.Marshal(o)
}

// expire makes one account's stored reading too old to be current.
func (s *accountStore) expire(t *testing.T, key string) {
	t.Helper()
	s.restampKey(t, key, time.Unix(0, 0).UTC())
}

func (s *accountStore) held(host string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for _, p := range s.presence {
		if p.HostID == host {
			keys = append(keys, p.AccountKey)
		}
	}
	return keys
}

// paceleverRunnerFake answers `pacelever refresh commandcode` with output and
// `pacelever --json` with status, and records every call.
type paceleverRunnerFake struct {
	mu     sync.Mutex
	calls  []string
	output string
	status string
	// pool is the pool whose refresh it answers; empty is commandcode.
	pool string
}

func (p *paceleverRunnerFake) run(_ context.Context, args ...string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, strings.Join(args, " "))
	switch {
	case len(args) == 2 && args[0] == "refresh" && args[1] == cmp.Or(p.pool, "commandcode") && p.output != "":
		return []byte(p.output), nil
	case len(args) == 1 && args[0] == "--json":
		return []byte(p.status), nil
	}
	return nil, errors.New("exit status 2")
}

func (p *paceleverRunnerFake) called() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// warnLog collects a client's warnings; the shadow comparison writes its line
// from a background goroutine.
type warnLog struct {
	mu  sync.Mutex
	log []string
}

func newWarnLog(client *HubClient) *warnLog {
	w := &warnLog{}
	client.warn = func(m string) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.log = append(w.log, m)
	}
	return w
}

func (w *warnLog) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.log...)
}

// Every account omp holds is published under its own key with its own windows:
// 5h and 7d, no month, stamped with the time omp fetched them (decisions 288-290).
func TestCommandcodeRefreshPublishesEveryAccountUnderItsOwnKey(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	source := commandcodeTestSource(claudeStore(t, store.server.URL), omp)

	readings, measured, err := source.RefreshAccounts(t.Context())
	if err != nil || !measured || len(readings) != 2 || readings[0].Key != commandcodeKeyA || readings[1].Key != commandcodeKeyB {
		t.Fatalf("readings %+v measured=%v err=%v", readings, measured, err)
	}
	for key, want := range map[string]string{commandcodeKeyA: "5h,7d", commandcodeKeyB: "5h,7d"} {
		if store.published(key) != 1 {
			t.Fatalf("%s published %d times", key, store.published(key))
		}
		var windows []string
		observation := store.observation(t, key)
		for _, b := range observation.Buckets {
			windows = append(windows, b.Window)
			if b.Scope != "account" {
				t.Fatalf("%s %s scope %q", key, b.Window, b.Scope)
			}
		}
		if strings.Join(windows, ",") != want {
			t.Fatalf("%s windows %v, want %s and no 30d", key, windows, want)
		}
	}
	a := store.observation(t, commandcodeKeyA)
	if !a.MeasuredAt.Equal(commandcodeFetched) {
		t.Fatalf("measured_at %v, want omp's fetchedAt %v: a cached report is not as new as the call", a.MeasuredAt, commandcodeFetched)
	}
	if b := store.observation(t, commandcodeKeyB); b.Buckets[1].UsedPct != 100 {
		t.Fatalf("account B weekly window %+v, want 100%% (exhausted)", b.Buckets[1])
	}
	if got := store.held("node-a"); len(got) != 2 {
		t.Fatalf("presence %v, want both accounts announced for this host", got)
	}
}

// A refresh whose shared readings are all current asks omp nothing: the accounts
// are the ones the last read listed.
func TestCommandcodeCurrentSharedReadingsAskOmpNothing(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	source := commandcodeTestSource(claudeStore(t, store.server.URL), omp)
	if _, _, err := source.RefreshAccounts(t.Context()); err != nil || omp.readCount() != 1 {
		t.Fatalf("first refresh: reads=%d err=%v", omp.readCount(), err)
	}
	readings, measured, err := source.RefreshAccounts(t.Context())
	if err != nil || measured || len(readings) != 2 || omp.readCount() != 1 {
		t.Fatalf("second refresh: readings=%d measured=%v reads=%d err=%v", len(readings), measured, omp.readCount(), err)
	}
}

// An account omp stopped listing is no longer refreshed, and the other keeps its
// reading: the pool is usable while one account is.
func TestCommandcodeAccountOmpNoLongerHoldsDoesNotHideTheOther(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	now := commandcodeFetched.Add(90 * time.Second)
	source := commandcodeTestSourceAt(claudeStore(t, store.server.URL), omp, &now)
	if _, _, err := source.RefreshAccounts(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Both stored readings are now older than the flow's refresh point, and omp
	// lists only account A.
	now = commandcodeFetched.Add(4 * time.Minute)
	var only struct {
		Reports []json.RawMessage `json:"reports"`
	}
	if err := json.Unmarshal(omp.output, &only); err != nil {
		t.Fatal(err)
	}
	refetched := bytes.ReplaceAll(only.Reports[0], []byte("1794696766070"), []byte("1794696946070"))
	omp.output, _ = json.Marshal(map[string]any{"reports": []json.RawMessage{refetched}})
	readings, _, err := source.RefreshAccounts(t.Context())
	if err != nil || len(readings) != 1 || readings[0].Key != commandcodeKeyA {
		t.Fatalf("readings %+v err=%v, want only account A", readings, err)
	}
}

// The account inventory is read from omp at least every MaxReportAge even when
// another host keeps the shared readings current: a removed account is no longer
// refreshed or announced, and a new one is found.
func TestCommandcodeInventoryIsReadEvenWhileSharedReadingsAreCurrent(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	now := commandcodeFetched.Add(90 * time.Second)
	source := commandcodeTestSourceAt(claudeStore(t, store.server.URL), omp, &now)
	if _, _, err := source.RefreshAccounts(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Four minutes on, another host has re-stamped both shared readings as
	// current, and omp now holds account A and a new account C, not B.
	now = commandcodeFetched.Add(5 * time.Minute)
	store.restamp(t, now.Add(-time.Second))
	moved := bytes.ReplaceAll(omp.output, []byte("user_fixture_b"), []byte("user_fixture_c"))
	moved = bytes.ReplaceAll(moved, []byte("1794696766070"), []byte(strconv.FormatInt(now.Add(-time.Second).UnixMilli(), 10)))
	moved = bytes.ReplaceAll(moved, []byte("1794696736070"), []byte(strconv.FormatInt(now.Add(-2*time.Second).UnixMilli(), 10)))
	omp.output = moved
	before := omp.readCount()
	readings, _, err := source.RefreshAccounts(t.Context())
	if err != nil || omp.readCount() == before {
		t.Fatalf("err=%v reads %d->%d: the inventory must be read again", err, before, omp.readCount())
	}
	var keys []string
	for _, r := range readings {
		keys = append(keys, r.Key)
	}
	keyC := "commandcode:" + commandcode.Fingerprint("user_fixture_c")
	if len(keys) != 2 || keys[0] != commandcodeKeyA && keys[1] != commandcodeKeyA || keys[0] != keyC && keys[1] != keyC {
		t.Fatalf("keys %v, want account A and the new account C, not B", keys)
	}
}

// An omp output the node cannot read aborts the whole refresh, also when the
// inventory is still current and another account's shared reading is current:
// nothing is returned to publish or admit on.
func TestCommandcodeUnreadableOutputAbortsRefreshDespiteACurrentSharedReading(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	now := commandcodeFetched.Add(90 * time.Second)
	source := commandcodeTestSourceAt(claudeStore(t, store.server.URL), omp, &now)
	if _, _, err := source.RefreshAccounts(t.Context()); err != nil {
		t.Fatal(err)
	}
	// 100 s on: the inventory is within 3 minutes, account A (fetched 10 s
	// earlier in the fixture clock) is due, account B's shared reading was
	// re-stamped current by another host, and omp now answers an unknown shape.
	now = commandcodeFetched.Add(190 * time.Second)
	store.restampKey(t, commandcodeKeyB, now.Add(-time.Second))
	store.expire(t, commandcodeKeyA)
	omp.output = []byte(`{"version":9}`)
	readings, _, err := source.RefreshAccounts(t.Context())
	if quota.KindOf(err) != quota.ErrKindParse || len(readings) != 0 {
		t.Fatalf("readings %d err %v, want parse_error and no reading", len(readings), err)
	}
}

// The node loop publishes each account to the hub under its own key and notes
// one account for the readiness login.
func TestFleetQuotaCommandcodePublishesEachAccount(t *testing.T) {
	store := newAccountStore(t)
	client := quotaClient(t)
	client.events = make(chan hubClientEvent, 8)
	client.spawnConfigPath = claudeStore(t, store.server.URL)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": commandcodeTestSource(client.spawnConfigPath, omp)}
	warned := newWarnLog(client)

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "commandcode", sent)
	if got := warned.all(); len(got) != 0 {
		t.Fatalf("warnings %q", got)
	}
	var keys []string
	for range 2 {
		select {
		case event := <-client.events:
			var publish FleetStatePublish
			if err := json.Unmarshal(event.Payload, &publish); err != nil {
				t.Fatal(err)
			}
			if publish.Quota == nil || publish.Quota.Pool != "commandcode" || len(publish.Quota.Buckets) != 2 {
				t.Fatalf("publish %+v quota %+v", publish, publish.Quota)
			}
			keys = append(keys, publish.Key)
		default:
			t.Fatalf("published %v, want one state value per account", keys)
		}
	}
	if keys[0] != commandcodeKeyA || keys[1] != commandcodeKeyB {
		t.Fatalf("keys %v", keys)
	}
	// An unchanged reading is not published again before the republish bound.
	client.refreshFleetQuota(t.Context(), "commandcode", sent)
	select {
	case event := <-client.events:
		t.Fatalf("republished an unchanged reading: %s", event.Payload)
	default:
	}
}

// `panewire quota status` reads every account's stored reading without
// measuring, with the age of omp's report.
func TestCommandcodeStatusListsEachAccountWithItsAge(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	source := commandcodeTestSource(claudeStore(t, store.server.URL), omp)
	if _, _, err := source.RefreshAccounts(t.Context()); err != nil {
		t.Fatal(err)
	}
	reads := omp.readCount()
	readings, err := source.storedAccounts(t.Context())
	if err != nil || len(readings) != 2 || omp.readCount() != reads {
		t.Fatalf("readings %+v err=%v reads %d->%d; status must not call omp", readings, err, reads, omp.readCount())
	}
	if readings[0].Account != commandcodeFPA || readings[1].Account != commandcodeFPB || *readings[0].AgeSeconds != 90 || *readings[1].AgeSeconds != 120 {
		t.Fatalf("readings %+v ages %v %v", readings, *readings[0].AgeSeconds, *readings[1].AgeSeconds)
	}
	policy, err := fleet.LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	writeQuotaStatusTable(&out, source.now(), quotaStatusHost{Host: "node-a", Readings: readings}, nil, map[string]string{"commandcode": "spend"}, policy)
	table := out.String()
	for _, want := range []string{commandcodeFPA, commandcodeFPB, "fresh 1m", "fresh 2m", "5h", "7d"} {
		if !strings.Contains(table, want) {
			t.Fatalf("status table lacks %q:\n%s", want, table)
		}
	}
}

// An omp output this build does not know is unmeasurable: nothing is published,
// the reason is logged once, and the node reads the login as unknown, not missing.
func TestCommandcodeUnknownOmpOutputFailsClosedAndLogsOnce(t *testing.T) {
	store := newAccountStore(t)
	client := quotaClient(t)
	client.events = make(chan hubClientEvent, 8)
	client.spawnConfigPath = claudeStore(t, store.server.URL)
	omp := &ompFake{output: []byte(`{"version":2,"accounts":[]}`)}
	source := commandcodeTestSource(client.spawnConfigPath, omp)
	source.Warn = client.warnMessage
	client.nativeQuota = map[string]*nativeQuotaSource{"commandcode": source}
	warned := newWarnLog(client)
	source.Warn = client.warnMessage

	sent := map[string]fleetQuotaSent{}
	client.refreshFleetQuota(t.Context(), "commandcode", sent)
	client.refreshFleetQuota(t.Context(), "commandcode", sent)

	if store.published(commandcodeKeyA)+store.published(commandcodeKeyB) != 0 || len(client.events) != 0 {
		t.Fatal("an unreadable omp output was published")
	}
	var reason int
	for _, line := range warned.all() {
		if strings.Contains(line, "unsupported omp usage output") {
			reason++
		}
	}
	if reason != 1 {
		t.Fatalf("the reason was logged %d times, want once: %q", reason, warned.all())
	}
	_, _, err := source.RefreshAccounts(t.Context())
	if quota.KindOf(err) != quota.ErrKindParse {
		t.Fatalf("err = %v, want parse_error", err)
	}
	if fleetQuotaLoginMissingKinds[quota.KindOf(err)] {
		t.Fatal("an unreadable output must not read as a missing login")
	}
}

// A host with no omp, or none holding a Command Code account, has no login here;
// that is not a fault and is not logged.
func TestCommandcodeNoOmpIsNoLoginAndSilent(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{err: exec.ErrNotFound}
	source := commandcodeTestSource(claudeStore(t, store.server.URL), omp)
	var warnings []string
	source.Warn = func(m string) { warnings = append(warnings, m) }
	_, _, err := source.RefreshAccounts(t.Context())
	if quota.KindOf(err) != quota.ErrKindCredentials || !fleetQuotaLoginMissingKinds[quota.KindOf(err)] || len(warnings) != 0 {
		t.Fatalf("err = %v warnings %q, want credentials and silence", err, warnings)
	}
}

// With no shared store the node still measures directly and reads every account.
func TestCommandcodeWithoutStoreReadsEveryAccountDirectly(t *testing.T) {
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	source := commandcodeTestSource("/nonexistent", omp)
	readings, measured, err := source.RefreshAccounts(t.Context())
	if err != nil || !measured || len(readings) != 2 || readings[0].Key != commandcodeKeyA || !readings[0].MeasuredAt.Equal(commandcodeFetched) {
		t.Fatalf("readings %+v measured=%v err=%v", readings, measured, err)
	}
}

// `panewire quota refresh commandcode` prints one line per account.
func TestQuotaCLIRefreshCommandcodePrintsEachAccount(t *testing.T) {
	store := newAccountStore(t)
	omp := &ompFake{output: commandcodeFixture(t, "omp_usage_two_accounts.json")}
	spawn := claudeStore(t, store.server.URL)
	previous := quotaCLISource
	t.Cleanup(func() { quotaCLISource = previous })
	quotaCLISource = func(string) *nativeQuotaSource { return commandcodeTestSource(spawn, omp) }
	var out, errOut bytes.Buffer
	if code := runQuotaCLI([]string{"refresh", "commandcode"}, &out, &errOut); code != 0 {
		t.Fatalf("rc=%d stderr=%s", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("output %q, want one line per account", out.String())
	}
	var a, b quotaReading
	if json.Unmarshal([]byte(lines[0]), &a) != nil || json.Unmarshal([]byte(lines[1]), &b) != nil || a.Account != commandcodeFPA || b.Account != commandcodeFPB || a.Pool != "commandcode" || len(a.Buckets) != 2 {
		t.Fatalf("lines %q", lines)
	}
}
