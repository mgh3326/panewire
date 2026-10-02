package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// AC1-AC3 for task #1101: one node token must not be able to fill the shared
// lanes file and block every other machine's registration. A node owns at
// most --lanes-node-row-cap rows (default 64), a node write may never grow
// the file into the operator reserve at its tail, and any write whose result
// would exceed lanesFileMaxBytes is a named refusal instead of a 500.

// lanesNodeFileBytes reads the raw file so a refused write can be checked
// byte-identical, not merely semantically equal.
func lanesNodeFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}

func lanesNodeBackups(t *testing.T, path string) []string {
	t.Helper()
	backups, err := filepath.Glob(path + ".bak-*")
	if err != nil {
		t.Fatal(err)
	}
	return backups
}

// AC1: at the row cap a node's next create is refused with the named 4xx and
// leaves the file untouched; updates and deletes of its own rows and every
// other caller's creates are unaffected.
func TestLanesNodeRowCapRefusesCreateBeyondCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"},"lane-b":{"machine":"machine-b","pane":"w9:p9"}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) {
		config.LanesNodeRowCap = 3
	})

	// Two creates reach the cap of three: the node already owned lane-a.
	for _, lane := range []string{"lane-a2", "lane-a3"} {
		if writer := lanesNodePut(t, hub, lane, `{"machine":"machine-a","pane":"w1:p2"}`); writer.Code != http.StatusCreated {
			t.Fatalf("node create %s status=%d body=%s", lane, writer.Code, writer.Body.String())
		}
	}
	before := lanesNodeFileBytes(t, path)
	backupsBefore := lanesNodeBackups(t, path)
	writer := lanesNodePut(t, hub, "lane-a4", `{"machine":"machine-a","pane":"w1:p3"}`)
	if writer.Code != http.StatusTooManyRequests || writer.Body.String() != `{"error":"lane_quota_exceeded"}`+"\n" {
		t.Fatalf("node create beyond the row cap status=%d body=%s want 429 lane_quota_exceeded", writer.Code, writer.Body.String())
	}
	if after := lanesNodeFileBytes(t, path); !reflect.DeepEqual(after, before) {
		t.Fatalf("cap-refused create changed the lanes file: %q", after)
	}
	if backups := lanesNodeBackups(t, path); !reflect.DeepEqual(backups, backupsBefore) {
		t.Fatalf("cap-refused create wrote backups: %v", backups)
	}
	// A refused write is not an existence oracle the other direction either:
	// a scoped-field refusal still answers 403, not 429.
	lanesNodeForbidden(t, lanesNodePut(t, hub, "lane-a5", `{"machine":"machine-b","pane":"w9:p2"}`), "foreign-machine create at cap")

	// Updating a row the node already owns still succeeds at the cap.
	if writer := lanesNodePut(t, hub, "lane-a", `{"machine":"machine-a","pane":"w1:p9"}`); writer.Code != http.StatusOK {
		t.Fatalf("node update own lane at cap status=%d body=%s", writer.Code, writer.Body.String())
	}
	// Deleting back under the cap re-opens creates.
	if writer := lanesNodeDelete(t, hub, "lane-a3"); writer.Code != http.StatusOK {
		t.Fatalf("node delete own lane at cap status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := lanesNodePut(t, hub, "lane-a4", `{"machine":"machine-a","pane":"w1:p3"}`); writer.Code != http.StatusCreated {
		t.Fatalf("node create after delete status=%d body=%s", writer.Code, writer.Body.String())
	}
	// Another machine's node and the operator are never throttled by it.
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b2", lanesNodeBToken, "machine-b", `{"machine":"machine-b","pane":"w9:p2"}`); writer.Code != http.StatusCreated {
		t.Fatalf("machine-b node create status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-op", lanesNodeOperatorToken, "", `{"machine":"machine-a","pane":"w1:p8"}`); writer.Code != http.StatusCreated {
		t.Fatalf("operator create while machine-a at cap status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// AC1 deploy edge: a node already over the cap — more rows than the new cap,
// written before deploy — keeps update and delete of its own rows but cannot
// create until it shrinks back under the cap.
func TestLanesNodeOverCapKeepsUpdateAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a1":{"machine":"machine-a","pane":"w1:p1"},"lane-a2":{"machine":"machine-a","pane":"w1:p2"},"lane-a3":{"machine":"machine-a","pane":"w1:p3"},"lane-a4":{"machine":"machine-a","pane":"w1:p4"},"lane-a5":{"machine":"machine-a","pane":"w1:p5"}}}`)
	hub := lanesNodeHub(t, path, func(config *HubServerConfig) {
		config.LanesNodeRowCap = 3
	})

	writer := lanesNodePut(t, hub, "lane-new", `{"machine":"machine-a","pane":"w1:p6"}`)
	if writer.Code != http.StatusTooManyRequests || writer.Body.String() != `{"error":"lane_quota_exceeded"}`+"\n" {
		t.Fatalf("over-cap node create status=%d body=%s want 429 lane_quota_exceeded", writer.Code, writer.Body.String())
	}
	if writer := lanesNodePut(t, hub, "lane-a1", `{"machine":"machine-a","pane":"w1:p9"}`); writer.Code != http.StatusOK {
		t.Fatalf("over-cap node update status=%d body=%s", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeDelete(t, hub, "lane-a5"); writer.Code != http.StatusOK {
		t.Fatalf("over-cap node delete status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// AC2: the original #1037-part-A attack — one node token creates rows until
// refused — can no longer make another machine's create or an operator
// create fail. Phase A repeats the tester's many-small-rows shape against
// the shipped default; phase B uses long-field rows under a raised cap so
// the byte headroom, not the row cap, is what stops the fill.
func TestLanesNodeFillCannotBlockOperatorOrMachines(t *testing.T) {
	t.Run("row cap stops small-row fill", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lanes.json")
		hub := lanesNodeHub(t, path, nil)

		refused := false
		for index := 0; index < 512; index++ {
			writer := lanesNodePut(t, hub, fmt.Sprintf("fill-%02d", index), `{"machine":"machine-a","pane":"w1:p1"}`)
			if writer.Code == http.StatusCreated {
				continue
			}
			if writer.Code != http.StatusTooManyRequests || writer.Body.String() != `{"error":"lane_quota_exceeded"}`+"\n" {
				t.Fatalf("small-row fill refusal status=%d body=%s want 429 lane_quota_exceeded", writer.Code, writer.Body.String())
			}
			refused = true
			break
		}
		if !refused {
			t.Fatal("node created 512 rows without a refusal")
		}
		if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-b1", lanesNodeBToken, "machine-b", `{"machine":"machine-b","pane":"w9:p1"}`); writer.Code != http.StatusCreated {
			t.Fatalf("machine-b node create after fill status=%d body=%s", writer.Code, writer.Body.String())
		}
		if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-op", lanesNodeOperatorToken, "", `{"machine":"machine-a","pane":"w1:p8"}`); writer.Code != http.StatusCreated {
			t.Fatalf("operator create after fill status=%d body=%s", writer.Code, writer.Body.String())
		}
	})

	t.Run("byte headroom protects the operator", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "lanes.json")
		hub := lanesNodeHub(t, path, func(config *HubServerConfig) {
			// Raised far above the attack so the byte budget, not the row
			// cap, is the refusal under test.
			config.LanesNodeRowCap = 1 << 20
		})

		pane := "w1:p" + strings.Repeat("a", 124)
		body := fmt.Sprintf(`{"machine":"machine-a","pane":"%s","standby":{"machine":"machine-a","pane":"%s"}}`, pane, pane)
		refused := false
		var before []byte
		for index := 0; index < 4096; index++ {
			writer := lanesNodePut(t, hub, fmt.Sprintf("fill-%04d", index), body)
			if writer.Code == http.StatusCreated {
				continue
			}
			if writer.Code != http.StatusRequestEntityTooLarge || writer.Body.String() != `{"error":"lanes_file_full"}`+"\n" {
				t.Fatalf("node fill refusal status=%d body=%s want 413 lanes_file_full", writer.Code, writer.Body.String())
			}
			before = lanesNodeFileBytes(t, path)
			if len(before) > lanesNodeFileMaxBytes {
				t.Fatalf("node writes left a %d-byte file, over the %d-byte node limit", len(before), lanesNodeFileMaxBytes)
			}
			refused = true
			break
		}
		if !refused {
			t.Fatal("node long-field fill was never refused")
		}
		// A retried refused write changes nothing.
		if writer := lanesNodePut(t, hub, "fill-final", body); writer.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("retry status=%d body=%s", writer.Code, writer.Body.String())
		}
		if after := lanesNodeFileBytes(t, path); !reflect.DeepEqual(after, before) {
			t.Fatal("refused node write changed the lanes file")
		}
		// The reserve is intact: operator creates still succeed.
		for _, lane := range []string{"lane-op1", "lane-op2"} {
			if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/"+lane, lanesNodeOperatorToken, "", `{"machine":"machine-b","pane":"w9:p1"}`); writer.Code != http.StatusCreated {
				t.Fatalf("operator create after node filled its share status=%d body=%s want 201", writer.Code, writer.Body.String())
			}
		}
	})
}

// AC3: a write whose result would exceed lanesFileMaxBytes answers 413
// lanes_file_full — for the operator and for a node alike — and the file on
// disk stays byte-identical.
func TestLanesWriteFileFullIsNamedRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	// Build the largest lane set that still serializes under the cap: the
	// exact row the PUT below writes is the probe, so adding it is the write
	// that provably does not fit.
	routes := map[string]reportRelayRoute{}
	for index := 0; ; index++ {
		candidate := make(map[string]reportRelayRoute, len(routes)+1)
		for name, route := range routes {
			candidate[name] = route
		}
		candidate["one-more"] = reportRelayRoute{Machine: "machine-b", Pane: "w9:p1"}
		contents, err := encodeLanesFile(lanesFileSnapshot{Routes: candidate})
		if err != nil {
			t.Fatal(err)
		}
		if len(contents) > lanesFileMaxBytes {
			break
		}
		// f-XXXXXX names are the same length as the probe row's one-more, so
		// the written set can never exceed the cap it was measured against.
		routes[fmt.Sprintf("f-%06d", index)] = reportRelayRoute{Machine: "machine-b", Pane: "w9:p1"}
	}
	contents, err := encodeLanesFile(lanesFileSnapshot{Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	lanesProjectionWrite(t, path, string(contents))
	hub := lanesNodeHub(t, path, nil)
	before := lanesNodeFileBytes(t, path)

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/one-more", lanesNodeOperatorToken, "", `{"machine":"machine-b","pane":"w9:p1"}`)
	if writer.Code != http.StatusRequestEntityTooLarge || writer.Body.String() != `{"error":"lanes_file_full"}`+"\n" {
		t.Fatalf("oversized operator write status=%d body=%s want 413 lanes_file_full", writer.Code, writer.Body.String())
	}
	if after := lanesNodeFileBytes(t, path); !reflect.DeepEqual(after, before) {
		t.Fatal("file-full operator write changed the lanes file")
	}
	if backups := lanesNodeBackups(t, path); len(backups) != 0 {
		t.Fatalf("file-full write wrote backups: %v", backups)
	}
	// A node create at a full file meets its own budget first and is refused
	// the same way — never a 500.
	writer = lanesNodePut(t, hub, "one-more", `{"machine":"machine-a","pane":"w1:p1"}`)
	if writer.Code != http.StatusRequestEntityTooLarge || writer.Body.String() != `{"error":"lanes_file_full"}`+"\n" {
		t.Fatalf("node create at full file status=%d body=%s want 413 lanes_file_full", writer.Code, writer.Body.String())
	}
	// The file is not wedged: deletes still shrink it.
	if writer := lanesNodeRequest(t, hub, http.MethodDelete, "/v1/lanes/f-000000", lanesNodeOperatorToken, "", ""); writer.Code != http.StatusOK {
		t.Fatalf("operator delete at full file status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// A node DELETE of its own row normally shrinks the file, but when the file
// on disk is compact hand-edited JSON the canonical re-encode is larger —
// and that growth must not spend the operator reserve either. The operator
// is exempt: its delete of the same row proceeds.
func TestLanesNodeDeleteCannotSpendOperatorReserve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	routes := map[string]reportRelayRoute{
		"lane-a": {Machine: "machine-a", Pane: "w1:p1"},
	}
	encodedAfterDelete := func() []byte {
		candidate := make(map[string]reportRelayRoute, len(routes))
		for name, route := range routes {
			if name != "lane-a" {
				candidate[name] = route
			}
		}
		contents, err := encodeLanesFile(lanesFileSnapshot{Routes: candidate})
		if err != nil {
			t.Fatal(err)
		}
		return contents
	}
	var compact, encoded []byte
	for index := 0; ; index++ {
		if index > 8192 {
			t.Fatal("could not reach the reserve band with compact rows")
		}
		compact, _ = json.Marshal(struct {
			Lanes map[string]reportRelayRoute `json:"lanes"`
		}{Lanes: routes})
		encoded = encodedAfterDelete()
		if len(encoded) > lanesNodeFileMaxBytes {
			break
		}
		routes[fmt.Sprintf("f-%05d", index)] = reportRelayRoute{Machine: "machine-b", Pane: "w9:p9"}
	}
	if len(encoded) > lanesFileMaxBytes || len(encoded) <= len(compact) {
		t.Fatalf("fixture outside the reserve band: compact=%d encoded-after-delete=%d node=%d max=%d",
			len(compact), len(encoded), lanesNodeFileMaxBytes, lanesFileMaxBytes)
	}
	lanesProjectionWrite(t, path, string(compact))
	hub := lanesNodeHub(t, path, nil)
	before := lanesNodeFileBytes(t, path)

	writer := lanesNodeDelete(t, hub, "lane-a")
	if writer.Code != http.StatusRequestEntityTooLarge || writer.Body.String() != `{"error":"lanes_file_full"}`+"\n" {
		t.Fatalf("node delete re-encode into reserve status=%d body=%s want 413 lanes_file_full", writer.Code, writer.Body.String())
	}
	if after := lanesNodeFileBytes(t, path); !reflect.DeepEqual(after, before) {
		t.Fatal("reserve-refused node delete changed the lanes file")
	}
	// The operator is not bounded by the node reserve: the same delete —
	// still under lanesFileMaxBytes — succeeds.
	if writer := lanesNodeRequest(t, hub, http.MethodDelete, "/v1/lanes/lane-a", lanesNodeOperatorToken, "", ""); writer.Code != http.StatusOK {
		t.Fatalf("operator delete of the same row status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// The cap is hub configuration: a negative value is rejected and a zero
// value selects the default.
func TestLanesNodeRowCapConfig(t *testing.T) {
	if _, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "fixture-operator-token"},
		LanesNodeRowCap: -1,
	}); err == nil {
		t.Fatal("negative LanesNodeRowCap was accepted")
	}
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": "fixture-operator-token"},
		LanesNodeRowCap: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hub.Close() }()
	if hub.lanesNodeRowCap != defaultLanesNodeRowCap {
		t.Fatalf("default cap=%d want %d", hub.lanesNodeRowCap, defaultLanesNodeRowCap)
	}
}

// The lanes CLI names the hub's refusal code so a quota refusal reads
// differently from a generic rejection.
func TestLanesCLIPrintsHubErrorCode(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "quota", status: http.StatusTooManyRequests, body: `{"error":"lane_quota_exceeded"}`, want: "lanes rejected by hub: lane_quota_exceeded"},
		{name: "file full", status: http.StatusRequestEntityTooLarge, body: `{"error":"lanes_file_full"}`, want: "lanes rejected by hub: lanes_file_full"},
		{name: "mismatch", status: http.StatusForbidden, body: `{"error":"lane_machine_mismatch"}`, want: "lanes rejected by hub: lane_machine_mismatch"},
		{name: "authority use hint", status: http.StatusConflict, body: `{"error":"authority_lane_direct_write","use":"POST /v1/control-plane/transfer"}`, want: "lanes rejected by hub: authority_lane_direct_write (use: POST /v1/control-plane/transfer)"},
		{name: "server failure code", status: http.StatusInternalServerError, body: `{"error":"lanes_write_failed"}`, want: "lanes unavailable: lanes_write_failed"},
		{name: "unparseable body", status: http.StatusForbidden, body: `denied`, want: "lanes rejected by hub"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			envPath := filepath.Join(t.TempDir(), "node.env")
			if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=machine-a\nHUB_TOKEN=fixture-node-token\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var stdout, stderr strings.Builder
			code := runLanesCLI([]string{"add", "lane-a", "--machine", "machine-a", "--pane", "w1:p1", "--hub-url", server.URL, "--hub-token-env", envPath}, &stdout, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true})
			wantExit := ExitConditionInvalid
			if test.status >= 500 {
				wantExit = ExitInternal
			}
			if code != wantExit || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("code=%d stderr=%q want exit %d and %q", code, stderr.String(), wantExit, test.want)
			}
		})
	}
}
