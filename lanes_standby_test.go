package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func standby(valueMachine, valuePane string) *reportRelayStandby {
	return &reportRelayStandby{Machine: valueMachine, Pane: valuePane}
}

func standbyGETLane(t *testing.T, hub *HubServer, lane string) map[string]any {
	t.Helper()
	for _, route := range lanesProjectionResponse(t, lanesProjectionGet(hub, lanesProjectionOperatorToken)) {
		if route["lane"] == lane {
			return route
		}
	}
	t.Fatalf("lane %q missing from GET", lane)
	return nil
}

func TestLanesStandbyAC1ParsePresent(t *testing.T) {
	routes, err := parseReportRelayRoutes([]byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := routes["lane-a"].Standby, standby("machine-b", "w2:p2"); !reflect.DeepEqual(got, want) {
		t.Fatalf("standby=%+v want=%+v", got, want)
	}
}

func TestLanesStandbyAC2ExistingFixtureUnchanged(t *testing.T) {
	routes, err := parseReportRelayRoutes([]byte(r20t8Lanes))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]reportRelayRoute{
		"lane-source":      {Machine: "host-a", Pane: "w1:p1", Parent: "lane-destination"},
		"lane-destination": {Machine: "host-b", Pane: "w1:p2"},
	}
	if !reflect.DeepEqual(routes, want) {
		t.Fatalf("routes=%+v want=%+v", routes, want)
	}
}

func TestLanesStandbyAC3AndAC4InvalidEntriesAreDroppedIndividually(t *testing.T) {
	for _, test := range []struct {
		name    string
		standby string
	}{
		{name: "AC3 invalid machine", standby: `{"machine":"Machine-A!","pane":"w2:p2"}`},
		{name: "AC4 empty pane", standby: `{"machine":"machine-b","pane":""}`},
		{name: "AC4b pane longer than 128", standby: `{"machine":"machine-b","pane":"` + strings.Repeat("p", 129) + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			contents := `{"lanes":{"lane-good":{"machine":"machine-a","pane":"w1:p1"},"lane-invalid":{"machine":"machine-a","pane":"w1:p2","standby":` + test.standby + `}}}`
			routes, err := parseReportRelayRoutes([]byte(contents))
			if err != nil {
				t.Fatal(err)
			}
			if _, present := routes["lane-invalid"]; present {
				t.Fatalf("invalid lane was retained: %+v", routes)
			}
			if got := routes["lane-good"]; got.Machine != "machine-a" || got.Pane != "w1:p1" {
				t.Fatalf("valid lane was changed: %+v", got)
			}
		})
	}
}

func TestLanesStandbyAC5SinkClearsStandby(t *testing.T) {
	routes, err := parseReportRelayRoutes([]byte(`{"lanes":{"lane-sink":{"sink":true,"standby":{"machine":"machine-b","pane":"w2:p2"}},"lane-empty":{"machine":"machine-a","pane":"","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	for lane, route := range routes {
		if !route.Sink || route.Standby != nil {
			t.Fatalf("lane=%s route=%+v", lane, route)
		}
	}
}

func TestLanesStandbyAC6GETProjectionOmitsAbsentKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-with":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}},"lane-without":{"machine":"machine-a","pane":"w1:p2"}}}`)
	writer := lanesProjectionGet(lanesWriteHub(t, path), "fixture-operator-token")
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	var response struct {
		Lanes []json.RawMessage `json:"lanes"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	byLane := make(map[string]json.RawMessage, len(response.Lanes))
	for _, raw := range response.Lanes {
		var route struct {
			Lane string `json:"lane"`
		}
		if err := json.Unmarshal(raw, &route); err != nil {
			t.Fatal(err)
		}
		byLane[route.Lane] = raw
	}
	if !bytes.Contains(byLane["lane-with"], []byte(`"standby":{"machine":"machine-b","pane":"w2:p2"}`)) {
		t.Fatalf("standby route raw JSON=%s", byLane["lane-with"])
	}
	if bytes.Contains(byLane["lane-without"], []byte(`"standby"`)) {
		t.Fatalf("standby-free route emitted standby key: %s", byLane["lane-without"])
	}
}

func TestLanesStandbyAC7OmittedPUTPreservesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`)
	hub := lanesWriteHub(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-b","pane":"w2:p2"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if got := standbyGETLane(t, hub, "lane-a")["standby"]; !reflect.DeepEqual(got, map[string]any{"machine": "machine-b", "pane": "w2:p2"}) {
		t.Fatalf("GET standby=%v", got)
	}
}

func TestLanesStandbyAC8NullPUTRemoves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`)
	hub := lanesWriteHub(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1","standby":null}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if route := standbyGETLane(t, hub, "lane-a"); strings.Contains(writer.Body.String(), `"standby"`) || route["standby"] != nil {
		t.Fatalf("standby was not removed: response=%s GET=%v", writer.Body.String(), route)
	}
}

func TestLanesStandbyAC9ValidPUTStoresAndGETShows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	hub := lanesWriteHub(t, path)
	writer := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	if got := standbyGETLane(t, hub, "lane-a")["standby"]; !reflect.DeepEqual(got, map[string]any{"machine": "machine-b", "pane": "w2:p2"}) {
		t.Fatalf("GET standby=%v", got)
	}
}

func TestLanesStandbyAC10InvalidPUTDoesNotChangeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	original := []byte(`{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	writer := lanesWriteRequest(t, lanesWriteHub(t, path), http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"Machine-A!","pane":"w2:p2"}}`)
	if writer.Code < http.StatusBadRequest || writer.Code >= http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("invalid PUT changed lanes file: got=%s want=%s", got, original)
	}
}

func TestLanesStandbyAC11UnknownPUTFieldIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1"}}}`)
	writer := lanesWriteRequest(t, lanesWriteHub(t, path), http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-a","pane":"w1:p1","bogus":1}`)
	if writer.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
}

func TestLanesStandbyAC12FlipRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","standby":{"machine":"machine-b","pane":"w2:p2"}}}}`)
	hub := lanesWriteHub(t, path)
	flip := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-b","pane":"w2:p2"}`)
	if flip.Code != http.StatusOK {
		t.Fatalf("flip status=%d body=%s", flip.Code, flip.Body.String())
	}
	if got := standbyGETLane(t, hub, "lane-a"); got["machine"] != "machine-b" || got["pane"] != "w2:p2" || !reflect.DeepEqual(got["standby"], map[string]any{"machine": "machine-b", "pane": "w2:p2"}) {
		t.Fatalf("after flip GET=%v", got)
	}
	reverse := lanesWriteRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", "fixture-operator-token", `{"machine":"machine-b","pane":"w2:p2","standby":{"machine":"machine-a","pane":"w1:p1"}}`)
	if reverse.Code != http.StatusOK {
		t.Fatalf("reverse status=%d body=%s", reverse.Code, reverse.Body.String())
	}
	got := standbyGETLane(t, hub, "lane-a")
	if got["machine"] != "machine-b" || got["pane"] != "w2:p2" || !reflect.DeepEqual(got["standby"], map[string]any{"machine": "machine-a", "pane": "w1:p1"}) {
		t.Fatalf("after reverse GET=%v", got)
	}
}
