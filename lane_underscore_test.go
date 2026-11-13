package panewire

import (
	"bytes"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaneUnderscoreValidators(t *testing.T) {
	validators := []struct {
		name  string
		valid func(string) bool
		upper bool
	}{
		{"hub lane, control plane and lanes CLI", LaneNamePattern.MatchString, false},
		{"relay owner and inbox", LaneNamePattern.MatchString, false},
		{"agent label", hubAgentLabelPattern.MatchString, true},
		{"node registration projection", func(s string) bool {
			return validHubLaneProjection(hubLaneProjection{Lane: s, Machine: "host-a", Pane: "w1:p1"})
		}, false},
	}
	for _, v := range validators {
		for _, c := range []struct {
			name string
			want bool
		}{{"auto_trader-design", true}, {"a_b", true}, {"A_B", v.upper}, {"a.b", false}, {"a b", false}, {"a/b", false}} {
			t.Run(v.name+"/"+c.name, func(t *testing.T) {
				if got := v.valid(c.name); got != c.want {
					t.Errorf("valid(%q)=%v want %v", c.name, got, c.want)
				}
			})
		}
	}
}

func TestLaneUnderscoreHubPut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, `{"lanes":{}}`)
	hub := lanesNodeHub(t, path, nil)
	w := lanesNodePut(t, hub, "auto_trader-design", `{"machine":"machine-a","pane":"w1:p1"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestLaneUnderscoreEventRouted(t *testing.T) {
	_, client, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"auto_trader-design":{"machine":"host-a","pane":"w1:p1"}}}`, client, nil)
	dest := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	hub.nodes["host-a"] = &hubNodeRecord{agent: dest}
	body := r24IngressBody("event-1", "hello")
	body["lane"] = "auto_trader-design"
	w := r24PostIngress(t, hub, "op", body)
	if w.Code != http.StatusCreated || !r24DecodeIngressResponse(t, w).Routed {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	select {
	case d := <-dest.relays:
		if d.Pane != "w1:p1" {
			t.Fatalf("directive=%+v", d)
		}
	default:
		t.Fatal("no routed event")
	}
}

func TestLaneUnderscoreInboxParses(t *testing.T) {
	var out, errout bytes.Buffer
	code := runInboxCLI([]string{"list", "--lane", "auto_trader-design"}, &out, &errout, hubCLIDeps{SocketPath: filepath.Join(t.TempDir(), "missing.sock")})
	if code == ExitUsage || strings.Contains(errout.String(), "usage:") {
		t.Fatalf("code=%d stderr=%s", code, errout.String())
	}
}
