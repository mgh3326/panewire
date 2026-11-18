package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

// agyFakeHerdr answers the node's herdr calls for one agent list: the rows
// given, a screen for `agent read`, or a read error. Every read is counted.
type agyFakeHerdr struct {
	mu      sync.Mutex
	list    string
	panes   string
	screen  string
	readErr bool
	reads   int
}

func (fake *agyFakeHerdr) run(_ context.Context, args ...string) ([]byte, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(args) >= 2 && args[0] == "agent" && args[1] == "read" {
		fake.reads++
		if fake.readErr {
			return nil, errAgyFakeRead
		}
		return []byte(fake.screen), nil
	}
	switch strings.Join(args, " ") {
	case "agent list":
		return []byte(fake.list), nil
	case "pane list":
		return []byte(fake.panes), nil
	}
	return nil, errAgyFakeRead
}

var errAgyFakeRead = errors.New("herdr read failed")

// agyMidToolRunScreen is an agy pane's bottom during a long tool run,
// reconstructed from herdr's agy rules (internal/agybusy holds the evidence).
const agyMidToolRunScreen = ` ⏺ Bash(go test ./... -count=1)
   ⎿  (output truncated)

 ⠙ Running tools · 42s
╭──────────────────────────────────────────────────────────────────────────────╮
│ >                                                                            │
╰──────────────────────────────────────────────────────────────────────────────╯
   esc to cancel
`

// agyEndOfTurnScreen is an agy pane back at its prompt.
const agyEndOfTurnScreen = ` ⏺ The fleet package passes.

╭──────────────────────────────────────────────────────────────────────────────╮
│ >                                                                            │
╰──────────────────────────────────────────────────────────────────────────────╯
 ? for shortcuts
`

func agyRow(pane, agent, status string) string {
	return `{"pane_id":"` + pane + `","agent":"` + agent + `","agent_status":"` + status + `","cwd":"/w"}`
}

func agyListing(rows ...string) string {
	return `{"result":{"agents":[` + strings.Join(rows, ",") + `]}}`
}

func agyStatusOf(t *testing.T, fake *agyFakeHerdr, pane string) string {
	t.Helper()
	client := &HubClient{nodeHerdr: fake.run}
	result, code := client.nodeAgentsListInSession(context.Background(), "worker")
	if code != "" {
		t.Fatalf("agent listing code = %q", code)
	}
	for _, row := range result.Agents {
		if row.PaneID == pane {
			return row.AgentStatus
		}
	}
	t.Fatalf("pane %s not in the listing", pane)
	return ""
}

func TestHubListingJudgesAgyDoneWithMidToolRunScreenAsWorking(t *testing.T) {
	fake := &agyFakeHerdr{list: agyListing(agyRow("w:p1", "agy", "done")), screen: agyMidToolRunScreen}
	if got := agyStatusOf(t, fake, "w:p1"); got != "working" {
		t.Fatalf("agy done, mid-tool-run screen: listed as %q, want working", got)
	}
	if fake.reads != 1 {
		t.Fatalf("screen reads = %d, want 1", fake.reads)
	}
}

func TestHubListingKeepsAgyDoneAtARealEndOfTurn(t *testing.T) {
	fake := &agyFakeHerdr{list: agyListing(agyRow("w:p1", "agy", "done")), screen: agyEndOfTurnScreen}
	if got := agyStatusOf(t, fake, "w:p1"); got != "done" {
		t.Fatalf("agy done, end-of-turn screen: listed as %q, want done", got)
	}
}

func TestHubListingJudgesUnreadableAgyScreenAsWorking(t *testing.T) {
	// Mutant guard: an unreadable screen must not read as idle.
	fake := &agyFakeHerdr{list: agyListing(agyRow("w:p1", "agy", "idle")), readErr: true}
	if got := agyStatusOf(t, fake, "w:p1"); got != "working" {
		t.Fatalf("agy idle, unreadable screen: listed as %q, want working", got)
	}
}

func TestHubListingLeavesDevinDoneAndNoScreenRead(t *testing.T) {
	fake := &agyFakeHerdr{list: agyListing(agyRow("w:p1", "devin", "done")), screen: agyMidToolRunScreen}
	if got := agyStatusOf(t, fake, "w:p1"); got != "done" {
		t.Fatalf("devin done: listed as %q, want done", got)
	}
	if fake.reads != 0 {
		t.Fatalf("devin pane screen reads = %d, want 0", fake.reads)
	}
}

func TestHubPaneListJudgesAgyPanesToo(t *testing.T) {
	fake := &agyFakeHerdr{
		list:   agyListing(),
		panes:  `{"result":{"panes":[` + agyRow("w:p2", "agy", "done") + `]}}`,
		screen: agyMidToolRunScreen,
	}
	client := &HubClient{nodeHerdr: fake.run}
	result, code := client.nodeAgentsListInSession(context.Background(), "worker")
	if code != "" || !result.PanesOK || len(result.Panes) != 1 {
		t.Fatalf("pane listing = %+v, code %q", result, code)
	}
	if got := result.Panes[0].AgentStatus; got != "working" {
		t.Fatalf("agy pane in the pane list: %q, want working", got)
	}
}

// The daemon's event path names no agent, so agyEventStatus asks agent.get
// for the harness and reads the screen over the socket (agent.read).
func TestDaemonEventJudgesAgyDoneByItsScreen(t *testing.T) {
	cases := []struct {
		name, harness, screen, status, want string
		reads                               bool
	}{
		{"agy mid-tool-run", "agy", agyMidToolRunScreen, "done", "working", true},
		{"agy end of turn", "agy", agyEndOfTurnScreen, "done", "done", true},
		{"agy unreadable", "agy", "", "idle", "working", true},
		{"devin is not read", "devin", agyMidToolRunScreen, "done", "done", false},
		{"working is not read", "agy", agyEndOfTurnScreen, "working", "working", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			reads := 0
			server := startFakeHerdrPaneServer(t, func(params map[string]any) string {
				if _, ok := params["source"]; ok {
					mu.Lock()
					reads++
					mu.Unlock()
					if tc.screen == "" {
						return `"result":{"type":"text"}`
					}
					return `"result":{"text":` + jsonString(tc.screen) + `}`
				}
				return `"result":{"agent":{"agent":"` + tc.harness + `","pane_id":"w:p1","agent_status":"` + tc.status + `"}}`
			})
			client, err := NewHerdrClient(server.socket)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			got := client.agyEventStatus(context.Background(), "w:p1", tc.status)
			if got != tc.want {
				t.Fatalf("agyEventStatus = %q, want %q", got, tc.want)
			}
			mu.Lock()
			defer mu.Unlock()
			wantReads := 0
			if tc.reads {
				wantReads = 1
			}
			if reads != wantReads {
				t.Fatalf("screen reads = %d, want %d", reads, wantReads)
			}
		})
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
