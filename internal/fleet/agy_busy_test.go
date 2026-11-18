package fleet

import (
	"context"
	"strings"
	"testing"
)

// agyMidToolRunScreen is the bottom of an agy pane during a long tool run,
// reconstructed from herdr's agy rules (internal/agybusy holds the evidence).
const agyMidToolRunScreen = ` ⏺ Bash(go test ./internal/fleet/ -count=1)
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

// agyStatusFor runs App.agents against one herdr agent row and a screen read
// (readOut, readCode), and reports the status the fleet judges the pane by and
// whether herdr was asked for the screen at all.
func agyStatusFor(t *testing.T, row, readOut string, readCode int) (string, int) {
	t.Helper()
	a := testApp(t)
	reads := 0
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		if c.Name != "herdr" {
			return Result{}
		}
		switch strings.Join(c.Args, " ") {
		case "agent list":
			return Result{Out: `{"result":{"agents":[` + row + `]}}`}
		}
		if len(c.Args) >= 2 && c.Args[0] == "agent" && c.Args[1] == "read" {
			reads++
			return Result{Out: readOut, Code: readCode}
		}
		return Result{Code: 2, Err: "unexpected herdr call " + strings.Join(c.Args, " ")}
	}
	agents, err := a.agents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 {
		t.Fatalf("agents = %d rows, want 1", len(agents))
	}
	return agents[0].Status, reads
}

func TestAgentsJudgesAgyDoneWithBusyScreenAsWorking(t *testing.T) {
	row := `{"pane_id":"w:p1","agent":"agy","agent_status":"done","cwd":"/w"}`
	status, reads := agyStatusFor(t, row, agyMidToolRunScreen, 0)
	if status != "working" || reads != 1 {
		t.Fatalf("agy done with a mid-tool-run screen: status=%q reads=%d, want working after one read", status, reads)
	}
}

func TestAgentsKeepsAgyDoneAtAnEndOfTurn(t *testing.T) {
	row := `{"pane_id":"w:p1","agent":"agy","agent_status":"done","cwd":"/w"}`
	status, reads := agyStatusFor(t, row, agyEndOfTurnScreen, 0)
	if status != "done" || reads != 1 {
		t.Fatalf("agy done at an end of turn: status=%q reads=%d, want done after one read", status, reads)
	}
}

func TestAgentsJudgesUnreadableAgyScreenAsWorking(t *testing.T) {
	// Mutant guard: an unreadable screen must not read as idle.
	row := `{"pane_id":"w:p1","agent":"agy","agent_status":"idle","cwd":"/w"}`
	status, _ := agyStatusFor(t, row, "pane gone", 1)
	if status != "working" {
		t.Fatalf("agy idle with an unreadable screen: status=%q, want working", status)
	}
}

func TestAgentsLeavesDevinAndWorkingStatusesAlone(t *testing.T) {
	devin := `{"pane_id":"w:p1","agent":"devin","agent_status":"done","cwd":"/w"}`
	if status, reads := agyStatusFor(t, devin, agyMidToolRunScreen, 0); status != "done" || reads != 0 {
		t.Fatalf("devin done: status=%q reads=%d, want done with no screen read", status, reads)
	}
	agyWorking := `{"pane_id":"w:p1","agent":"agy","agent_status":"working","cwd":"/w"}`
	if status, reads := agyStatusFor(t, agyWorking, agyEndOfTurnScreen, 0); status != "working" || reads != 0 {
		t.Fatalf("agy working: status=%q reads=%d, want working with no screen read", status, reads)
	}
}
