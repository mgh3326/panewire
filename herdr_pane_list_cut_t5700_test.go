package panewire

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
)

var errUnexpectedHerdrCall = errors.New("unexpected herdr call")

// cutPaneList answers the first `pane list` with cut, a herdr answer stopped at
// the 8192 byte boundary, and every later call from the fake (herdrpanes).
type cutPaneList struct {
	mu    sync.Mutex
	fake  *nodeFakeHerdr
	cut   []string // successive cut answers; once used up, the fake answers
	lists int      // `pane list` calls so far
}

func (c *cutPaneList) run(ctx context.Context, args ...string) ([]byte, error) {
	if strings.Join(args, " ") == "pane list" {
		c.mu.Lock()
		c.lists++
		var cut string
		if len(c.cut) > 0 {
			cut, c.cut = c.cut[0], c.cut[1:]
		}
		c.mu.Unlock()
		if cut != "" {
			return []byte(cut), nil
		}
	}
	return c.fake.run(ctx, args...)
}

func (c *cutPaneList) listCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

// cutPaneAnswer is a `pane list` answer of herdr's cut size: the bytes up to
// 8192 of a complete answer, which do not decode.
func cutPaneAnswer(pane, label string) string {
	full := `{"result":{"panes":[{"pane_id":"` + pane + `","label":"` + label + `","cwd":"` + strings.Repeat("/w", 6000) + `"}]}}`
	return full[:8192]
}

// The landing reader neither fails the spawn nor reports the pane absent when
// herdr's first pane list is cut: it reads the list again and gets the pane.
// Without the retry the reader is unproven (fails on base).
func TestSpawnEvidenceRetriesCutPaneListOnce(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes("w1:p1", "label-a")}
	cut := &cutPaneList{fake: fake, cut: []string{cutPaneAnswer("w1:p1", "label-a")}}
	client := &HubClient{nodeHerdr: cut.run}
	evidence, ok := client.readSpawnEvidence(t.Context(), "worker")
	if !ok {
		t.Fatal("cut pane list read as unreadable; want the retry to succeed")
	}
	if cut.listCalls() != 2 {
		t.Fatalf("pane list read %d times, want the cut one and one retry", cut.listCalls())
	}
	if len(evidence.panes) != 1 || evidence.panes[0].PaneID != "w1:p1" || evidence.panes[0].Label != "label-a" {
		t.Fatalf("panes %+v, want w1:p1 from the retried answer", evidence.panes)
	}
}

// The settle, end to end: a pane that only the retried list shows is still
// unproven, so the row stays executing. It is not reported gone, which is what
// a cut answer read as an empty list would do.
func TestSpawnSettleCutPaneListDoesNotReportPaneGone(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes("w1:p1", "label-a")}
	cut := &cutPaneList{fake: fake, cut: []string{cutPaneAnswer("w1:p1", "label-a")}}
	f := newSettleFixture(t, cut.run)
	f.recordSpawn(t, "w1:p1")
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 || f.stored(t) != nil {
		t.Fatalf("settled=%d left=%d stored=%+v", settled, left, f.stored(t))
	}
	if cut.listCalls() != 2 {
		t.Fatalf("pane list read %d times, want one retry", cut.listCalls())
	}
}

// A list cut twice is unreadable, not an empty list: the spawn is unproven,
// and the row stays executing rather than ending lost.
func TestSpawnSettleListCutTwiceStaysExecuting(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList()}, panes: settlePanes()}
	cut := &cutPaneList{fake: fake, cut: []string{cutPaneAnswer("w1:p1", "label-a"), cutPaneAnswer("w1:p1", "label-a")}}
	f := newSettleFixture(t, cut.run)
	f.recordSpawn(t, "w1:p1")
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 0 || left != 1 || f.stored(t) != nil {
		t.Fatalf("settled=%d left=%d stored=%+v", settled, left, f.stored(t))
	}
}

// A close that knows its pane asks herdr for that pane (`pane get`), and never
// reads the whole pane list. pane_not_found is absent.
func TestHubClosePaneListedAsksForTheKnownPane(t *testing.T) {
	var calls []string
	var mu sync.Mutex
	run := func(_ context.Context, args ...string) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, strings.Join(args, " "))
		switch strings.Join(args, " ") {
		case "pane get w1:p1":
			return []byte(`{"result":{"pane":{"pane_id":"w1:p1"}}}`), nil
		case "pane get w1:p9":
			_, err := exec.CommandContext(t.Context(), "sh", "-c", `printf '%s' "$0" >&2; exit 1`, `{"error":{"code":"pane_not_found"}}`).Output()
			return nil, err
		}
		return nil, errUnexpectedHerdrCall
	}
	client := &HubClient{nodeHerdr: run}
	if present, err := client.hubClosePaneListed(t.Context(), "worker", "w1:p1"); !present || err != nil {
		t.Fatalf("known pane: present=%v err=%v", present, err)
	}
	if present, err := client.hubClosePaneListed(t.Context(), "worker", "w1:p9"); present || err != nil {
		t.Fatalf("missing pane: present=%v err=%v, want absent", present, err)
	}
	if got := strings.Join(calls, ", "); got != "pane get w1:p1, pane get w1:p9" {
		t.Fatalf("herdr calls %q, want two pane get calls and no pane list", got)
	}
}

// herdrCutExit is what herdr's CLI does on a reply cut at 8192 bytes: exit
// nonzero, the diagnostic on stderr, and no stdout (herdr 0.9.5).
func herdrCutExit(t *testing.T) error {
	t.Helper()
	_, err := exec.CommandContext(t.Context(), "sh", "-c", `printf '%s' "$0" >&2; exit 1`, "Error: EOF while parsing a string, line 1, column 8192").Output()
	if err == nil {
		t.Fatal("want a failed command")
	}
	return err
}

// The real cut: the first `pane list` exits nonzero with the boundary
// diagnostic, and the retry reads the landed pane. The spawn settles landed.
func TestSpawnSettleRetriesCutPaneListExit(t *testing.T) {
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeAgentJSON("w1:p1", "label-a", "working", "/work/a"))}, panes: settlePanes("w1:p1", "label-a")}
	lists := 0
	run := func(ctx context.Context, args ...string) ([]byte, error) {
		if strings.Join(args, " ") == "pane list" {
			lists++
			if lists == 1 {
				return nil, herdrCutExit(t)
			}
		}
		return fake.run(ctx, args...)
	}
	f := newSettleFixture(t, run)
	f.recordSpawn(t, "w1:p1")
	// One settle pass, not runSpawnSettle's loop: the loop would settle on a
	// later pass without the retry, and this test must fail without it.
	if settled, left := f.client.settleExecutingSpawns(t.Context(), f.journal); settled != 1 || left != 0 {
		t.Fatalf("settled=%d left=%d, want the landed spawn settled in one pass", settled, left)
	}
	if result := f.stored(t); result == nil || result.RC != 0 || result.Pane != "w1:p1" {
		t.Fatalf("settled as %+v, want landed on w1:p1", result)
	}
	if lists != 2 {
		t.Fatalf("pane list ran %d times, want one retry", lists)
	}
}
