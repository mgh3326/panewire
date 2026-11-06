package devin

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// fakeClock is virtual time driven by the test: Ticker's channel only fires
// when the test sends a tick, which also advances Now by one probe tick, and
// Timer fires once virtual time passes its deadline. Nothing here sleeps.
type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	start    time.Time
	ticks    chan time.Time
	timers   []*fakeTimer
	nowCalls chan struct{}
}

type fakeTimer struct {
	at time.Time
	c  chan time.Time
}

func newFakeClock() *fakeClock {
	start := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return &fakeClock{now: start, start: start, ticks: make(chan time.Time), nowCalls: make(chan struct{}, 64)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.nowCalls <- struct{}{}:
	default:
	}
	return c.now
}

func (c *fakeClock) Ticker(time.Duration) (<-chan time.Time, func()) { return c.ticks, func() {} }

func (c *fakeClock) Timer(d time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), c: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.c, func() {}
}

// advance moves virtual time on by d and fires the timers it passed.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			select {
			case t.c <- c.now:
			default:
			}
		}
	}
}

func (c *fakeClock) elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(c.start)
}

// fakeTerminal is the devin TUI as the probe sees it: the test emits what the
// terminal draws, and script answers what the probe types.
type fakeTerminal struct {
	out    chan []byte
	closed chan struct{}
	once   sync.Once

	mu     sync.Mutex
	writes []string
	script func(typed string, t *fakeTerminal)
}

func newFakeTerminal(script func(string, *fakeTerminal)) *fakeTerminal {
	return &fakeTerminal{out: make(chan []byte), closed: make(chan struct{}), script: script}
}

// emit draws chunk and returns once the probe's reader holds it.
func (f *fakeTerminal) emit(chunk string) {
	select {
	case f.out <- []byte(chunk):
	case <-f.closed:
	}
}

func (f *fakeTerminal) Read(p []byte) (int, error) {
	select {
	case chunk := <-f.out:
		return copy(p, chunk), nil
	case <-f.closed:
		return 0, io.EOF
	}
}

func (f *fakeTerminal) Write(p []byte) (int, error) {
	f.mu.Lock()
	f.writes = append(f.writes, string(p))
	script := f.script
	f.mu.Unlock()
	if script != nil {
		script(string(p), f)
	}
	return len(p), nil
}

func (f *fakeTerminal) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeTerminal) typed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}

// panelOnEnter draws the /usage panel once /usage has been typed and Enter
// sent, as devin does.
func panelOnEnter(panel string) func(string, *fakeTerminal) {
	var sawUsage bool
	return func(typed string, f *fakeTerminal) {
		switch typed {
		case "/usage":
			sawUsage = true
		case "\r":
			if sawUsage {
				f.emit(panel)
			}
		}
	}
}

// probeRun is one probe over a fake terminal and virtual time.
type probeRun struct {
	text    string
	err     error
	elapsed time.Duration
	term    *fakeTerminal
}

// runProbe drives Config.drive: the boot screen is drawn, then virtual time
// advances 50 ms per tick until the probe finishes. A probe that waited out its
// 35 s shows as elapsed >= 35 s.
func runProbe(t *testing.T, boot string, script func(string, *fakeTerminal)) probeRun {
	t.Helper()
	clock := newFakeClock()
	term := newFakeTerminal(script)
	cfg := config(nil, nil)
	cfg.Clock = clock
	result := make(chan probeRun, 1)
	go func() {
		text, err := cfg.drive(t.Context(), term)
		result <- probeRun{text: text, err: err}
	}()
	<-clock.nowCalls // the probe started its clock
	if boot != "" {
		term.emit(boot)
		<-clock.nowCalls // and read the boot screen
	}
	for range 2000 { // 100 s of virtual time
		select {
		case r := <-result:
			r.elapsed, r.term = clock.elapsed(), term
			return r
		case clock.ticks <- clock.Now():
			clock.advance(probeTick)
		}
	}
	t.Fatal("probe did not finish in 100 s of virtual time")
	return probeRun{}
}

const panelWeekly = "Usage\r\nWeekly: 62% used · resets in 6d 2h\r\n"

// pacelever#57: devin draws its prompt with U+276D. The probe must see it as
// ready, type /usage, then Enter in a separate write, and parse the panel —
// well inside the 35 s timeout (virtual time here).
func TestProbeTypesUsageAtTheU276DPrompt(t *testing.T) {
	run := runProbe(t, fixture(t, "tui_prompt_u276d.txt"), panelOnEnter(panelWeekly))
	if run.err != nil {
		t.Fatalf("probe failed after %s: %v", run.elapsed, run.err)
	}
	if got := run.term.typed(); len(got) != 2 || got[0] != "/usage" || got[1] != "\r" {
		t.Fatalf("typed %q, want /usage then Enter in separate writes", got)
	}
	o, err := ParseScreen(run.text, testNow)
	if err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{{"7d", 62, "2026-11-12T14:00:00Z"}})
	if run.elapsed >= ProbeTimeout-time.Second {
		t.Fatalf("probe took %s of virtual time, expected well inside %s", run.elapsed, ProbeTimeout)
	}
}

// The older prompt glyph is ready too.
func TestProbeTypesUsageAtTheOldPrompt(t *testing.T) {
	run := runProbe(t, fixture(t, "tui_prompt_u276f.txt"), panelOnEnter(panelWeekly))
	if run.err != nil || len(run.term.typed()) != 2 || run.elapsed >= ProbeTimeout-time.Second {
		t.Fatalf("probe err=%v typed=%q elapsed=%s", run.err, run.term.typed(), run.elapsed)
	}
}

// Every prompt the probe knows is ready; a banner without one is not.
func TestProbeReady(t *testing.T) {
	for _, test := range []struct {
		name, screen string
		want         bool
	}{
		{"U+276D", fixture(t, "tui_prompt_u276d.txt"), true},
		{"U+276F", fixture(t, "tui_prompt_u276f.txt"), true},
		{"single angle", "› ", true},
		{"greater-than", "> ", true},
		{"hint line", "Shift+Tab to cycle", true},
		{"boot banner", fixture(t, "tui_boot.txt"), false},
		{"loading", fixture(t, "tui_no_prompt.txt"), false},
	} {
		if got := probeReady(cleanScreen(test.screen)); got != test.want {
			t.Errorf("%s: probeReady = %t, want %t", test.name, got, test.want)
		}
	}
}

// Without a prompt the probe never types and ends at its timeout, naming the
// stage — this is the failure pacelever#57 fixed for devin's glyph.
func TestProbeWithoutAPromptTimesOutAfter35Seconds(t *testing.T) {
	run := runProbe(t, fixture(t, "tui_no_prompt.txt"), panelOnEnter(panelWeekly))
	if run.err == nil || !strings.Contains(run.err.Error(), "probe timeout stage=no-prompt") {
		t.Fatalf("err = %v", run.err)
	}
	if len(run.term.typed()) != 0 {
		t.Fatalf("typed %q without a prompt", run.term.typed())
	}
	if run.elapsed < ProbeTimeout || run.elapsed > ProbeTimeout+time.Second {
		t.Fatalf("timed out after %s of virtual time, want %s", run.elapsed, ProbeTimeout)
	}
}

func TestProbeTimeoutNamesTheStage(t *testing.T) {
	for name, test := range map[string]struct {
		boot, stage string
		script      func(string, *fakeTerminal)
	}{
		"silent terminal":   {"", "no-output", nil},
		"panel never comes": {fixture(t, "tui_prompt_u276d.txt"), "no-panel", nil},
	} {
		t.Run(name, func(t *testing.T) {
			run := runProbe(t, test.boot, test.script)
			if run.err == nil || !strings.HasSuffix(run.err.Error(), "stage="+test.stage) {
				t.Fatalf("err = %v, want stage %s", run.err, test.stage)
			}
		})
	}
}

func TestProbeRefusesAWorkspaceTrustPrompt(t *testing.T) {
	run := runProbe(t, "Do you trust the contents of this folder?\r\n❭ ", nil)
	if run.err == nil || !strings.Contains(run.err.Error(), "workspace trust") || len(run.term.typed()) != 0 {
		t.Fatalf("err = %v typed=%q", run.err, run.term.typed())
	}
}

func TestProbeEndsWhenDevinExitsBeforeThePanel(t *testing.T) {
	clock := newFakeClock()
	term := newFakeTerminal(nil)
	cfg := config(nil, nil)
	cfg.Clock = clock
	done := make(chan error, 1)
	go func() { _, err := cfg.drive(t.Context(), term); done <- err }()
	<-clock.nowCalls
	term.emit(fixture(t, "tui_boot.txt"))
	<-clock.nowCalls
	_ = term.Close() // devin exits
	if err := <-done; err == nil || !strings.Contains(err.Error(), "exited before complete usage panel") {
		t.Fatalf("err = %v", err)
	}
}

// A cancelled caller stops the probe at once.
func TestProbeStopsWhenTheCallerCancels(t *testing.T) {
	clock := newFakeClock()
	term := newFakeTerminal(nil)
	cfg := config(nil, nil)
	cfg.Clock = clock
	done := make(chan error, 1)
	cctx, stop := context.WithCancel(t.Context())
	go func() { _, err := cfg.drive(cctx, term); done <- err }()
	<-clock.nowCalls
	stop()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "probe cancelled") {
		t.Fatalf("err = %v", err)
	}
}

// fetchProbe is probe_error for every failure, with a fixed sentence, and the
// terminal is always closed.
func TestFetchProbeFailureIsProbeErrorAndClosesTheTerminal(t *testing.T) {
	clock := newFakeClock()
	term := newFakeTerminal(nil)
	cfg := config(nil, nil)
	cfg.Clock = clock
	cfg.ProbeTimeout = time.Second
	cfg.StartProbe = func(_ context.Context, binary, dir string, args []string) (Terminal, error) {
		if binary != "devin" || strings.Join(args, " ") != "--respect-workspace-trust false" || dir == "" {
			t.Errorf("started %q %q in %q", binary, args, dir)
		}
		return term, nil
	}
	go func() {
		for range 2000 {
			select {
			case clock.ticks <- clock.Now():
				clock.advance(probeTick)
			case <-term.closed:
				return
			}
		}
	}()
	_, err := cfg.fetchProbe(t.Context(), "devin")
	if kindOf(err) != quota.ErrKindProbe || !strings.Contains(err.Error(), "probe timeout") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-term.closed:
	default:
		t.Fatal("the terminal was not closed")
	}
}

func TestFetchProbeStartFailureIsProbeError(t *testing.T) {
	cfg := config(nil, nil)
	cfg.StartProbe = func(context.Context, string, string, []string) (Terminal, error) { return nil, io.ErrClosedPipe }
	if _, err := cfg.fetchProbe(t.Context(), "devin"); kindOf(err) != quota.ErrKindProbe {
		t.Fatalf("err = %v", err)
	}
}

// A read can end in the middle of a line. The panel is accepted only after the
// screen has been quiet, so "Daily: 40%" followed by " left" is 60% used, not
// 40%, and a weekly line that arrives with the next read is not lost.
func TestProbeWaitsForTheScreenToSettleBeforeParsing(t *testing.T) {
	var sawUsage bool
	script := func(typed string, f *fakeTerminal) {
		switch typed {
		case "/usage":
			sawUsage = true
		case "\r":
			if sawUsage {
				f.emit("Usage\r\nDaily: 40%")
				f.emit(" left \u00b7 resets in 3h 5m\r\n")
				f.emit("Weekly: 70% left \u00b7 resets in 2 days 4 hours\r\n")
			}
		}
	}
	run := runProbe(t, fixture(t, "tui_prompt_u276d.txt"), script)
	if run.err != nil {
		t.Fatalf("probe failed after %s: %v", run.elapsed, run.err)
	}
	o, err := ParseScreen(run.text, testNow)
	if err != nil {
		t.Fatal(err)
	}
	checkBuckets(t, o.Buckets, []wantBucket{{"1d", 60, "2026-11-06T15:05:00Z"}, {"7d", 30, "2026-11-08T16:00:00Z"}})
}
