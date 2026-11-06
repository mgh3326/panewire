package devin

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// Pacing of the probe, as pacelever's: the prompt must have been up this long
// before /usage is typed, Enter follows after the input settles, and the panel
// is parsed once the screen has been quiet.
const (
	probeTick       = 50 * time.Millisecond
	probeReadyAfter = 400 * time.Millisecond
	probeSettle     = 500 * time.Millisecond
	probeQuiet      = 250 * time.Millisecond
	probeScreenCap  = 1 << 20
)

// Terminal is the pseudo-terminal the devin TUI runs on. Closing it ends the
// process and reaps it.
type Terminal interface {
	io.ReadWriteCloser
}

// Clock paces the probe. Elapsed time and the probe's own timeout read it, so a
// test drives virtual time instead of waiting out 35 seconds.
type Clock interface {
	Now() time.Time
	// Ticker delivers a tick every d until stopped.
	Ticker(d time.Duration) (ticks <-chan time.Time, stop func())
	// Timer delivers one tick after d unless stopped.
	Timer(d time.Duration) (ticks <-chan time.Time, stop func())
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() } // wall-clock: pacing

func (wallClock) Ticker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

func (wallClock) Timer(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// probeReady reports whether the cleaned screen shows an input prompt. devin
// draws its prompt with U+276D; without that glyph the probe never types and
// runs into its timeout. The other glyphs are the prompts of the CLIs sharing
// the probe's rules.
func probeReady(clean string) bool {
	for _, marker := range []string{"❯", "❭", "›", "> ", "Shift+Tab"} {
		if strings.Contains(clean, marker) {
			return true
		}
	}
	return false
}

// probeMu keeps one probe at a time in this process: two devin TUIs on one
// login would only race each other's panels.
var probeMu sync.Mutex

// fetchProbe types /usage into devin's TUI and parses the panel. The TUI runs
// in a private empty repository so it reads no caller's project. A failure to
// run, to read the panel or to finish in time is probe_error.
func (c Config) fetchProbe(ctx context.Context, binary string) (quota.Observation, error) {
	if !probeMu.TryLock() {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, "probe already in progress")
	}
	defer probeMu.Unlock()
	dir, err := privateRepository()
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, "probe directory unavailable")
	}
	defer os.RemoveAll(dir)
	start := c.StartProbe
	if start == nil {
		start = startPTY
	}
	terminal, err := start(ctx, binary, dir, []string{"--respect-workspace-trust", "false"})
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, "cannot start PTY probe")
	}
	defer terminal.Close()
	text, err := c.drive(ctx, terminal)
	if err != nil {
		return quota.Observation{}, quota.Fail(quota.ErrKindProbe, err.Error())
	}
	return ParseScreen(text, c.now())
}

// privateRepository is a temporary directory holding an empty git repository.
func privateRepository() (string, error) {
	dir, err := os.MkdirTemp("", "panewire-devin-probe-")
	if err != nil {
		return "", err
	}
	gitDir := filepath.Join(dir, ".git")
	for _, sub := range []string{"objects", "refs/heads"} {
		if err = os.MkdirAll(filepath.Join(gitDir, sub), 0o700); err != nil {
			os.RemoveAll(dir)
			return "", err
		}
	}
	for name, body := range map[string]string{"HEAD": "ref: refs/heads/main\n", "config": "[core]\nrepositoryformatversion = 0\nbare = false\n"} {
		if err = os.WriteFile(filepath.Join(gitDir, name), []byte(body), 0o600); err != nil {
			os.RemoveAll(dir)
			return "", err
		}
	}
	return dir, nil
}

// bounded keeps the first probeScreenCap bytes.
type bounded struct{ b []byte }

func (b *bounded) write(p []byte) {
	if left := probeScreenCap - len(b.b); left > 0 {
		b.b = append(b.b, p[:min(len(p), left)]...)
	}
}

// drive plays the probe over terminal until the panel parses. The error text is
// a fixed sentence naming the stage reached, never screen content.
func (c Config) drive(ctx context.Context, terminal Terminal) (string, error) {
	clock := c.Clock
	if clock == nil {
		clock = wallClock{}
	}
	timeout := c.ProbeTimeout
	if timeout <= 0 {
		timeout = ProbeTimeout
	}
	chunks := make(chan []byte, 8)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(chunks)
		buf := make([]byte, 8192)
		for {
			n, err := terminal.Read(buf)
			if n > 0 {
				select {
				case chunks <- append([]byte(nil), buf[:n]...):
				case <-done:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	ticks, stopTicker := clock.Ticker(probeTick)
	defer stopTicker()
	expired, stopTimer := clock.Timer(timeout)
	defer stopTimer()
	started := clock.Now()
	lastChange := started
	var typedAt time.Time
	var out bounded
	typed, submitted := false, false
	stage := "no-output"
	parsed := func() bool {
		_, err := ParseScreen(string(out.b), c.now())
		return err == nil
	}
	for {
		select {
		case <-ctx.Done():
			return "", errors.New("probe cancelled stage=" + stage)
		case <-expired:
			return "", errors.New("probe timeout stage=" + stage)
		case chunk, ok := <-chunks:
			if !ok {
				if parsed() {
					return string(out.b), nil
				}
				return "", errors.New("probe exited before complete usage panel stage=" + stage)
			}
			out.write(chunk)
			lastChange = clock.Now()
			stage = "no-prompt"
			if submitted {
				// A panel is accepted only once the screen has been quiet (the
				// tick below): a read can end mid-line, "Daily: 40%" before
				// " left", and parse as a different number.
				stage = "unparsed"
			}
		case <-ticks:
			clean := cleanScreen(string(out.b))
			lower := strings.ToLower(clean)
			if strings.Contains(lower, "trust this folder") || strings.Contains(lower, "do you trust the contents") {
				return "", errors.New("probe requires workspace trust stage=trust-prompt")
			}
			now := clock.Now()
			if !typed && probeReady(clean) && now.Sub(started) > probeReadyAfter {
				if _, err := terminal.Write([]byte("/usage")); err != nil {
					return "", errors.New("probe input failed")
				}
				typed, typedAt, stage = true, now, "no-panel"
			}
			// Input text and Enter never share a write: Enter follows once the
			// input has settled.
			if typed && !submitted && now.Sub(typedAt) >= probeSettle {
				if _, err := terminal.Write([]byte("\r")); err != nil {
					return "", errors.New("probe submit failed")
				}
				submitted = true
			}
			if submitted && now.Sub(lastChange) > probeQuiet && parsed() {
				return string(out.b), nil
			}
		}
	}
}
