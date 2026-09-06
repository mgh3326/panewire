package panewire

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestHostLoadParsersFixedSamples(t *testing.T) {
	darwin, err := parseDarwinHostLoad("{ 1.25 6.00 8.50 }\n", "total = 16384.00M  used = 8192.00M  free = 8192.00M  (encrypted)\n")
	if err != nil || darwin.Load1 != 1.25 || darwin.Load5 != 6 || darwin.SwapUsedGB != 8 {
		t.Fatalf("darwin=%+v err=%v", darwin, err)
	}
	linux, err := parseLinuxHostLoad("0.50 6.00 8.50 1/100 1\n", "MemTotal: 1 kB\nSwapTotal:       16777216 kB\nSwapFree:         8388608 kB\n")
	if err != nil || linux.Load1 != .5 || linux.Load5 != 6 || linux.SwapUsedGB != 8 {
		t.Fatalf("linux=%+v err=%v", linux, err)
	}
}

func TestBurstPolicyLastGoodHotReloadAndDecisions(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "burst.json")
	policy := BurstPolicy{SourceMachine: "mac-personal", SwapGB: 8, Load5: 6, Consecutive: 3, WakeVia: "rpi", WakeMAC: "02:1a:2b:3c:4d:5e", TargetMachine: "desktop", IdleMinutes: 30, CooldownMinutes: 20}
	if err := os.WriteFile(path, []byte(formatBurstPolicy(policy)), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 2, 8, 30, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-personal": r6NodeAToken, "rpi": r6NodeBToken, "desktop": "desktop-token-123456"}, BurstPolicyPath: path, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	observe := func(machine string, load HubHostLoad) []hubBurstEvent {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.observeBurstLocked(now, machine, load)
	}
	load := HubHostLoad{Load5: 6, SwapUsedGB: 0, WorkerProcs: 1} // exact load boundary
	if got := observe("mac-personal", load); len(got) != 0 {
		t.Fatalf("early burst=%+v", got)
	}
	if got := observe("mac-personal", load); len(got) != 0 {
		t.Fatalf("early burst=%+v", got)
	}
	if got := observe("mac-personal", load); len(got) != 1 || got[0].Phase != "up" {
		t.Fatalf("burst=%+v", got)
	}
	if got := observe("mac-personal", load); len(got) != 0 {
		t.Fatalf("cooldown burst=%+v", got)
	}
	if got := observe("desktop", HubHostLoad{WorkerProcs: 1}); len(got) != 0 {
		t.Fatalf("worker must prohibit down: %+v", got)
	}
	now = now.Add(30 * time.Minute)
	if got := observe("desktop", HubHostLoad{WorkerProcs: 0}); len(got) != 0 {
		t.Fatalf("first idle must not down: %+v", got)
	}
	now = now.Add(30 * time.Minute)
	if got := observe("desktop", HubHostLoad{WorkerProcs: 0}); len(got) != 1 || got[0].Phase != "down" {
		t.Fatalf("idle down=%+v", got)
	}
	if err := os.WriteFile(path, []byte(`{"source_machine":""}`), 0600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	hub.mu.Lock()
	hub.reloadBurstPolicyLocked()
	current := hub.burstPolicy
	hub.mu.Unlock()
	if current.SourceMachine != "mac-personal" {
		t.Fatalf("bad reload replaced last good: %+v", current)
	}
	policy.Load5 = 7
	if err := writeBurstPolicy(path, policy); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	hub.mu.Lock()
	hub.reloadBurstPolicyLocked()
	current = hub.burstPolicy
	hub.mu.Unlock()
	if current.Load5 != 7 {
		t.Fatalf("hot reload not applied: %+v", current)
	}
}

// This fixes the safety boundary that matters most for a desktop: a worker
// observed after almost-idle time must reset the clock and must itself never
// receive a poweroff decision. Keep the two assertions separate so removing
// either the reset branch or the WorkerProcs guard turns this test red.
func TestBurstWorkerAppearanceResetsIdleAndProhibitsDown(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "burst.json")
	policy := BurstPolicy{SourceMachine: "mac-personal", SwapGB: 8, Load5: 6, Consecutive: 3, WakeVia: "rpi", WakeMAC: "02:1a:2b:3c:4d:5e", TargetMachine: "desktop", IdleMinutes: 30, CooldownMinutes: 0}
	if err := os.WriteFile(path, []byte(formatBurstPolicy(policy)), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 2, 9, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-personal": r6NodeAToken, "rpi": r6NodeBToken, "desktop": "desktop-token-123456"}, BurstPolicyPath: path, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	observe := func(load HubHostLoad) []hubBurstEvent {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.observeBurstLocked(now, "desktop", load)
	}
	if got := observe(HubHostLoad{WorkerProcs: 0}); len(got) != 0 {
		t.Fatalf("initial idle event=%+v", got)
	}
	now = now.Add(29 * time.Minute)
	if got := observe(HubHostLoad{WorkerProcs: 0}); len(got) != 0 {
		t.Fatalf("pre-threshold idle event=%+v", got)
	}
	now = now.Add(time.Minute)
	if got := observe(HubHostLoad{WorkerProcs: 1}); len(got) != 0 {
		t.Fatalf("worker heartbeat must never down: %+v", got)
	}
	now = now.Add(29 * time.Minute)
	if got := observe(HubHostLoad{WorkerProcs: 0}); len(got) != 0 {
		t.Fatalf("idle below reset threshold must not down: %+v", got)
	}
}

func TestBurstCLISetWritesPolicyAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "burst.json")
	policy := BurstPolicy{SourceMachine: "mac-personal", SwapGB: 8, Load5: 6, Consecutive: 3, WakeVia: "rpi", WakeMAC: "02:1a:2b:3c:4d:5e", TargetMachine: "desktop", IdleMinutes: 30, CooldownMinutes: 20}
	if err := os.WriteFile(path, []byte(formatBurstPolicy(policy)), 0600); err != nil {
		t.Fatal(err)
	}
	if code := runBurstCLI([]string{"set", "--burst-policy", path, "--swap-gb", "9", "--load5", "7", "--consecutive", "4", "--idle-min", "31"}, io.Discard, io.Discard); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	got, _, err := LoadBurstPolicy(path)
	if err != nil || got.SwapGB != 9 || got.Load5 != 7 || got.Consecutive != 4 || got.IdleMinutes != 31 {
		t.Fatalf("policy=%+v err=%v", got, err)
	}
}

// macOS pgrep has no -c flag (exit 2 on Linux-only "-fc"), so the worker count
// must be derived from listed PIDs on both platforms. A zero match is pgrep
// exit 1 with empty output; anything else that fails must suppress telemetry.
func TestWorkerCountIsPortableAcrossPgrepVariants(t *testing.T) {
	ctx := context.Background()
	exit := func(code int) error {
		cmd := exec.Command("sh", "-c", "exit "+strconv.Itoa(code))
		return cmd.Run()
	}
	cases := []struct {
		name   string
		output string
		err    error
		want   int
		wantOK bool
	}{
		{"three workers listed", "123\n456\n789\n", nil, 3, true},
		{"idle: exit 1 with no output", "", exit(1), 0, true},
		{"usage error (macOS -c): exit 2", "usage: pgrep", exit(2), 0, false},
		{"garbage line", "123\nnot-a-pid\n", nil, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var argv []string
			run := func(_ context.Context, args ...string) ([]byte, error) {
				argv = args
				return []byte(tc.output), tc.err
			}
			got, err := countHubWorkerProcesses(ctx, run)
			if (err == nil) != tc.wantOK || got != tc.want {
				t.Fatalf("got %d, err=%v; want %d ok=%v", got, err, tc.want, tc.wantOK)
			}
			for _, a := range argv {
				if a == "-fc" || a == "-c" {
					t.Fatalf("worker count must not use the Linux-only pgrep -c flag: %v", argv)
				}
			}
		})
	}
}
