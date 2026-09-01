package panewire

import (
	"io"
	"os"
	"path/filepath"
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
