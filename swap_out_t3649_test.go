package panewire

import (
	"context"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Swap-out is sampled exactly like swap-in (checks.go): the page delta over the
// interval between two samples, a first sample only sets the baseline, and a
// read gap restarts it. The clock is fixed here, so the rate is exact.
func TestSwapOutRateFromFixtureCounters(t *testing.T) {
	linuxVM := string(readMemoryFixture(t, "memory/linux-vmstat.txt"))
	macVM := string(readMemoryFixture(t, "memory/macos-vm_stat.txt"))
	if pages, ok := parseLinuxVMStatPages(linuxVM, "pswpout"); !ok || pages != 7 {
		t.Fatalf("linux swap-out pages=%d ok=%t", pages, ok)
	}
	if pages, ok := parseDarwinSwapOuts(macVM); !ok || pages != 0 {
		t.Fatalf("mac swap-out pages=%d ok=%t", pages, ok)
	}
	// Five seconds later the Linux counter grew by 25 pages and the macOS one
	// by 25 pages, so each rate is 5 pages/s.
	linuxLater := strings.Replace(linuxVM, "pswpout 7", "pswpout 32", 1)
	macLater := regexp.MustCompile(`Swapouts:\s+0\.`).ReplaceAllString(macVM, "Swapouts: 25.")
	linuxSecond, linuxOK := parseLinuxVMStatPages(linuxLater, "pswpout")
	macSecond, macOK := parseDarwinSwapOuts(macLater)
	if !linuxOK || !macOK || linuxSecond != 32 || macSecond != 25 {
		t.Fatalf("later samples linux=%d ok=%t mac=%d ok=%t", linuxSecond, linuxOK, macSecond, macOK)
	}
	var linuxRate, macRate swapInRateSampler
	start := time.Unix(100, 0)
	if linuxRate.observe(7, start) != nil || macRate.observe(0, start) != nil {
		t.Fatal("a first swap-out sample produced a rate")
	}
	if got := linuxRate.observe(linuxSecond, start.Add(5*time.Second)); !sameMemoryFloat(got, 5) {
		t.Fatalf("linux swap-out rate=%v want 5", got)
	}
	if got := macRate.observe(macSecond, start.Add(5*time.Second)); !sameMemoryFloat(got, 5) {
		t.Fatalf("mac swap-out rate=%v want 5", got)
	}
}

func TestSwapOutRateResetsOnReadGap(t *testing.T) {
	resetLinuxSwapRates()
	resetDarwinSwapRates()
	collectLinux := func(vmstat string, vmstatErr error) *HubHostMemory {
		t.Helper()
		memory, err := collectLinuxHostMemory(func(path string) ([]byte, error) {
			switch path {
			case "/proc/meminfo":
				return []byte("SwapTotal: 2097152 kB\nSwapFree: 524288 kB\n"), nil
			case "/proc/vmstat":
				return []byte(vmstat), vmstatErr
			default:
				return nil, errors.New("unavailable")
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		return memory
	}
	if got := collectLinux("pswpin 100\npswpout 10\n", nil); got.SwapOutPerS != nil {
		t.Fatalf("first Linux sample produced a swap-out rate: %v", *got.SwapOutPerS)
	}
	// The collector reads the real clock, so only the presence and sign of the
	// second rate are pinned: the wiring from counter to heartbeat field.
	if got := collectLinux("pswpin 150\npswpout 30\n", nil); got.SwapOutPerS == nil || *got.SwapOutPerS < 0 {
		t.Fatalf("second Linux sample gave no swap-out rate: %+v", got.SwapOutPerS)
	}
	if got := collectLinux("", errors.New("read failed")); got.SwapOutPerS != nil {
		t.Fatalf("failed Linux read produced a swap-out rate: %v", *got.SwapOutPerS)
	}
	// The baseline was dropped with the failed read, so this is a first sample
	// again rather than a rate across the gap.
	if got := collectLinux("pswpin 300\npswpout 90\n", nil); got.SwapOutPerS != nil {
		t.Fatalf("swap-out rate was computed across a read gap: %v", *got.SwapOutPerS)
	}

	resetDarwinSwapRates()
	macVM := string(readMemoryFixture(t, "memory/macos-vm_stat.txt"))
	ok := func(_ context.Context, argv ...string) ([]byte, error) {
		switch strings.Join(argv, " ") {
		case "memory_pressure":
			return []byte("The system has 4096 (1 pages with a page size of 4096).\nPages used by compressor: 10\n"), nil
		case "vm_stat":
			return []byte(macVM), nil
		case "sysctl -n hw.memsize":
			return []byte("34359738368"), nil
		case "sysctl -n kern.memorystatus_vm_pressure_level":
			return []byte("2"), nil
		default:
			return nil, errors.New("unexpected fixture command")
		}
	}
	failing := func(ctx context.Context, argv ...string) ([]byte, error) {
		if strings.Join(argv, " ") == "vm_stat" {
			return nil, errors.New("vm_stat failed")
		}
		return ok(ctx, argv...)
	}
	if first, err := collectDarwinHostMemory(context.Background(), ok); err != nil || first.SwapOutPerS != nil {
		t.Fatalf("first macOS sample=%+v err=%v", first, err)
	}
	if second, err := collectDarwinHostMemory(context.Background(), ok); err != nil || second.SwapOutPerS == nil || *second.SwapOutPerS < 0 {
		t.Fatalf("second macOS sample gave no swap-out rate: %+v err=%v", second, err)
	}
	if gap, err := collectDarwinHostMemory(context.Background(), failing); err != nil || gap.SwapOutPerS != nil {
		t.Fatalf("failed macOS vm_stat produced a swap-out rate: %+v err=%v", gap, err)
	}
}

func TestHubHostMemoryAcceptsSwapOutRateOnlyWhenValid(t *testing.T) {
	ok := HubHostMemory{Source: "vm_stat", SwapOutPerS: memoryFloat(0)}
	if !ok.valid() {
		t.Fatal("a zero swap-out rate was rejected")
	}
	for _, bad := range []float64{-1, math.Inf(1)} {
		memory := HubHostMemory{Source: "vm_stat", SwapOutPerS: memoryFloat(bad)}
		if memory.valid() {
			t.Fatalf("swap-out rate %v was accepted", bad)
		}
	}
}
