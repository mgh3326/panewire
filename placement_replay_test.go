package panewire

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// Acceptance 1: replay the score on two days of fleet metrics and the spawn
// failures recorded in the same window. The data is the design session's
// extract (hk:doc research/2026-11-04/placement-score-vs-failures): set
// PLACEMENT_REPLAY_DIR to the directory holding prom2d.json and node_spawn.json.
//
//	PLACEMENT_REPLAY_DIR=~/work/herdr-inbox/jobs/pw-score-v2/placement-data \
//	  go test -run TestPlacementScoreReplay -v .
//
// The replay scores each decision with v1 (the formula docs/placement.md
// described before score v2, kept here as a reference only) and with the real
// v2 code (scoreK().terms, decide). What it cannot see is recorded below.

// replayPower is the live placement.json power (research doc, "Current hub
// score"). replayPolicyOrder is the candidate order: the claude pool, mac-dev
// first (research doc, fact 2).
var (
	replayPower       = map[string]float64{"mac-dev": 100, "desktop": 39, "rpi": 12}
	replayPolicyOrder = []string{"mac-dev", "desktop", "rpi"}
)

type replaySeries map[string][][2]float64

type replayData struct {
	prom     map[string]replaySeries
	failures []replayFailure
}

type replayFailure struct {
	at    time.Time
	job   string
	class string
}

// at returns the newest sample at or before t that is at most 180 s old, as the
// hub's freshness guard does, or false.
func (s replaySeries) at(machine string, t float64) (float64, bool) {
	points := s[machine]
	i := sort.Search(len(points), func(i int) bool { return points[i][0] > t }) - 1
	if i < 0 || t-points[i][0] >= 180 {
		return 0, false
	}
	return points[i][1], true
}

// avg3m is avg_over_time(x[3m]) at t.
func (s replaySeries) avg3m(machine string, t float64) (float64, bool) {
	points := s[machine]
	sum, n := 0.0, 0
	for i := sort.Search(len(points), func(i int) bool { return points[i][0] > t }) - 1; i >= 0 && t-points[i][0] < 180; i-- {
		sum, n = sum+points[i][1], n+1
	}
	if n == 0 {
		return 0, false
	}
	return sum / float64(n), true
}

// replayLoadPlausible names the failures a busy host can cause: the sixteen of
// the research doc's fact 4, mac-dev on 2026-11-04 KST (UTC+9).
var replayLoadPlausible = []struct{ class, text string }{
	{"BRIEF_SUBMISSION_UNCONFIRMED", "BRIEF_SUBMISSION_UNCONFIRMED"},
	{"shell command not proven", "shell command execution not proven"},
	{"identity", "Devin identity not proven"},
	{"footer", "model/effort footer not proven"},
	{"foreground", "agent not in foreground"},
}

func loadReplayData(t *testing.T, dir string) replayData {
	t.Helper()
	var data replayData
	raw, err := os.ReadFile(filepath.Join(dir, "prom2d.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &data.prom); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, "node_spawn.json"))
	if err != nil {
		t.Fatal(err)
	}
	var events [][2]json.RawMessage
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	from, to := time.Date(2026, 11, 3, 15, 0, 0, 0, time.UTC), time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	for _, pair := range events {
		var host string
		var ev struct{ TS, Job, Outcome, Detail string }
		if json.Unmarshal(pair[0], &host) != nil || json.Unmarshal(pair[1], &ev) != nil {
			t.Fatalf("node_spawn.json entry %s", pair[0])
		}
		at, err := time.Parse(time.RFC3339Nano, ev.TS)
		if err != nil || host != "mac-dev" || ev.Outcome != "failed" || at.Before(from) || !at.Before(to) {
			continue
		}
		for _, class := range replayLoadPlausible {
			if strings.Contains(ev.Detail, class.text) {
				data.failures = append(data.failures, replayFailure{at: at, job: ev.Job, class: class.class})
				break
			}
		}
	}
	sort.Slice(data.failures, func(i, j int) bool { return data.failures[i].at.Before(data.failures[j].at) })
	return data
}

// replayV1 is the pre-v2 score: (1-max(cpu1,cpu5)) * min(1, mem/0.10) /
// (1+swapin/300) * power. The claims term is zero in both versions: the
// extract carries no claim history.
func replayV1(d replayData, machine string, t float64) (float64, bool) {
	c1, ok1 := d.prom["cpu1"].at(machine, t)
	c5, ok5 := d.prom["cpu5"].at(machine, t)
	if !ok1 || !ok5 {
		return 0, false
	}
	mem, swap := d.memAndSwap(machine, t)
	free := 1 - math.Min(math.Max(math.Max(c1, c5), 0), 1)
	if mem != nil {
		free *= math.Min(*mem/0.10, 1)
	}
	if swap != nil {
		free /= 1 + *swap/300
	}
	return free * replayPower[machine], true
}

func (d replayData) memAndSwap(machine string, t float64) (mem, swap *float64) {
	memKey, swapKey := "memavail_linux", "swapin_linux"
	if strings.HasPrefix(machine, "mac") {
		memKey, swapKey = "mem_mac", "swapin_mac"
	}
	if v, ok := d.prom[memKey].at(machine, t); ok {
		mem = &v
	}
	if v, ok := d.prom[swapKey].at(machine, t); ok {
		swap = &v
	}
	return mem, swap
}

// replayV2Candidate builds the hub's candidate for one host and instant from
// the same signals the hub would have read, and scores it with the real code.
func replayV2Candidate(d replayData, machine string, t float64) (PlacementCandidate, bool) {
	c1, ok1 := d.prom["cpu1"].at(machine, t)
	c5, ok5 := d.prom["cpu5"].at(machine, t)
	if !ok1 || !ok5 {
		return PlacementCandidate{}, false
	}
	signals := PlacementSignals{CPUBusy1m: &c1, CPUBusy5m: &c5}
	mem, swap := d.memAndSwap(machine, t)
	signals.MemAvailable, signals.SwapInMBps = mem, swap
	signals.SwapInLinux = !strings.HasPrefix(machine, "mac")
	if load, ok := d.prom["load1"].avg3m(machine, t); ok {
		if cores, ok := d.prom["ncpu"].at(machine, t); ok && cores > 0 {
			perCore := load / cores
			signals.Load1PerCore = &perCore
		}
	}
	k := (PlacementPolicy{}).scoreK()
	terms := k.terms(signals, 0)
	signals.Terms = &terms
	return PlacementCandidate{Machine: machine, Score: k.scoreBase(terms.Free(), 0, replayPower[machine]), Signals: &signals}, true
}

// replayWinners returns the v1 and v2 winner at t over the candidate hosts, or
// false when a candidate has no reading (the hub would call it load_unknown).
func replayWinners(d replayData, t float64) (v1, v2 string, v2Reason string, ok bool) {
	var usable []PlacementCandidate
	best := math.Inf(-1)
	for _, machine := range replayPolicyOrder {
		score, have := replayV1(d, machine, t)
		candidate, have2 := replayV2Candidate(d, machine, t)
		if !have || !have2 {
			continue
		}
		if score > best {
			best, v1 = score, machine
		}
		usable = append(usable, candidate)
	}
	if len(usable) == 0 {
		return "", "", "", false
	}
	// The policy-order winner is the first usable host in replayPolicyOrder.
	picked, reason := (PlacementPolicy{}).scoreK().decide(usable, 0)
	return v1, usable[picked].Machine, reason, true
}

func TestPlacementScoreReplay(t *testing.T) {
	dir := os.Getenv("PLACEMENT_REPLAY_DIR")
	if dir == "" {
		t.Skip("PLACEMENT_REPLAY_DIR is not set: the replay needs the design session's prom2d.json and node_spawn.json")
	}
	data := loadReplayData(t, dir)
	if len(data.failures) != 16 {
		t.Fatalf("load-plausible failures=%d, the research doc counts 16", len(data.failures))
	}

	// Per decision: the host each version would have picked at the moment of
	// each load-plausible failure, all on mac-dev.
	elsewhere := 0
	var table strings.Builder
	fmt.Fprintf(&table, "\n%-20s %-26s %-9s %-9s %-14s %s\n", "time (UTC)", "failure", "v1", "v2", "v2 reason", "job")
	for _, f := range data.failures {
		v1, v2, reason, ok := replayWinners(data, float64(f.at.Unix()))
		if !ok {
			t.Fatalf("no readings at %s", f.at)
		}
		if v2 != "mac-dev" {
			elsewhere++
		}
		fmt.Fprintf(&table, "%-20s %-26s %-9s %-9s %-14s %s\n", f.at.Format("2006-01-02 15:04:05"), f.class, v1, v2, firstOr(reason, "policy_order"), f.job)
	}
	t.Log(table.String())
	t.Logf("%d of %d load-plausible failures would have gone elsewhere under v2", elsewhere, len(data.failures))
	if elsewhere < 10 {
		t.Errorf("only %d of 16 load-plausible failures would have gone elsewhere, want at least 10", elsewhere)
	}

	// Calm minutes: where mac-dev ran under 0.5 load per core the winner must
	// mostly stay.
	calm, flips := 0, 0
	for _, point := range data.prom["load1"]["mac-dev"] {
		cores, ok := data.prom["ncpu"].at("mac-dev", point[0])
		if !ok || point[1]/cores >= 0.5 {
			continue
		}
		v1, v2, _, ok := replayWinners(data, point[0])
		if !ok {
			continue
		}
		calm++
		if v1 != v2 {
			flips++
		}
	}
	t.Logf("%d of %d minutes with load1/core < 0.5 flip (%.1f%%)", flips, calm, 100*float64(flips)/float64(max(calm, 1)))
	if calm == 0 || float64(flips) > 0.10*float64(calm) {
		t.Errorf("%d of %d calm minutes flip, want at most 10%%", flips, calm)
	}
}

func firstOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
