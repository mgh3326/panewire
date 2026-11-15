package panewire

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// runPlaceCLI is the thin operator client for placement recommendations. A
// successful hub-only answer is still a successful placement decision.
func runPlaceCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire place", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	class := flags.String("class", "", "worker or verifier")
	cwd := flags.String("cwd", "", "repository key (metadata only)")
	tagsFlag := flags.String("tags", "", "required capability tags, comma-separated")
	explain := flags.Bool("explain", false, "render candidates as a table")
	signals := flags.Bool("signals", false, "include score signals in JSON")
	floor := flags.Bool("floor", false, "with --signals, include the free-floor flag the spawn engine ranks on (a hub older than this flag ignores it)")
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfPath := flags.String("hub-cf-env", "", "optional mode-0600 CF Access env file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || (*class != "worker" && *class != "verifier") || len(*cwd) > 256 || strings.ContainsAny(*cwd, "\r\n\x00") || *hubURL == "" || *tokenPath == "" {
		return ExitUsage
	}
	tags, err := parsePlacementTags(*tagsFlag)
	if err != nil {
		fmt.Fprintln(stderr, "place rejected: invalid --tags; use a comma-separated list of capability names")
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "place rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cf hubCFAccessEnv
	if *cfPath != "" {
		cf, err = loadHubCFAccessEnv(*cfPath)
		if err != nil {
			fmt.Fprintln(stderr, "place rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client, err := newHubOperatorClient(*hubURL, env.Token, cf, deps, 15*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "place rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	query := url.Values{}
	query.Set("class", *class)
	if len(tags) > 0 {
		query.Set("tags", strings.Join(tags, ","))
	}
	if *cwd != "" {
		query.Set("cwd", *cwd)
	}
	if *explain || *signals {
		// Only a caller that renders the signals asks for them; the JSON
		// form keeps the wire older decoders accept.
		query.Set("signals", "1")
		if *floor {
			query.Set("floor", "1")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodGet, "/v1/placement", query, nil)
	if err != nil {
		fmt.Fprintln(stderr, "place unavailable")
		return ExitDaemonUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 && response.StatusCode <= 599 {
		fmt.Fprintln(stderr, "place unavailable")
		return ExitDaemonUnavailable
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		fmt.Fprintln(stderr, "place authentication failed")
		return ExitDeliveryFailure
	}
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "place rejected: hub response is fail-closed")
		return ExitInternal
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		fmt.Fprintln(stderr, "place rejected: malformed hub response")
		return ExitInternal
	}
	var result PlacementResult
	if json.Unmarshal(body, &result) != nil || !validPlacementCLIResult(result) {
		fmt.Fprintln(stderr, "place rejected: malformed hub response")
		return ExitInternal
	}
	if result.Decision == "unavailable" {
		fmt.Fprintf(stderr, "place denied: %s; configure matching placement policy tags or wait for an eligible connected host\n", result.Reason)
		return ExitConditionInvalid
	}
	if *explain {
		renderPlacementExplain(stdout, result)
	} else {
		_ = json.NewEncoder(stdout).Encode(result)
	}
	return ExitOK
}

func validPlacementCLIResult(result PlacementResult) bool {
	if result.Source != "prometheus" && result.Source != "hub-only" {
		return false
	}
	if result.Decision != "unavailable" && !machineIDPattern.MatchString(result.Decision) {
		return false
	}
	// An exhausted slot policy legitimately answers with zero candidates; only
	// an unavailable decision may carry that shape.
	if len(result.Candidates) > 128 || (len(result.Candidates) == 0 && result.Decision != "unavailable") {
		return false
	}
	decisionFound := result.Decision == "unavailable"
	for _, candidate := range result.Candidates {
		if !machineIDPattern.MatchString(candidate.Machine) || strings.ContainsAny(candidate.Reason, "\x00\r\n") {
			return false
		}
		if candidate.Machine == result.Decision {
			decisionFound = true
		}
	}
	return decisionFound
}

// renderPlacementExplain prints each candidate's score with the terms behind
// it: score = CPU*MEM*IO*LOAD*JOBS * POWER - BRIDGED * claim cost, minus the
// connection/acceptance penalties named in REASON (docs/placement.md). A
// signal the host does not export, or an older hub that sends no terms,
// prints "-". LOAD1_CORE and PAGEIN_MBPS are the readings behind LOAD and IO.
func renderPlacementExplain(w io.Writer, result PlacementResult) {
	fmt.Fprintf(w, "DECISION\t%s\nSOURCE\t%s\nASOF\t%s\n", result.Decision, result.Source, result.Asof.UTC().Format(time.RFC3339))
	if result.Reason != "" {
		fmt.Fprintf(w, "REASON\t%s\n", result.Reason)
	}
	fmt.Fprintln(w, "MACHINE\tSCORE\tFREE\tCPU\tMEM\tIO\tLOAD\tJOBS\tCPU_BUSY\tMEM_AVAIL\tPAGEIN_MBPS\tLOAD1_CORE\tPOWER\tBRIDGED\tACTIVE_JOBS\tTHROTTLED\tCONNECTED\tREASON")
	for _, candidate := range result.Candidates {
		free, cpu, mem, ioTerm, load, jobs, busy, avail, pageIn, load1, power, bridged := "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"
		if s := candidate.Signals; s != nil {
			free, power, bridged = fmt.Sprintf("%.2f", s.Free), fmt.Sprintf("%.0f", s.Power), fmt.Sprintf("%d", s.BridgedJobs)
			if t := s.Terms; t != nil {
				cpu, mem, ioTerm, load, jobs = fmt.Sprintf("%.2f", t.CPU), fmt.Sprintf("%.2f", t.Mem), fmt.Sprintf("%.2f", t.IO), fmt.Sprintf("%.2f", t.Load), fmt.Sprintf("%.2f", t.Jobs)
			}
			busy, avail, pageIn, load1 = explainSignal(maxSignal(s.CPUBusy1m, s.CPUBusy5m)), explainSignal(s.MemAvailable), explainSignal(s.SwapInMBps), explainSignal(s.Load1PerCore)
		}
		fmt.Fprintf(w, "%s\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%t\t%t\t%s\n", candidate.Machine, candidate.Score, free, cpu, mem, ioTerm, load, jobs, busy, avail, pageIn, load1, power, bridged, candidate.ActiveJobs, candidate.Throttled, candidate.Connected, candidate.Reason)
	}
}

// maxSignal is the larger of two optional readings, nil when neither exists.
func maxSignal(a, b *float64) *float64 {
	if a == nil || (b != nil && *b > *a) {
		return b
	}
	return a
}

func explainSignal(value *float64) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f", *value)
}
