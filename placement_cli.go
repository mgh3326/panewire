package panewire

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// runPlaceCLI is the intentionally thin operator client used by wrk. A
// successful hub-only answer is still a successful placement decision.
func runPlaceCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire place", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	class := flags.String("class", "", "worker or verifier")
	cwd := flags.String("cwd", "", "repository key (metadata only)")
	explain := flags.Bool("explain", false, "render candidates as a table")
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfPath := flags.String("hub-cf-env", "", "optional mode-0600 CF Access env file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || (*class != "worker" && *class != "verifier") || len(*cwd) > 256 || strings.ContainsAny(*cwd, "\r\n\x00") || *hubURL == "" || *tokenPath == "" {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "place rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	endpoint, err := hubHTTPSEndpoint(*hubURL, "/v1/placement", deps.AllowInsecureForTests)
	if err != nil {
		fmt.Fprintln(stderr, "place rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	query := endpoint.Query()
	query.Set("class", *class)
	if *cwd != "" {
		query.Set("cwd", *cwd)
	}
	endpoint.RawQuery = query.Encode()
	var cf hubCFAccessEnv
	if *cfPath != "" {
		cf, err = loadHubCFAccessEnv(*cfPath)
		if err != nil {
			fmt.Fprintln(stderr, "place rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return ExitInternal
	}
	req.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	if cf.ClientID != "" {
		req.Header.Set("CF-Access-Client-Id", cf.ClientID)
		req.Header.Set("CF-Access-Client-Secret", cf.ClientSecret)
	}
	response, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(stderr, "place unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "place unavailable")
		return ExitInternal
	}
	var result PlacementResult
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || !machineIDPattern.MatchString(result.Decision) || (result.Source != "prometheus" && result.Source != "hub-only") {
		fmt.Fprintln(stderr, "place unavailable")
		return ExitInternal
	}
	if *explain {
		renderPlacementExplain(stdout, result)
	} else {
		_ = json.NewEncoder(stdout).Encode(result)
	}
	return ExitOK
}

func renderPlacementExplain(w io.Writer, result PlacementResult) {
	fmt.Fprintf(w, "DECISION\t%s\nSOURCE\t%s\nASOF\t%s\n", result.Decision, result.Source, result.Asof.UTC().Format(time.RFC3339))
	fmt.Fprintln(w, "MACHINE\tSCORE\tLOAD_RATIO\tTHROTTLED\tACTIVE_JOBS\tCONNECTED\tREASON")
	for _, candidate := range result.Candidates {
		fmt.Fprintf(w, "%s\t%.2f\t%.2f\t%t\t%d\t%t\t%s\n", candidate.Machine, candidate.Score, candidate.LoadRatio, candidate.Throttled, candidate.ActiveJobs, candidate.Connected, candidate.Reason)
	}
}
