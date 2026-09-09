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
	pool := flags.String("pool", "", "scopefuel-reported pool")
	accountFP := flags.String("account-fp", "", "optional sha8 account selector")
	explain := flags.Bool("explain", false, "render candidates as a table")
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfPath := flags.String("hub-cf-env", "", "optional mode-0600 CF Access env file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || (*class != "worker" && *class != "verifier") || len(*cwd) > 256 || strings.ContainsAny(*cwd, "\r\n\x00") || (*pool == "" && *accountFP != "") || (*pool != "" && !hubQuotaPoolPattern.MatchString(*pool)) || (*accountFP != "" && !hubQuotaAccountFPPattern.MatchString(*accountFP)) || *hubURL == "" || *tokenPath == "" {
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
	if *pool != "" {
		query.Set("pool", *pool)
	}
	if *accountFP != "" {
		query.Set("account_fp", *accountFP)
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
	if json.Unmarshal(body, &result) != nil || !validPlacementCLIResult(result, *pool, *accountFP) {
		fmt.Fprintln(stderr, "place rejected: malformed hub response")
		return ExitInternal
	}
	if result.Decision == "unavailable" {
		fmt.Fprintln(stderr, "place denied")
		return ExitConditionInvalid
	}
	if *explain {
		renderPlacementExplain(stdout, result)
	} else {
		_ = json.NewEncoder(stdout).Encode(result)
	}
	return ExitOK
}

func validPlacementCLIResult(result PlacementResult, pool, accountFP string) bool {
	if result.Source != "prometheus" && result.Source != "hub-only" {
		return false
	}
	if result.Decision != "unavailable" && !machineIDPattern.MatchString(result.Decision) {
		return false
	}
	if len(result.Candidates) == 0 || len(result.Candidates) > 128 {
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
	if !decisionFound {
		return false
	}
	if pool == "" {
		return result.Quota == nil
	}
	quota := result.Quota
	if quota == nil || quota.Pool != pool || quota.AccountFP != accountFP || (quota.Decision != "allow" && quota.Decision != "boost" && quota.Decision != "deny" && quota.Decision != "unknown") || (quota.PolicyStatus != "default" && quota.PolicyStatus != "current" && quota.PolicyStatus != "stale" && quota.PolicyStatus != "invalid") {
		return false
	}
	if (quota.Decision == "deny" || quota.Decision == "unknown") != (result.Decision == "unavailable") {
		return false
	}
	return true
}

func renderPlacementExplain(w io.Writer, result PlacementResult) {
	fmt.Fprintf(w, "DECISION\t%s\nSOURCE\t%s\nASOF\t%s\n", result.Decision, result.Source, result.Asof.UTC().Format(time.RFC3339))
	if result.Quota != nil {
		fmt.Fprintf(w, "QUOTA\t%s\t%s\t%s\n", result.Quota.Decision, result.Quota.PolicyStatus, result.Quota.Reason)
	}
	fmt.Fprintln(w, "MACHINE\tSCORE\tLOAD_RATIO\tTHROTTLED\tACTIVE_JOBS\tCONNECTED\tREASON")
	for _, candidate := range result.Candidates {
		fmt.Fprintf(w, "%s\t%.2f\t%.2f\t%t\t%d\t%t\t%s\n", candidate.Machine, candidate.Score, candidate.LoadRatio, candidate.Throttled, candidate.ActiveJobs, candidate.Connected, candidate.Reason)
	}
}
