package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

// `panewire fleet checkout-pull` is the operator's path to the checkout.pull
// order (docs/checkout-pull.md). It prints one row per host and exits
// ExitPartial unless every host pulled or was already current and the record
// was written.

const fleetCheckoutPullUsage = "usage: panewire fleet checkout-pull --hub-url <url> --hub-token-env <file> [--hub-cf-env <file>] [--checkout rstack] [--rev SHA] [--json]"

func runFleetCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "checkout-pull" {
		fmt.Fprintln(stderr, "fleet: expected checkout-pull")
		fmt.Fprintln(stderr, fleetCheckoutPullUsage)
		return ExitUsage
	}
	return runFleetCheckoutPullCLI(args[1:], stdout, stderr, deps)
}

func runFleetCheckoutPullCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire fleet checkout-pull", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", "", "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", "", "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	checkout := flags.String("checkout", nodeCheckoutDefaultName, "allow-listed checkout to pull on every host")
	rev := flags.String("rev", "", "optional target revision: each host reports whether its HEAD contains it")
	asJSON := flags.Bool("json", false, "print the order's answer as JSON")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnvPath == "" ||
		!nodeCheckoutNamePattern.MatchString(*checkout) || (*rev != "" && !nodeCheckoutRevPattern.MatchString(*rev)) {
		fmt.Fprintln(stderr, fleetCheckoutPullUsage)
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "checkout-pull rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		cfAccess, err = loadHubCFAccessEnv(*cfEnvPath)
		if err != nil {
			fmt.Fprintln(stderr, "checkout-pull rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	encoded, _ := json.Marshal(hubCheckoutPullRequest{Checkout: *checkout, Rev: *rev})
	timeout := hubCheckoutPullWait + 30*time.Second
	client, err := newHubOperatorClient(*hubURL, env.Token, cfAccess, deps, timeout)
	if err != nil {
		fmt.Fprintln(stderr, "checkout-pull rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	response, err := client.do(ctx, http.MethodPost, "/v1/fleet/checkout-pull", nil, bytes.NewReader(encoded))
	if err != nil {
		fmt.Fprintln(stderr, "checkout-pull unavailable")
		return ExitTimeout
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	if response.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "checkout-pull rejected")
		if response.StatusCode == http.StatusGatewayTimeout {
			return ExitTimeout
		}
		return ExitConditionInvalid
	}
	var answer hubCheckoutPullResponse
	if err := json.Unmarshal(data, &answer); err != nil {
		fmt.Fprintln(stderr, "checkout-pull unavailable: unreadable answer")
		return ExitInternal
	}
	if *asJSON {
		_, _ = stdout.Write(append(data, '\n'))
	} else {
		renderCheckoutPull(stdout, answer)
	}
	if checkoutPullAllOK(answer, *rev) {
		return ExitOK
	}
	return ExitPartial
}

// checkoutPullAllOK is true only when every host pulled or was current with
// no refusal, no failed install, and, with a target, HEAD containing it, and
// the record was written.
func checkoutPullAllOK(answer hubCheckoutPullResponse, rev string) bool {
	if answer.Record.Outcome != hubCheckoutRecordWrote {
		return false
	}
	for _, host := range answer.Hosts {
		if host.Error != "" {
			return false
		}
		var result nodeCheckoutPullResult
		if json.Unmarshal(host.Result, &result) != nil || result.Reason != "" {
			return false
		}
		if result.Outcome != nodeCheckoutOutcomePulled && result.Outcome != nodeCheckoutOutcomeCurrent {
			return false
		}
		if result.InstallRan && result.InstallExit != 0 {
			return false
		}
		if rev != "" && (result.TargetReached == nil || !*result.TargetReached) {
			return false
		}
	}
	return true
}

// renderCheckoutPull prints one row per host, in the order the hub answered.
func renderCheckoutPull(writer io.Writer, answer hubCheckoutPullResponse) {
	fmt.Fprintln(writer, "MACHINE\tSTATE\tOUTCOME\tFROM\tTO\tINSTALL\tTARGET\tDETAIL")
	for _, host := range answer.Hosts {
		var result nodeCheckoutPullResult
		_ = json.Unmarshal(host.Result, &result)
		outcome, detail := "-", host.Error
		if host.Error == "" {
			outcome, detail = result.Outcome, result.Reason
		}
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", host.Machine, host.State, outcome,
			shortCheckoutSHA(result.From), shortCheckoutSHA(result.To), renderCheckoutInstall(host.Error, result),
			renderCheckoutTarget(host.Error, result), detail)
	}
	fmt.Fprintf(writer, "record: %s (%s)\n", answer.Record.Key, answer.Record.Outcome)
}

func shortCheckoutSHA(sha string) string {
	if sha == "" {
		return "-"
	}
	return utf8PrefixWithin(sha, 12)
}

func renderCheckoutInstall(hostErr string, result nodeCheckoutPullResult) string {
	switch {
	case hostErr != "" || !result.InstallRan:
		return "-"
	case result.InstallExit == 0:
		return "ok"
	default:
		return fmt.Sprintf("exit %d", result.InstallExit)
	}
}

func renderCheckoutTarget(hostErr string, result nodeCheckoutPullResult) string {
	switch {
	case hostErr != "" || result.TargetReached == nil:
		return "-"
	case *result.TargetReached:
		return "reached"
	default:
		return "not-reached"
	}
}
