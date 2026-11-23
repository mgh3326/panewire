package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// runHubOperatorReason is the argv and transport shared by the operator's
// one-reason commands (`hub spawn abandon`, `hub ladder release`): a request id,
// a --reason line, the hub credentials, and one POST whose answer the command
// reports. label names the command in every message it writes; path builds the
// request path for the id; report prints the answer and returns the exit code.
func runHubOperatorReason(label, usage string, path func(requestID string) string, rest []string, stdout, stderr io.Writer, deps hubCLIDeps, report func(requestID string, status int, body map[string]string) int) int {
	var requestID string
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		requestID, rest = rest[0], rest[1:]
	}
	flags := flag.NewFlagSet("panewire "+label, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", os.Getenv("PANEWIRE_HUB_URL"), "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", os.Getenv("PANEWIRE_HUB_CF_ENV"), "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	reason := flags.String("reason", "", "why the operator acts on the request")
	if err := flags.Parse(rest); err != nil {
		return writeHubCLIUsage(stderr, label+": invalid flag", usage)
	}
	if requestID == "" && flags.NArg() == 1 {
		requestID = flags.Arg(0)
	} else if flags.NArg() != 0 {
		return writeHubCLIUsage(stderr, label+": unexpected positional argument", usage)
	}
	if !validHubSpawnRequestID(requestID) {
		return writeHubCLIUsage(stderr, label+": a valid REQUEST_ID is required", usage)
	}
	if !validSpawnAbandonReason(*reason) {
		return writeHubCLIUsage(stderr, fmt.Sprintf("%s: --reason must be one printable line of at most %d bytes", label, spawnAbandonReasonLimit), usage)
	}
	if *hubURL == "" || *tokenEnvPath == "" {
		return writeHubCLIUsage(stderr, label+": --hub-url and --hub-token-env are required", usage)
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintf(stderr, "%s rejected: invalid operator token env\n", label)
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		if cfAccess, err = loadHubCFAccessEnv(*cfEnvPath); err != nil {
			fmt.Fprintf(stderr, "%s rejected: invalid Cloudflare Access env\n", label)
			return ExitConditionInvalid
		}
	}
	client, err := newHubOperatorClient(*hubURL, env.Token, cfAccess, deps, 30*time.Second)
	if err != nil {
		fmt.Fprintf(stderr, "%s rejected: invalid hub URL\n", label)
		return ExitConditionInvalid
	}
	encoded, _ := json.Marshal(map[string]string{"reason": *reason})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodPost, path(requestID), nil, bytes.NewReader(encoded))
	if err != nil {
		fmt.Fprintf(stderr, "%s unavailable\n", label)
		return ExitInternal
	}
	defer response.Body.Close()
	var body map[string]string
	_ = json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&body)
	return report(requestID, response.StatusCode, body)
}
