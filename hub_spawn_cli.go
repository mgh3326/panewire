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

const hubSpawnUsage = `Usage:
  panewire hub spawn abandon REQUEST_ID --reason TEXT [` + hubCredentialUsage + `]

abandon finishes a pending hub spawn request as failed and frees its
(task, role) reservation, but only when the request's node lists no pane
bearing its job label. It needs an operator token env (HUB_MACHINE_ID=operator).
--hub-url, --hub-token-env and --hub-cf-env default to PANEWIRE_HUB_URL,
PANEWIRE_HUB_TOKEN_ENV and PANEWIRE_HUB_CF_ENV.`

// runHubSpawnCLI is `panewire hub spawn`, the operator's handle on durable
// spawn operations (docs/hub-spawn-operations.md, "Stranded requests").
func runHubSpawnCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "abandon" {
		return writeHubCLIUsage(stderr, "hub spawn: expected the abandon subcommand", hubSpawnUsage)
	}
	rest := args[1:]
	var requestID string
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		requestID, rest = rest[0], rest[1:]
	}
	flags := flag.NewFlagSet("panewire hub spawn abandon", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", os.Getenv("PANEWIRE_HUB_URL"), "HTTPS hub base URL")
	tokenEnvPath := flags.String("hub-token-env", os.Getenv("PANEWIRE_HUB_TOKEN_ENV"), "mode-0600 operator HUB_MACHINE_ID/HUB_TOKEN env file")
	cfEnvPath := flags.String("hub-cf-env", os.Getenv("PANEWIRE_HUB_CF_ENV"), "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
	reason := flags.String("reason", "", "why the request is abandoned")
	if err := flags.Parse(rest); err != nil {
		return writeHubCLIUsage(stderr, "hub spawn abandon: invalid flag", hubSpawnUsage)
	}
	if requestID == "" && flags.NArg() == 1 {
		requestID = flags.Arg(0)
	} else if flags.NArg() != 0 {
		return writeHubCLIUsage(stderr, "hub spawn abandon: unexpected positional argument", hubSpawnUsage)
	}
	if !validHubSpawnRequestID(requestID) {
		return writeHubCLIUsage(stderr, "hub spawn abandon: a valid REQUEST_ID is required", hubSpawnUsage)
	}
	if !validSpawnAbandonReason(*reason) {
		return writeHubCLIUsage(stderr, fmt.Sprintf("hub spawn abandon: --reason must be one printable line of at most %d bytes", spawnAbandonReasonLimit), hubSpawnUsage)
	}
	if *hubURL == "" || *tokenEnvPath == "" {
		return writeHubCLIUsage(stderr, "hub spawn abandon: --hub-url and --hub-token-env are required", hubSpawnUsage)
	}
	env, err := loadHubTokenEnv(*tokenEnvPath)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "hub spawn abandon rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	var cfAccess hubCFAccessEnv
	if *cfEnvPath != "" {
		if cfAccess, err = loadHubCFAccessEnv(*cfEnvPath); err != nil {
			fmt.Fprintln(stderr, "hub spawn abandon rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client, err := newHubOperatorClient(*hubURL, env.Token, cfAccess, deps, 30*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "hub spawn abandon rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	encoded, _ := json.Marshal(map[string]string{"reason": *reason})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodPost, "/v1/spawns/"+requestID+"/abandon", nil, bytes.NewReader(encoded))
	if err != nil {
		fmt.Fprintln(stderr, "hub spawn abandon unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	var body map[string]string
	_ = json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&body)
	switch response.StatusCode {
	case http.StatusOK:
		fmt.Fprintf(stdout, "ABANDONED request=%s job=%s status=%s\n", requestID, body["job_id"], body["status"])
		return ExitOK
	case http.StatusConflict, http.StatusNotFound:
		fmt.Fprintf(stderr, "hub spawn abandon refused: request=%s error=%s status=%s held_by=%s\n", requestID, body["error"], body["status"], body["reason"])
		return ExitConditionInvalid
	}
	fmt.Fprintf(stderr, "hub spawn abandon unavailable: http %d\n", response.StatusCode)
	return ExitInternal
}
