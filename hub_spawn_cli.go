package panewire

import (
	"fmt"
	"io"
	"net/http"
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
	return runHubOperatorReason("hub spawn abandon", hubSpawnUsage, func(requestID string) string {
		return "/v1/spawns/" + requestID + "/abandon"
	}, args[1:], stdout, stderr, deps, func(requestID string, status int, body map[string]string) int {
		switch status {
		case http.StatusOK:
			fmt.Fprintf(stdout, "ABANDONED request=%s job=%s status=%s\n", requestID, body["job_id"], body["status"])
			return ExitOK
		case http.StatusConflict, http.StatusNotFound:
			fmt.Fprintf(stderr, "hub spawn abandon refused: request=%s error=%s status=%s held_by=%s\n", requestID, body["error"], body["status"], body["reason"])
			return ExitConditionInvalid
		}
		fmt.Fprintf(stderr, "hub spawn abandon unavailable: http %d\n", status)
		return ExitInternal
	})
}
