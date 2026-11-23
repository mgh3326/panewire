package panewire

import (
	"fmt"
	"io"
	"net/http"
)

const hubLadderUsage = `Usage:
  panewire hub ladder release STAGE_REQUEST_ID --reason TEXT [` + hubCredentialUsage + `]

release frees a ladder stage that is still staged and whose close verdict settles
that no close can run for it: a legacy marker stage, or a v1 stage the node
answers unknown, fenced or refused (a v1 stage is fenced first, so no close for
its K runs afterwards). A stage whose close is in progress (closing) or whose
fence does not answer is refused and retried; a stage whose pane was closed or
ended (closed, gone) is the reconciler's. Not staged or not a ladder step is
refused too. The holder's role and the reservation come back as after a normal
release. A repeat of a release that landed answers the same result.
It needs an operator token env (HUB_MACHINE_ID=operator); see docs/escalate-author.md.
--hub-url, --hub-token-env and --hub-cf-env default to PANEWIRE_HUB_URL,
PANEWIRE_HUB_TOKEN_ENV and PANEWIRE_HUB_CF_ENV.`

// runHubLadderCLI is `panewire hub ladder`, the operator's handle on the ladder
// steps the reconciler keeps (docs/escalate-author.md, "Releasing an unresolved stage").
func runHubLadderCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "release" {
		return writeHubCLIUsage(stderr, "hub ladder: expected the release subcommand", hubLadderUsage)
	}
	return runHubOperatorReason("hub ladder release", hubLadderUsage, func(requestID string) string {
		return "/v1/ladder/stages/" + requestID + "/release"
	}, args[1:], stdout, stderr, deps, func(requestID string, status int, body map[string]string) int {
		switch status {
		case http.StatusOK:
			fmt.Fprintf(stdout, "RELEASED request=%s job=%s\n", requestID, body["job"])
			return ExitOK
		case http.StatusConflict, http.StatusNotFound:
			fmt.Fprintf(stderr, "hub ladder release refused: request=%s error=%s status=%s state=%s\n", requestID, body["error"], body["status"], body["state"])
			return ExitConditionInvalid
		}
		fmt.Fprintf(stderr, "hub ladder release unavailable: http %d\n", status)
		return ExitInternal
	})
}
