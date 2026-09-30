package panewire

import (
	"fmt"
	"io"
	"strings"
)

const hubCredentialUsage = "--hub-url URL --hub-token-env FILE [--hub-cf-env FILE]"

const lanesUsage = `Usage:
  panewire lanes ls ` + hubCredentialUsage + `
  panewire lanes add LANE --machine MACHINE --pane PANE [--parent LANE] [--sink] ` + hubCredentialUsage + `
  panewire lanes rm LANE ` + hubCredentialUsage + `
  panewire lanes self-check --lane LANE --expect-machine MACHINE --expect-pane PANE --expect-epoch N ` + hubCredentialUsage + `

--hub-token-env names a mode-0600 file with HUB_MACHINE_ID and HUB_TOKEN. An
operator file (HUB_MACHINE_ID=operator) sees and manages every lane. A node
file sees and manages only the lanes routed to its own machine id: ls lists
just those lanes, add is refused unless --machine equals it, --parent names a
lane on it, and the new name is not another machine lane's parent, and rm
removes only its own lanes no foreign lane parents. self-check requires an
operator file.`

const jobsUsage = `Usage:
  panewire jobs jobs [--machine MACHINE] ` + hubCredentialUsage + `
  panewire jobs orphaned ` + hubCredentialUsage + `
  panewire jobs reassign --job-id JOB --to MACHINE ` + hubCredentialUsage

const sessionsUsage = "Usage: panewire sessions find LABEL [--contains] [--machine MACHINE] [--json] " + hubCredentialUsage
const fleetCensusUsage = "Usage: panewire fleet-census [--json] [--grace D] [--jobs-root PATH] [--herdr-socket PATH] [--machine-id ID] [--node-env FILE] [" + hubCredentialUsage + "]"
const sessionReapUsage = "Usage: panewire session-reap [--json] [--grace D] [--jobs-root PATH] [--herdr-socket PATH]"
const lanesAuditUsage = "Usage: panewire lanes-audit [--json] [--sibling A=B] " + hubCredentialUsage

var (
	lanesValueFlags       = hubCLIFlagSet("--hub-url --hub-token-env --hub-cf-env --machine --pane --parent --lane --expect-machine --expect-pane --expect-epoch")
	lanesKnownFlags       = hubCLIFlagSet("--sink --hub-url --hub-token-env --hub-cf-env --machine --pane --parent --lane --expect-machine --expect-pane --expect-epoch")
	sessionsValueFlags    = hubCLIFlagSet("--hub-url --hub-token-env --hub-cf-env --machine")
	sessionsKnownFlags    = hubCLIFlagSet("--contains --json --hub-url --hub-token-env --hub-cf-env --machine")
	fleetCensusValueFlags = hubCLIFlagSet("--grace --jobs-root --herdr-socket --machine-id --node-env --hub-url --hub-token-env --hub-cf-env")
	fleetCensusKnownFlags = hubCLIFlagSet("--json --grace --jobs-root --herdr-socket --machine-id --node-env --hub-url --hub-token-env --hub-cf-env")
	sessionReapValueFlags = hubCLIFlagSet("--grace --jobs-root --herdr-socket")
	sessionReapKnownFlags = hubCLIFlagSet("--json --grace --jobs-root --herdr-socket")
	lanesAuditValueFlags  = hubCLIFlagSet("--hub-url --hub-token-env --hub-cf-env --sibling")
	lanesAuditKnownFlags  = hubCLIFlagSet("--json --hub-url --hub-token-env --hub-cf-env --sibling")
)

func hubCLIFlagSet(names string) map[string]bool {
	flags := make(map[string]bool)
	for _, name := range strings.Fields(names) {
		flags[name] = true
	}
	return flags
}

func writeHubCLIUsage(stderr io.Writer, reason, usage string) int {
	fmt.Fprintln(stderr, reason)
	fmt.Fprintln(stderr, usage)
	return ExitUsage
}

// A following recognized flag suggests an omitted value only after the
// original parser has rejected the invocation. Some valid values are literal
// names beginning with "--", so this must not preempt successful parsing.
func missingHubCLIFlagValue(args []string, valueFlags, knownFlags map[string]bool) bool {
	for index := 0; index+1 < len(args); index++ {
		name, _, hasValue := strings.Cut(args[index], "=")
		if !hasValue && valueFlags[name] && knownFlags[args[index+1]] {
			return true
		}
	}
	return false
}
