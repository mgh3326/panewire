package panewire

import (
	"fmt"
	"io"
	"strings"
)

const hubCredentialUsage = "--hub-url URL --hub-token-env FILE [--hub-cf-env FILE]"

const lanesUsage = `Usage:
  panewire lanes ls ` + hubCredentialUsage + `
  panewire lanes add LANE [--machine MACHINE --pane PANE | --sink] [--parent LANE] [--takeover JOB] ` + hubCredentialUsage + `
  panewire lanes rm LANE ` + hubCredentialUsage + `
  panewire lanes self-check --lane LANE --expect-machine MACHINE --expect-pane PANE --expect-epoch N ` + hubCredentialUsage + `

--hub-token-env names a mode-0600 file with HUB_MACHINE_ID and HUB_TOKEN. An
operator file (HUB_MACHINE_ID=operator) sees and manages every lane. A node
file sees and manages only the lanes routed to its own machine id: ls lists
just those lanes, add is refused unless --machine equals it, --parent names an
existing lane on it, and the new name is not referenced by another machine's
lanes or jobs, and rm removes only its own lanes no foreign machine
references. self-check requires an operator file. --sink registers a lane
with no machine/pane route; the hub discards route fields for it, and only an
operator token may write one. --takeover JOB moves the row the hub holds at
JOB's pane to this route, keeping the row's name, so a --parent-job successor
can carry the lane its task's findings use onto its own machine. A takeover
the hub cannot confirm from its own job record answers 409
lane_takeover_unconfirmed.`

const jobsUsage = `Usage:
  panewire jobs list --lane LANE [--json] ` + hubCredentialUsage + `
  panewire jobs jobs [--machine MACHINE] [--include-completed] ` + hubCredentialUsage + `
  panewire jobs orphaned ` + hubCredentialUsage + `
  panewire jobs reassign --job-id JOB --to MACHINE ` + hubCredentialUsage + `
` + ownJobsUsage

const fleetCensusFindUsage = "Usage: panewire fleet-census find LABEL [--contains] [--machine MACHINE] [--json] " + hubCredentialUsage
const fleetCensusUsage = "Usage: panewire fleet-census [--json] [--grace D] [--jobs-root PATH] [--herdr-socket PATH] [--machine-id ID] [--node-env FILE] [" + hubCredentialUsage + "]\n" + fleetCensusFindUsage

var (
	lanesValueFlags       = hubCLIFlagSet("--hub-url --hub-token-env --hub-cf-env --machine --pane --parent --takeover --lane --expect-machine --expect-pane --expect-epoch")
	lanesKnownFlags       = hubCLIFlagSet("--sink --hub-url --hub-token-env --hub-cf-env --machine --pane --parent --takeover --lane --expect-machine --expect-pane --expect-epoch")
	sessionsValueFlags    = hubCLIFlagSet("--hub-url --hub-token-env --hub-cf-env --machine")
	sessionsKnownFlags    = hubCLIFlagSet("--contains --json --hub-url --hub-token-env --hub-cf-env --machine")
	fleetCensusValueFlags = hubCLIFlagSet("--grace --jobs-root --herdr-socket --machine-id --node-env --hub-url --hub-token-env --hub-cf-env")
	fleetCensusKnownFlags = hubCLIFlagSet("--json --grace --jobs-root --herdr-socket --machine-id --node-env --hub-url --hub-token-env --hub-cf-env")
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
