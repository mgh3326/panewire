package panewire

import (
	"fmt"
	"io"
)

const hubCredentialUsage = "--hub-url URL --hub-token-env FILE [--hub-cf-env FILE]"

const lanesUsage = `Usage:
  panewire lanes ls ` + hubCredentialUsage + `
  panewire lanes add LANE --machine MACHINE --pane PANE [--parent LANE] [--sink] ` + hubCredentialUsage + `
  panewire lanes rm LANE ` + hubCredentialUsage + `
  panewire lanes self-check --lane LANE --expect-machine MACHINE --expect-pane PANE --expect-epoch N ` + hubCredentialUsage

const jobsUsage = `Usage:
  panewire jobs jobs [--machine MACHINE] ` + hubCredentialUsage + `
  panewire jobs orphaned ` + hubCredentialUsage + `
  panewire jobs reassign --job-id JOB --to MACHINE ` + hubCredentialUsage

const sessionsUsage = "Usage: panewire sessions find LABEL [--contains] [--machine MACHINE] [--json] " + hubCredentialUsage
const fleetCensusUsage = "Usage: panewire fleet-census [--json] [--grace D] [--jobs-root PATH] [--herdr-socket PATH] [--machine-id ID] [--node-env FILE] [" + hubCredentialUsage + "]"
const sessionReapUsage = "Usage: panewire session-reap [--json] [--grace D] [--jobs-root PATH] [--herdr-socket PATH]"
const lanesAuditUsage = "Usage: panewire lanes-audit [--json] " + hubCredentialUsage

func writeHubCLIUsage(stderr io.Writer, reason, usage string) int {
	fmt.Fprintln(stderr, reason)
	fmt.Fprintln(stderr, usage)
	return ExitUsage
}
