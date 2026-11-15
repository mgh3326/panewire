package panewire

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const placementRequeueUsage = `Usage: panewire placement-requeue [--state-dir DIR]

Re-posts every placement record quarantined under placements-rejected/,
mapping its role to hk's work class. The hub's outbox never revisits a sealed
snapshot, so this is the one command that replays records hk refused. It uses
the ambient handoffkeep credential and [quota] URL; --state-dir points it at a
state directory other than panewire's default.
`

// runPlacementRequeueCLI is the operator's one-shot replay of quarantined
// placement records (hk 4728). A record stays in the quarantine until hk takes
// it, so an outage or a second refusal is safe to retry later.
func runPlacementRequeueCLI(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("panewire placement-requeue", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateDir := flags.String("state-dir", "", "state directory holding placements-rejected")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		fmt.Fprint(stderr, placementRequeueUsage)
		return ExitUsage
	}
	app, err := fleet.New(io.Discard, io.Discard)
	if err != nil {
		fmt.Fprintln(stderr, "placement-requeue unavailable: panewire configuration is unreadable")
		return ExitConditionInvalid
	}
	if *stateDir != "" {
		app.StateDir = *stateDir
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := app.RequeueRejectedPlacements(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "placement-requeue failed: %s is unreadable\n", result.Dir)
		return ExitInternal
	}
	counts := map[string]int{}
	for _, row := range result.Records {
		counts[row.Outcome]++
		line := fmt.Sprintf("%s job=%s attempt=%d role=%s %s", row.Name, row.Job, row.Attempt, row.Role, row.Outcome)
		if row.Reason != "" {
			line += ": " + row.Reason
		}
		fmt.Fprintln(stdout, line)
	}
	fmt.Fprintf(stdout, "placement-requeue: %d record(s) in %s: %d delivered, %d already held by hk, %d refused, %d pending, %d unreadable\n",
		len(result.Records), result.Dir, counts["delivered"], counts["duplicate"], counts["refused"], counts["pending"], counts["unreadable"])
	if counts["refused"] > 0 || counts["pending"] > 0 || counts["unreadable"] > 0 {
		return ExitDeliveryFailure
	}
	return ExitOK
}
