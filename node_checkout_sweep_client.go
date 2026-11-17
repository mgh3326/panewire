package panewire

import (
	"context"
	"flag"
	"io"
	"time"
)

// runNodeCheckoutSweepCLI is `panewire node sweep-checkouts`; it prints the
// pass's rows as one JSON document, like the other node verbs. --dry-run
// lists what would be removed and changes nothing.
func runNodeCheckoutSweepCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node sweep-checkouts", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	connect.allowOperator = true
	// The hub waits hubCheckoutSweepWait for this op; the client outlasts it.
	connect.timeout = hubCheckoutSweepWait + 30*time.Second
	flags.StringVar(connect.machine, "host", "", "machine to sweep")
	dryRun := flags.Bool("dry-run", false, "list what would be removed without changing anything")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	return connect.run(stdout, stderr, deps, func(ctx context.Context, client *NodeReapClient, machine string) (any, error) {
		var result nodeCheckoutSweepResult
		err := client.command(ctx, machine, nodeCommandCheckoutSweep, nodeCheckoutSweepArgs{DryRun: *dryRun}, &result)
		return result, err
	})
}
