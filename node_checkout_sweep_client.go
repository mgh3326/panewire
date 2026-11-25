package panewire

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

const nodeCheckoutSweepUsage = "Usage: panewire node sweep-checkouts --host MACHINE " + hubCredentialUsage + " [--dry-run]"

// runNodeCheckoutSweepCLI is `panewire node sweep-checkouts`; it prints the
// pass's rows as one JSON document, like the other node verbs. --dry-run
// lists what would be removed and changes nothing. A bad invocation says what
// is wrong, then the usage.
func runNodeCheckoutSweepCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node sweep-checkouts", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	connect.allowOperator = true
	// The hub waits hubCheckoutSweepWait for this op; the client outlasts it.
	connect.timeout = hubCheckoutSweepWait + 30*time.Second
	flags.StringVar(connect.machine, "host", "", "machine to sweep")
	dryRun := flags.Bool("dry-run", false, "list what would be removed without changing anything")
	refuse := func(reason string) int {
		return writeHubCLIUsage(stderr, "panewire node sweep-checkouts: "+reason, nodeCheckoutSweepUsage)
	}
	if err := flags.Parse(args); errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, nodeCheckoutSweepUsage)
		return ExitOK
	} else if err != nil {
		return refuse(err.Error())
	}
	switch {
	case flags.NArg() != 0:
		return refuse(fmt.Sprintf("unexpected argument %q", flags.Arg(0)))
	case *connect.hubURL == "":
		return refuse("--hub-url is required")
	case *connect.tokenPath == "":
		return refuse("--hub-token-env is required")
	case !machineIDPattern.MatchString(*connect.machine) || *connect.machine == hubOperatorMachineID:
		return refuse("--host (or --machine) must name a node's machine id")
	}
	return connect.run(stdout, stderr, deps, func(ctx context.Context, client *NodeReapClient, machine string) (any, error) {
		var result nodeCheckoutSweepResult
		err := client.command(ctx, machine, nodeCommandCheckoutSweep, nodeCheckoutSweepArgs{DryRun: *dryRun}, &result)
		return result, err
	})
}
