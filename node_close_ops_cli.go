package panewire

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// runNodeCloseOpsCLI runs `panewire node close-ops enable`, the close-op
// migration barrier (hk 4632; docs/node-commands.md, "Enabling close
// operations"). Run on the node host after its panewire binary is upgraded:
// it records the barrier under the jobs root only when no older-binary
// panewire watcher or launcher runs there. The running node then advertises
// close_ops_v1 at its next re-check, without a restart. list nil runs ps.
func runNodeCloseOpsCLI(args []string, stdout, stderr io.Writer, list fleet.ProcessLister) int {
	if len(args) == 0 || args[0] != "enable" {
		fmt.Fprintln(stderr, "usage: panewire node close-ops enable [--inbox-root DIR]")
		return ExitUsage
	}
	flags := flag.NewFlagSet("panewire node close-ops enable", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inboxRoot := flags.String("inbox-root", "", "the node's jobs inbox root (default: PANEWIRE_INBOX_ROOT or ~/work/herdr-inbox)")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	jobs := filepath.Join(emitInboxRoot(*inboxRoot), "jobs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fleet.EnableCloseOps(ctx, jobs, list, time.Now()); err != nil {
		fmt.Fprintf(stderr, "panewire node close-ops enable: %v\n", err)
		if errors.Is(err, fleet.ErrCloseOpsNotReady) {
			return ExitConditionInvalid
		}
		return ExitInternal
	}
	fmt.Fprintf(stdout, "close ops enabled under %s; the node advertises close_ops_v1 within %s\n", jobs, closeOpsRecheckInterval)
	return ExitOK
}
