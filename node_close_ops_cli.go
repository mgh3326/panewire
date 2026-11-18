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

// runNodeCloseOpsCLI runs the close-op rollout steps on a node host (hk 4632;
// docs/node-commands.md, "Enabling close operations"):
//
//   - `enable`, the migration barrier: run after the node's binary is
//     upgraded, it records the barrier under the jobs root only when no
//     older-binary watcher runs there.
//   - `advertise`, off by default: after the barrier, it turns close_ops_v1
//     on, so the running node advertises it at its next re-check. hk 4745's
//     rollout runs it once the hub issuer is deployed.
//   - `drain`, withdrawal for rollback: quiesce closes without restarting the
//     node, then report pending jobs until settlement finishes them and the
//     hub has acknowledged every close the drain cancelled (hk 5073).
//
// list nil runs ps.
func runNodeCloseOpsCLI(args []string, stdout, stderr io.Writer, list fleet.ProcessLister) int {
	if len(args) == 0 || args[0] != "enable" && args[0] != "advertise" && args[0] != "drain" {
		fmt.Fprintln(stderr, "usage: panewire node close-ops enable|advertise|drain [--inbox-root DIR]")
		return ExitUsage
	}
	verb := args[0]
	flags := flag.NewFlagSet("panewire node close-ops "+verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	inboxRoot := flags.String("inbox-root", "", "the node's jobs inbox root (default: PANEWIRE_INBOX_ROOT or ~/work/herdr-inbox)")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 {
		return ExitUsage
	}
	jobs := filepath.Join(emitInboxRoot(*inboxRoot), "jobs")
	if verb == "drain" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fleet.BeginCloseOpsDrain(ctx, jobs); err != nil {
			fmt.Fprintf(stderr, "panewire node close-ops drain: %v\n", err)
			return ExitInternal
		}
		pending, err := fleet.PendingHubCloseJobs(jobs)
		if err != nil {
			fmt.Fprintf(stderr, "panewire node close-ops drain: %v\n", err)
			return ExitInternal
		}
		if len(pending) != 0 {
			fmt.Fprintf(stdout, "close ops draining; pending jobs: %v\n", pending)
			return ExitConditionInvalid
		}
		// A drain-cancelled close the hub has not acknowledged is not drained:
		// the hub reads the cancellation on its own pass, and a rolled-back
		// binary would no longer answer it (hk 5073).
		unacked, err := fleet.PendingHubCloseAckJobs(jobs)
		if err != nil {
			fmt.Fprintf(stderr, "panewire node close-ops drain: %v\n", err)
			return ExitInternal
		}
		if len(unacked) != 0 {
			fmt.Fprintf(stdout, "close ops draining; waiting for the hub to acknowledge cancelled closes of jobs: %v\n", unacked)
			return ExitConditionInvalid
		}
		fmt.Fprintln(stdout, "close ops drained; no pending jobs")
		return ExitOK
	}
	if verb == "advertise" {
		if err := fleet.AdvertiseCloseOps(jobs, time.Now()); err != nil {
			fmt.Fprintf(stderr, "panewire node close-ops advertise: %v\n", err)
			if errors.Is(err, fleet.ErrCloseOpsNoBarrier) || errors.Is(err, fleet.ErrCloseOpsDraining) {
				return ExitConditionInvalid
			}
			return ExitInternal
		}
		fmt.Fprintf(stdout, "close ops advertised under %s; the node advertises close_ops_v1 within %s while no older watcher runs\n", jobs, closeOpsRecheckInterval)
		return ExitOK
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fleet.EnableCloseOps(ctx, jobs, list, time.Now()); err != nil {
		fmt.Fprintf(stderr, "panewire node close-ops enable: %v\n", err)
		if errors.Is(err, fleet.ErrCloseOpsNotReady) {
			return ExitConditionInvalid
		}
		return ExitInternal
	}
	fmt.Fprintf(stdout, "close ops barrier recorded under %s; close_ops_v1 stays off until `panewire node close-ops advertise`\n", jobs)
	return ExitOK
}
