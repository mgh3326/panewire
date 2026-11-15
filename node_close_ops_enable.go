package panewire

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// closeOpsRecheckInterval paces the re-check of close-op readiness inside a
// hub session: a readiness change re-sends hello with the new capabilities.
const closeOpsRecheckInterval = 30 * time.Second

// helloCapabilities is what this node advertises in a hello now:
// hubSpawnCapabilities, plus close_ops_v1 while the migration barrier holds
// (fleet.CloseOpsReady: the recorded `panewire node close-ops enable` step and
// no watcher of an older binary live).
func (client *HubClient) helloCapabilities(ctx context.Context) []string {
	capabilities := slices.Clone(hubSpawnCapabilities)
	if client.closeOpsReady(ctx) {
		capabilities = append(capabilities, hubCapabilityCloseOps)
	}
	return capabilities
}

func (client *HubClient) closeOpsReady(ctx context.Context) bool {
	if client.jobsInboxRoot == "" {
		return false
	}
	return fleet.CloseOpsReady(ctx, filepath.Join(client.jobsInboxRoot, "jobs"), client.nodeVersion(), client.closeOpsLister)
}

// nodeVersion is the binary identity the close-ops marker must name
// (fleet.BuildIdentity); tests set closeOpsIdentity.
func (client *HubClient) nodeVersion() string {
	if client.closeOpsIdentity != "" {
		return client.closeOpsIdentity
	}
	return fleet.BuildIdentity()
}

// admitCloseOps is the per-close barrier: a new close registration runs only
// while the barrier holds now, so a launcher that started after the hello's
// check (and holds a receipt a close could bind to) fences it.
func (client *HubClient) admitCloseOps(ctx context.Context) bool {
	if client.closeOpsAdmit != nil {
		return client.closeOpsAdmit(ctx)
	}
	return client.closeOpsReady(ctx)
}

// runNodeCloseOpsCLI runs `panewire node close-ops enable --inbox-root DIR`,
// the recorded migration step after a node upgrade. It refuses with
// ExitPartial, retryable, while an old-binary watcher or a spawn launcher
// runs.
func runNodeCloseOpsCLI(args []string, stdout, stderr io.Writer, now func() time.Time, list fleet.ProcessLister) int {
	if len(args) == 0 || args[0] != "enable" {
		return ExitUsage
	}
	flags := flag.NewFlagSet("panewire node close-ops enable", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	inbox := flags.String("inbox-root", "", "inbox root holding jobs/ (the node's jobs_inbox_root)")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *inbox == "" {
		fmt.Fprintln(stderr, "usage: panewire node close-ops enable --inbox-root DIR")
		return ExitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	jobs := filepath.Join(*inbox, "jobs")
	if err := fleet.EnableCloseOps(ctx, jobs, fleet.BuildIdentity(), now(), list); err != nil {
		fmt.Fprintf(stderr, "panewire node close-ops enable: %v\n", err)
		if errors.Is(err, fleet.ErrCloseOpsNotReady) {
			fmt.Fprintln(stderr, "wait for the old processes to end (or reap their jobs), then run it again")
			return ExitPartial
		}
		return ExitInternal
	}
	fmt.Fprintf(stdout, "close_ops_v1 enabled for %s; the node advertises it from its next hello\n", jobs)
	return ExitOK
}

func (client *HubClient) closeOpsRecheckOrDefault() time.Duration {
	if client.closeOpsRecheck > 0 {
		return client.closeOpsRecheck
	}
	return closeOpsRecheckInterval
}
