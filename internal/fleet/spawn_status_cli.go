package fleet

import (
	"context"
	"encoding/json"
	"fmt"
)

// spawnStatusUsage is the usage line for the read-only status subcommand,
// printed by `spawn --help` and `spawn status --help`.
const spawnStatusUsage = "Usage: panewire spawn status REQUEST_ID [--json] (read-only: one read of the hub operation, never a submit or a wait; exit 0 succeeded/landed, the hub's rc (81 when none) failed/retryable/refused, 84 cancelled/given_up, 75 deadline_exceeded, 91 parked, 85 any other in-flight or unknown state; docs/hub-spawn-operations.md)"

// spawnRecordReader is a hub transport that can read one spawn operation's
// record (hubHTTP.ReadSpawnStatus).
type spawnRecordReader interface {
	ReadSpawnStatus(context.Context, string) (SpawnStatusRecord, error)
}

// spawnStatusCommand reports whether spawn's arguments name the status
// subcommand.
func spawnStatusCommand(args []string) bool {
	return len(args) > 0 && args[0] == "status"
}

// spawnStatusCmd is `panewire spawn status REQUEST_ID`: one read of a hub
// spawn request (docs/hub-spawn-operations.md, "Request states a client
// reads"). It is how a caller observes an accepted request: rerunning spawn
// would replay a retryable request as a new attempt with a new time limit.
func (a *App) spawnStatusCmd(ctx context.Context, args []string) error {
	defer a.reportHubRetries()
	if len(args) > 0 && (args[0] == "--help" || args[0] == "-h") {
		fmt.Fprintln(a.Out, spawnStatusUsage)
		return nil
	}
	// The id is refused before the hub config is read: a mistyped argument is
	// a usage error whether or not this host has a credential.
	if len(args) == 0 || !validID(args[0]) {
		return fail(exitUsage, "spawn status needs one valid REQUEST_ID (usage: panewire spawn status REQUEST_ID [--json])")
	}
	id := args[0]
	f := flags("spawn status")
	asJSON := f.Bool("json", false, "print the record as JSON")
	if err := f.Parse(args[1:]); err != nil {
		return fail(exitUsage, "spawn status: %v (usage: panewire spawn status REQUEST_ID [--json])", err)
	}
	if f.NArg() != 0 {
		return fail(exitUsage, "unexpected spawn status arguments (usage: panewire spawn status REQUEST_ID [--json])")
	}
	hub, err := a.hub()
	if err != nil {
		return err
	}
	reader, ok := hub.(spawnRecordReader)
	if !ok {
		return fail(exitConfig, "hub transport cannot read spawn status; configure a current panewire hub")
	}
	record, err := reader.ReadSpawnStatus(ctx, id)
	if err != nil {
		return err
	}
	if *asJSON {
		encoded, _ := json.MarshalIndent(record, "", "  ")
		fmt.Fprintln(a.Out, string(encoded))
	} else {
		fmt.Fprintln(a.Out, record.line())
	}
	if code := spawnStatusExit(record); code != 0 {
		return fail(code, "spawn request %s is %s", record.Request, record.State)
	}
	return nil
}
