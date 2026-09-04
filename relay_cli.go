package panewire

import (
	"encoding/json"
	"flag"
	"io"
)

// relay routes is deliberately read-only: the operator owns the mode-0600
// route file. `test` validates a named route before an authenticated hub test.
func runRelayCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || (args[0] != "routes" && args[0] != "test") {
		return ExitUsage
	}
	fs := flag.NewFlagSet("panewire relay "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("routes", "/etc/panewire/lanes.json", "operator-owned lane route JSON")
	lane := fs.String("lane", "", "owner lane (required for test)")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
		return ExitUsage
	}
	routes := loadReportRelayRoutes(*path)
	if routes == nil {
		return ExitConditionInvalid
	}
	if args[0] == "routes" {
		_ = json.NewEncoder(stdout).Encode(reportRelayRoutes{Routes: routes})
		return ExitOK
	}
	if *lane == "" || routes[*lane].Machine == "" {
		return ExitConditionInvalid
	}
	// No prompt is made from this CLI: the hub's websocket round trip carries
	// a test directive and reports relay.delivered/unconfirmed to the operator.
	_, _ = io.WriteString(stdout, "route ready; send hub test event to await receipt\n")
	return ExitOK
}
