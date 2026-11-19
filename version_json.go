package panewire

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mgh3326/panewire/internal/fleet"
)

// versionDocument is `panewire version --json`: the build version and the
// feature names it advertises (fleet.FeatureCapabilities). A requester gates on
// a feature name before it passes the flag that needs it (hk 5360).
type versionDocument struct {
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
}

// versionJSON encodes the version document for version.
func versionJSON(version string) ([]byte, error) {
	return json.Marshal(versionDocument{Version: version, Capabilities: fleet.FeatureCapabilities()})
}

// printVersionJSON writes the version document to stdout.
func printVersionJSON(version string) int {
	raw, err := versionJSON(version)
	if err != nil {
		fmt.Fprintln(os.Stderr, "version: encode failed")
		return ExitInternal
	}
	fmt.Fprintln(os.Stdout, string(raw))
	return ExitOK
}
