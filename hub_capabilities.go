package panewire

import (
	"net/http"

	"github.com/mgh3326/panewire/internal/fleet"
)

// handleCapabilities answers the feature names this hub build advertises
// (fleet.FeatureCapabilities). They are names, not data, so the route needs no
// grant: a requester reads them before it passes a flag that needs one (hk 5360).
func (h *HubServer) handleCapabilities(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, struct {
		Capabilities []string `json:"capabilities"`
	}{Capabilities: fleet.FeatureCapabilities()})
}
