package panewire

import (
	"encoding/json"
	"net/http"
)

// writeJSON is the single JSON response writer for every hub, assistant, and
// control-plane HTTP handler: Content-Type, then status, then the
// json.Encoder body (which ends in a newline).
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
