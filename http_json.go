package panewire

import (
	"encoding/json"
	"net/http"
)

// writeJSON is the single JSON response writer for every hub HTTP handler:
// Content-Type, then status, then the json.Encoder body (which ends in a
// newline).
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

// writeJSONBytes writes an already-encoded JSON body byte for byte, for wire
// contracts pinned without json.Encoder's trailing newline.
func writeJSONBytes(writer http.ResponseWriter, status int, body []byte) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}
