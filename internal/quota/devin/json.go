package devin

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// decodeJSONObject decodes data into out and reports whether it was one valid
// JSON value of the right shape.
func decodeJSONObject(data []byte, out any) bool {
	return data != nil && json.Unmarshal(data, out) == nil
}

func object(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// finiteNumber accepts a decoded JSON number only.
func finiteNumber(v any) (float64, bool) {
	n, ok := v.(float64)
	return n, ok && !math.IsNaN(n) && !math.IsInf(n, 0)
}

// looseNumber accepts a JSON number or a numeric string, as the endpoint sends
// either.
func looseNumber(v any) (float64, bool) {
	if n, ok := finiteNumber(v); ok {
		return n, true
	}
	if text, ok := v.(string); ok {
		return quota.ParseFinite(strings.TrimSpace(text))
	}
	return 0, false
}

// resetAtValue reads a reset time sent as epoch seconds, milliseconds or
// microseconds (number or string), RFC 3339 or an HTTP date.
func resetAtValue(v any) string {
	if n, ok := finiteNumber(v); ok {
		return epochTime(n)
	}
	text := strings.TrimSpace(str(v))
	if text == "" {
		return ""
	}
	if at, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return at.UTC().Format(time.RFC3339Nano)
	}
	if n, ok := quota.ParseFinite(text); ok {
		return epochTime(n)
	}
	if at, err := http.ParseTime(text); err == nil {
		return at.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

func epochTime(n float64) string {
	if n <= 0 || n > 9.2e18 {
		return ""
	}
	var at time.Time
	switch {
	case n >= 1e14:
		at = time.UnixMicro(int64(n))
	case n >= 1e11:
		at = time.UnixMilli(int64(n))
	default:
		at = time.Unix(int64(n), 0)
	}
	at = at.UTC()
	// A quota reset outside this range is a malformed unit, not a real time.
	if at.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) || at.After(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return ""
	}
	return at.Format(time.RFC3339Nano)
}

// decodeBody decodes one JSON object and nothing after it but whitespace: a
// body with trailing data or a second value is malformed, not a measurement.
// (pacelever reads only the first value; the codex source here is strict too.)
func decodeBody(data []byte) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var raw map[string]any
	if decoder.Decode(&raw) != nil || raw == nil || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, false
	}
	return raw, true
}
