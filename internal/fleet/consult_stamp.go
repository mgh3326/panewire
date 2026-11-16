package fleet

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// A planned consult's seat is pinned by the planner, so selection never runs on
// the host that launches the job; the planner's own selection record would be
// lost. The stamp therefore rides the spawn args: the planner writes the
// consult object it selected with, and the host that writes the job's placement
// record merges it into the selection (hk 4875). Only the planner writes the
// flag, so an auto-selected consult keeps producing its record from selection
// alone.

// ConsultStampArg is the spawn argument that carries a planned consult's
// selection stamp. Its value is base64url of a JSON object with exactly the
// keys selPlan.consultStamp writes; see consultStampValue.
const ConsultStampArg = "--consult-stamp"

// consultStampMaxBytes bounds the encoded argument value. A family of
// consultStampValueMax bytes still encodes well inside it.
const consultStampMaxBytes = 320

// consultStampKeys is the stamp contract: the asker's grade and family ("unknown"
// when the spawn named none), where the grade came from, the floor asked and
// the floor used, and the fallback taken, if any.
var consultStampKeys = []string{"asker_grade", "asker_family", "grade_source", "floor", "used_floor", "fallback"}

// consultStampValueMax bounds one stamp value. The asker's family is a consult
// request's author_family (up to 32 bytes, hyphens and dots allowed) or a policy
// seat's free-form Family, so the rule must not be narrower than either; the
// grade, source and fallback names are far shorter. The encoded argument keeps
// its own bound (consultStampMaxBytes).
const consultStampValueMax = 64

// consultStampValue encodes a stamp for the spawn args. The empty string means
// no stamp, which every arg writer skips.
func consultStampValue(stamp map[string]string) string {
	if len(stamp) == 0 {
		return ""
	}
	raw, err := json.Marshal(stamp)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// parseConsultStamp reads the value consultStampValue wrote. The value arrives
// from another host, so the keys, the length and the characters are bounded
// here rather than trusted; a value is any UTF-8 text without a control
// character, because the record stores it as a JSON string.
func parseConsultStamp(value string) (map[string]string, error) {
	if len(value) == 0 || len(value) > consultStampMaxBytes {
		return nil, fmt.Errorf("stamp must be 1-%d bytes", consultStampMaxBytes)
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("stamp is not base64url: %w", err)
	}
	var stamp map[string]string
	if err := json.Unmarshal(raw, &stamp); err != nil {
		return nil, fmt.Errorf("stamp is not a JSON string object: %w", err)
	}
	if len(stamp) != len(consultStampKeys) {
		return nil, fmt.Errorf("stamp must name exactly %d keys", len(consultStampKeys))
	}
	for _, key := range consultStampKeys {
		text, ok := stamp[key]
		if !ok {
			return nil, fmt.Errorf("stamp has no %s", key)
		}
		if len(text) > consultStampValueMax || !utf8.ValidString(text) || strings.ContainsAny(text, "\x00\r\n") {
			return nil, fmt.Errorf("stamp %s is not a bounded text value", key)
		}
		// A consult asked without a task grade has no asker grade, and a
		// selection that met the floor leaves no fallback; nothing else is empty.
		if text == "" && key != "asker_grade" && key != "fallback" {
			return nil, fmt.Errorf("stamp %s is empty", key)
		}
	}
	return stamp, nil
}

// consultStampRecord renders a stamp as the selection record's consult object.
func consultStampRecord(stamp map[string]string) map[string]any {
	record := make(map[string]any, len(stamp))
	for _, key := range consultStampKeys {
		record[key] = stamp[key]
	}
	return record
}

// consultStampFromRecord reads a selection record's consult object into the
// wire stamp. ok is false for a record without one, or with a value that is not
// the string the stamp contract names.
func consultStampFromRecord(record map[string]any) (map[string]string, bool) {
	if record == nil {
		return nil, false
	}
	stamp := make(map[string]string, len(consultStampKeys))
	for _, key := range consultStampKeys {
		text, ok := record[key].(string)
		if !ok {
			return nil, false
		}
		stamp[key] = text
	}
	return stamp, true
}
