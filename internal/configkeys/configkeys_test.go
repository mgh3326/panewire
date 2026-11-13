package configkeys

import (
	"slices"
	"testing"
)

type inner struct {
	Mode string `json:"mode" yaml:"mode"`
}

type custom struct {
	Raw string `json:"raw"`
}

func (c *custom) UnmarshalJSON(b []byte) error { c.Raw = string(b); return nil }

type sample struct {
	Name     string           `json:"name" yaml:"name"`
	Plain    int              `json:"-" yaml:"plain"`
	Untagged string           `json:"Untagged"`
	Nested   *inner           `json:"nested" yaml:"nested"`
	Items    []inner          `json:"items" yaml:"items"`
	ByName   map[string]inner `json:"by_name" yaml:"by_name"`
	Opaque   custom           `json:"opaque"`
	Embedded
	hidden string
}

type Embedded struct {
	Shared string `yaml:"shared"`
}

func TestUnknownReportsKeysNoFieldAccepts(t *testing.T) {
	doc := map[string]any{
		"name":    "ok",
		"gone":    1,
		"nested":  map[string]any{"mode": "x", "old": true},
		"items":   []any{map[string]any{"mode": "a"}, map[string]any{"retired": 2}},
		"by_name": map[string]any{"a": map[string]any{"mode": "m", "stale": 3}},
		"opaque":  map[string]any{"anything": 4},
		"shared":  "via embedded",
		"plain":   "json-ignored, yaml-known",
	}
	got := Unknown(doc, &sample{}, "json")
	want := []string{"by_name.a.stale", "gone", "items[1].retired", "nested.old", "plain"}
	if !slices.Equal(got, want) {
		t.Fatalf("Unknown = %v, want %v", got, want)
	}
}

func TestUnknownMatchesUntaggedFieldsWithoutCase(t *testing.T) {
	doc := map[string]any{"untagged": "x", "UNTAGGED": "y"}
	if got := Unknown(doc, &sample{}, "json"); len(got) != 0 {
		t.Fatalf("case-insensitive untagged match flagged %v", got)
	}
}

func TestUnknownIgnoresScalarsAndTypeMismatches(t *testing.T) {
	doc := map[string]any{"nested": "not a table", "items": map[string]any{"mode": "x"}}
	if got := Unknown(doc, &sample{}, "json"); len(got) != 0 {
		t.Fatalf("type mismatch is the decoder's error, not an unknown key: %v", got)
	}
}

func TestJSONAndYAMLReturnFirstDocumentPaths(t *testing.T) {
	got, err := JSON([]byte(`{"name":"a","gone":{"x":1}} {"ignored":true}`), &sample{})
	if err != nil || !slices.Equal(got, []string{"gone"}) {
		t.Fatalf("JSON = %v, %v; want [gone], nil", got, err)
	}
	got, err = YAML([]byte("name: a\ngone: 1\n"), &sample{})
	if err != nil || !slices.Equal(got, []string{"gone"}) {
		t.Fatalf("YAML = %v, %v; want [gone], nil", got, err)
	}
	if _, err := JSON([]byte(`{"name":`), &sample{}); err == nil {
		t.Fatal("malformed JSON returned no error")
	}
}
