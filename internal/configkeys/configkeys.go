// Package configkeys reports the keys of a config document that the binary's
// target type does not read. A release that removes a key must not refuse a
// file that still carries it: the loader logs each such key once and goes on.
// A malformed value for a key the binary does read still refuses, in the
// loader itself.
package configkeys

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// JSON decodes the first JSON value in data generically and returns the
// dotted paths of keys that target's type does not read, sorted. target is a
// pointer to the struct the document decodes into.
func JSON(data []byte, target any) ([]string, error) {
	var doc any
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&doc); err != nil {
		return nil, err
	}
	return Unknown(doc, target, "json"), nil
}

// YAML is JSON for the first YAML document in data.
func YAML(data []byte, target any) ([]string, error) {
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return Unknown(doc, target, "yaml"), nil
}

// warned records each (file, key) pair Warn has logged, so a file read on every
// call (spawn.json, read per node request) warns once per key, not per read.
var (
	warnedMu sync.Mutex
	warned   = map[string]bool{}
)

// Warn logs one warning per key, naming the key and the file it came from. A
// key is logged once per process per file. A nil logger logs to slog's default.
func Warn(logger *slog.Logger, file string, keys []string) {
	if logger == nil {
		logger = slog.Default()
	}
	warnedMu.Lock()
	defer warnedMu.Unlock()
	for _, key := range keys {
		id := file + "\x00" + key
		if warned[id] {
			continue
		}
		warned[id] = true
		logger.Warn("config key is not read by this release; ignored", "key", key, "file", file)
	}
}

// Unknown walks a generically decoded document against target's type and
// returns the dotted paths (with [i] for slice indexes) of keys that no struct
// field accepts, sorted. tag names the format's struct tag ("json", "yaml").
// Keys match a field's tag name exactly, or an untagged field name without
// regard to case, as the decoders do. Maps are walked by value type; struct
// types with their own unmarshaler are not walked, since their document shape
// is theirs to read.
func Unknown(doc any, target any, tag string) []string {
	var out []string
	walk(reflect.TypeOf(target), doc, "", tag, &out)
	slices.Sort(out)
	return out
}

func walk(t reflect.Type, doc any, path, tag string, out *[]string) {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || hasUnmarshaler(t) {
		return
	}
	switch d := doc.(type) {
	case map[string]any:
		switch t.Kind() {
		case reflect.Struct:
			fields := fieldsOf(t, tag, nil)
			for _, key := range slices.Sorted(maps.Keys(d)) {
				at := join(path, key)
				ft, ok := lookup(fields, key)
				if !ok {
					*out = append(*out, at)
					continue
				}
				walk(ft, d[key], at, tag, out)
			}
		case reflect.Map:
			for _, key := range slices.Sorted(maps.Keys(d)) {
				walk(t.Elem(), d[key], join(path, key), tag, out)
			}
		}
	case []any:
		if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			for i, v := range d {
				walk(t.Elem(), v, fmt.Sprintf("%s[%d]", path, i), tag, out)
			}
		}
	}
}

type field struct {
	name string
	typ  reflect.Type
}

// fieldsOf lists the names a struct type accepts under tag. An embedded struct
// without a tag name (json embedding, yaml ",inline") contributes its fields
// directly, as both decoders do.
func fieldsOf(t reflect.Type, tag string, into []field) []field {
	for i := range t.NumField() {
		f := t.Field(i)
		raw := f.Tag.Get(tag)
		if raw == "-" {
			continue
		}
		name, _, _ := strings.Cut(raw, ",")
		if f.Anonymous && name == "" {
			inner := f.Type
			for inner.Kind() == reflect.Pointer {
				inner = inner.Elem()
			}
			if inner.Kind() == reflect.Struct {
				into = fieldsOf(inner, tag, into)
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		into = append(into, field{name: name, typ: f.Type})
	}
	return into
}

func lookup(fields []field, key string) (reflect.Type, bool) {
	for _, f := range fields {
		if f.name == key {
			return f.typ, true
		}
	}
	for _, f := range fields {
		if strings.EqualFold(f.name, key) {
			return f.typ, true
		}
	}
	return nil, false
}

// hasUnmarshaler reports whether t decodes itself from JSON or YAML.
func hasUnmarshaler(t reflect.Type) bool {
	p := reflect.PointerTo(t)
	for _, name := range []string{"UnmarshalJSON", "UnmarshalYAML"} {
		if _, ok := p.MethodByName(name); ok {
			return true
		}
	}
	return false
}

func join(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
