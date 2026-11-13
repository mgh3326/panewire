package panewire

import (
	"io"
	"os"
	"strings"
	"testing"
)

// hk 3708: the expect: header contract must be discoverable where a prompt
// file is written (`panewire prompt --help`) and where it is rejected (the
// parse error), so a malformed file teaches the format it violated.
func TestPromptHelpShowsExpectHeaderFormatT3708(t *testing.T) {
	for _, flag := range []string{"-h", "--help"} {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		previous := os.Stdout
		os.Stdout = writer
		rc := runPromptCLI([]string{flag}, CLIConfig{})
		_ = writer.Close()
		os.Stdout = previous
		output, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if rc != ExitOK {
			t.Fatalf("flag=%q exit=%d want %d; output=%q", flag, rc, ExitOK, output)
		}
		for _, want := range []string{"expect:", "name=", "cwd=", "title~=", "recent~=", "expect: name=orch cwd="} {
			if !strings.Contains(string(output), want) {
				t.Fatalf("flag=%q help output lacks %q:\n%s", flag, want, output)
			}
		}
	}
}

func TestPromptParseErrorShowsExpectHeaderFormatT3708(t *testing.T) {
	for _, raw := range []string{
		"no header here\n\nbody\n",
		"\n\n   \nbody without a header\n",
		"",
	} {
		_, _, err := parsePromptFile(raw)
		if err == nil {
			t.Fatalf("parsePromptFile(%q) succeeded, want a missing-header error", raw)
		}
		for _, want := range []string{"expect:", "name=", "cwd=", "title~=", "recent~=", "expect: name=orch cwd="} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("parse error %q lacks %q", err, want)
			}
		}
	}
}
