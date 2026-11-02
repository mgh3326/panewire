package fleet

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// mock refuses harnesses without exclusive MCP configuration, and never starts
// a server itself. The harness owns the reviewed stdio child's lifetime.
func (a *App) mock(_ context.Context, s Spawn, seat Seat) ([]string, error) {
	if s.ExecutionLane != "mock" {
		return nil, nil
	}
	if seat.Kind != "claude" && seat.Kind != "codex" {
		return nil, fail(exitMockRefused, "mock isolation unsupported for %s", seat.Kind)
	}
	if os.Getenv("MOCK_MCP_PROFILE") == "" {
		return nil, fail(exitMockRefused, "mock requires MOCK_MCP_PROFILE")
	}
	path := a.envOr("PANEWIRE_MOCK_MCP_CONFIG", "WRK_MOCK_MCP_CONFIG")
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, fail(exitMockRefused, "mock requires readable PANEWIRE_MOCK_MCP_CONFIG")
	}
	var config struct {
		Servers map[string]struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
			Type    string   `json:"type,omitempty"`
		} `json:"mcpServers"`
	}
	if e = decode(b, &config); e != nil || len(config.Servers) == 0 {
		return nil, fail(exitMockRefused, "mock requires an exclusive stdio mcpServers descriptor")
	}
	for _, server := range config.Servers {
		if !filepath.IsAbs(server.Command) || server.Type != "" && server.Type != "stdio" {
			return nil, fail(exitMockRefused, "mock accepts absolute stdio commands only")
		}
	}
	for p := s.CWD; ; p = filepath.Dir(p) {
		for _, part := range []string{".codex/config.toml", ".mcp.json"} {
			if data, e := os.ReadFile(filepath.Join(p, part)); e == nil && strings.Contains(string(data), "mcp") {
				return nil, fail(exitMockRefused, "project MCP configuration conflicts with isolated mock")
			}
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	// Persist the descriptor privately for the pane, avoiding a mutable reference
	// to the input file. The per-job configuration is an artifact, not a ledger.
	dir := filepath.Join(a.Jobs, s.Job, "mock")
	if e = os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	if seat.Kind == "claude" {
		p := filepath.Join(dir, "mcp.json")
		if e = os.WriteFile(p, b, 0600); e != nil {
			return nil, e
		}
		return []string{"PANEWIRE_MOCK_CONFIG=" + p}, nil
	}
	var text strings.Builder
	for _, name := range slices.Sorted(maps.Keys(config.Servers)) {
		server := config.Servers[name]
		n, _ := json.Marshal(name)
		cmd, _ := json.Marshal(server.Command)
		args, _ := json.Marshal(server.Args)
		text.WriteString("[mcp_servers." + string(n) + "]\ncommand = " + string(cmd) + "\nargs = " + string(args) + "\n")
	}
	if e = os.WriteFile(filepath.Join(dir, "config.toml"), []byte(text.String()), 0600); e != nil {
		return nil, e
	}
	return []string{"CODEX_HOME=" + dir}, nil
}
