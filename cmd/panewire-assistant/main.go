package main

import (
	"os"

	"github.com/mgh3326/panewire"
)

// panewire-assistant is the thin, stateless MCP surface the external
// assistant (berry) reads handoffkeep decision state through (MGH-36 PR-2).
// Read tools only; it holds no durable state and can be killed and restarted
// at any time.
func main() { os.Exit(panewire.RunAssistantServer(os.Args[1:], os.Stderr)) }
