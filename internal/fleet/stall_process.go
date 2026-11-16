package fleet

import (
	"context"
	"fmt"
)

// StallForegroundBusy reports a foreground command other than the agent or
// the pane's shell. An unavailable or empty process listing is not evidence.
func (a *App) StallForegroundBusy(ctx context.Context, pane, harness string) (bool, error) {
	p, err := a.pinfo(ctx, pane)
	if err != nil {
		return false, err
	}
	if p.ShellPID <= 0 || len(p.Processes) == 0 || harness == "" {
		return false, fmt.Errorf("incomplete pane process information")
	}
	for _, process := range p.Processes {
		if process.PID <= 0 {
			return false, fmt.Errorf("incomplete foreground process information")
		}
		if process.PID != p.ShellPID && process.Argv0 == "" && process.Name == "" && len(process.Argv) == 0 {
			return false, fmt.Errorf("missing foreground process identity")
		}
		if process.PID != p.ShellPID && !foregroundAgent(harness, process.Argv0, process.Name, process.Argv) {
			return true, nil
		}
	}
	return false, nil
}
