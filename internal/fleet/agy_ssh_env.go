package fleet

import (
	"fmt"
	"path/filepath"
	"strings"
)

// agySSHEnv are the variables that make agy keep its login in a file token
// store instead of the OS keyring (hk 5699). A herdr server started from an
// SSH session carries them and every pane inherits them, so an agy pane finds
// no file token and opens on the login screen.
var agySSHEnv = []string{"SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"}

// paneReadyPrefix returns the command typed into a pane's shell before the
// readiness token, which spawn appends as the last argument. For agy it removes
// the SSH variables and then echoes the token in the same submitted line, so the
// token appears only when the removal ran (a line a startup file swallows prints
// nothing). The command is chosen from the pane's shell: POSIX shells use
// unset, fish uses set -e and set -q. Other kinds echo the token alone.
// docs/config.md#agy-ssh-variables has the contract.
//
// The removal is not in the spawn's environment: herdr tab create --env can only
// set variables, and herdr agent start runs the kind's own executable, so an argv
// prefix would become that executable's arguments. The agent inherits the
// shell's environment.
//
// Only agy is scoped in. claude reads these variables to detect an SSH session
// (browser open, multiplexer and login choices), so removing them there would
// change its behavior.
func paneReadyPrefix(kind, shell string) (string, error) {
	if kind != "agy" {
		return "echo", nil
	}
	switch shell {
	case "sh", "bash", "dash", "zsh", "ksh", "mksh":
		return "unset " + strings.Join(agySSHEnv, " ") + ` && [ -z "${SSH_CONNECTION+x}${SSH_CLIENT+x}${SSH_TTY+x}" ] && echo`, nil
	case "fish":
		var b strings.Builder
		for _, k := range agySSHEnv {
			b.WriteString("set -e " + k + "; ")
		}
		for _, k := range agySSHEnv {
			b.WriteString("and not set -q " + k + "; ")
		}
		b.WriteString("and echo")
		return b.String(), nil
	}
	return "", fmt.Errorf("agy pane refused: pane shell %q is not one the SSH variable removal supports (sh, bash, dash, zsh, ksh, mksh, fish)", shell)
}

// paneShellName returns the executable name of a pane's shell from its process
// info, without a leading dash or a directory, or "" when herdr reports none.
func paneShellName(p processInfo) string {
	for _, proc := range p.Processes {
		if proc.PID != p.ShellPID {
			continue
		}
		name := proc.Name
		if name == "" {
			name = proc.Argv0
		}
		if name == "" {
			return ""
		}
		return strings.TrimPrefix(filepath.Base(name), "-")
	}
	return ""
}
