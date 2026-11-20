package fleet

// agySSHEnv are the variables that make agy keep its login in a file token
// store instead of the OS keyring (hk 5699). A herdr server started from an
// SSH session carries them and every pane inherits them, so an agy pane finds
// no file token and opens on the login screen.
var agySSHEnv = []string{"SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"}

// paneUnsetCommand returns the shell command that removes the SSH variables
// from a pane's shell before the pane starts its agent, or nil for a harness
// that keeps them. The pane's shell is the place to remove them: `herdr tab
// create --env` can only add variables, and `herdr agent start` runs the kind's
// own executable, so an argv prefix would become that executable's arguments.
// The agent inherits its shell's environment. See docs/config.md#agy-ssh-variables.
//
// Only agy is scoped in. claude reads the same variables to detect an SSH
// session (browser open, multiplexer and login choices), so removing them there
// would change claude's behavior, which is not part of this fix.
func paneUnsetCommand(kind string) []string {
	if kind != "agy" {
		return nil
	}
	return append([]string{"unset"}, agySSHEnv...)
}
