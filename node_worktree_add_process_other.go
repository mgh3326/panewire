//go:build !darwin && !linux

package panewire

// nodeReadAddProcess cannot identify a process on this host, so an add never
// records a child identity and recovery never force-removes a path.
func nodeReadAddProcess(int) (nodeAddProcess, bool) {
	return nodeAddProcess{}, false
}
