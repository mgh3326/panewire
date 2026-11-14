//go:build !darwin && !linux

package panewire

// nodeProcessGroupAlive cannot name a process group on this host, so an add
// records no usable identity and recovery never force-removes a path.
func nodeProcessGroupAlive(int) (alive, known bool) {
	return false, false
}
