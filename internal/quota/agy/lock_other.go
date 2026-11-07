//go:build !unix

package agy

// withFileLock has no advisory lock off unix; the wake runs unserialised.
func withFileLock(_ string, fn func() error) error { return fn() }
