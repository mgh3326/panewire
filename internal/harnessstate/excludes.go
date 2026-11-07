// Package harnessstate defines the Git patterns for harness-owned state.
package harnessstate

// Excludes lists Git exclude patterns, one per line. Add another harness by
// adding its pattern as a separate line.
const Excludes = ".commandcode/\n"
