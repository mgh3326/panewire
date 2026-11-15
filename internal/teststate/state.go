// Package teststate owns the temporary directory a package's TestMain scopes a
// test run's state to, and keeps such directories from piling up in the system
// temp dir when a test binary dies before its cleanup.
//
// A binary killed by -timeout, SIGTERM or a reaped pane never reaches the
// RemoveAll at the end of TestMain, so a crashed run leaves its whole state
// tree behind. SweepStale removes those leftovers on the next run, and the
// helpers here make the removal work on the read-only trees a Go module cache
// leaves (hk 4733).
package teststate

import "time"

// StatePrefix names the state directory a test run creates under the system
// temp dir. SweepStale only ever touches directories with this prefix.
const StatePrefix = "panewire-test-state-"

// StaleAfter is how old a leftover state directory must be before a later run
// removes it. It is longer than any local test run, so a run still in flight is
// never touched, and short enough that a leak does not survive the day.
const StaleAfter = 6 * time.Hour
