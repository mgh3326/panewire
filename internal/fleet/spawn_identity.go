package fleet

// SpawnIdentity is what a spawn's argv names, read as the launcher reads it.
// A hub job.spawn is read through it by everything that has to name the job
// before, instead of or after the launcher: the overlap key, the node journal,
// the node's and hub's not-started and timeout answers and the hub's
// landed-pane check (docs/hub-spawn-operations.md, "Spawn identity").
type SpawnIdentity struct {
	// Job is the effective job id: --job, else Label.
	Job string
	// Label is the -l value.
	Label string
	// Task is the --task value, for checks that match a pane against the
	// task the spawn was placed for.
	Task string
	// Dir is the -c value. A node launches in the directory it prepared or
	// mapped instead (bindNodeSpawn), so a node reader takes its directory
	// from the checkout spec, not from here.
	Dir string
	// ExecutionLane is the -L value ("live", "mock" or ""), not --lane.
	ExecutionLane string
}

// ParseSpawnIdentity reads the names in args with the launcher's own flag
// grammar: a repeated flag keeps its last value, and a flag's value is never
// read as a flag. It checks no value, so an argv the launcher later refuses
// for one (a role, a review pass) still names the job that refusal is about.
// An argv the grammar cannot read names nothing: the launcher refuses it
// before it claims or launches anything.
func ParseSpawnIdentity(args []string) (SpawnIdentity, error) {
	return ParseSpawnIdentityIgnoring(args, nil)
}

// ParseSpawnIdentityIgnoring reads identity from stored argv while accepting
// named retired flags as one-value no-ops. Unknown flags remain errors.
func ParseSpawnIdentityIgnoring(args, ignored []string) (SpawnIdentity, error) {
	s, _, f, err := readSpawnFlags(args, ignored)
	if err != nil {
		return SpawnIdentity{}, err
	}
	if f.NArg() != 0 {
		return SpawnIdentity{}, fail(exitUsage, "unexpected spawn arguments")
	}
	s.defaultJob()
	return SpawnIdentity{Job: s.Job, Label: s.Label, Task: s.Task, Dir: s.CWD, ExecutionLane: s.ExecutionLane}, nil
}
