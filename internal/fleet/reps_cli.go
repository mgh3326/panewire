package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const repsUsage = `usage: panewire reps add|list|compare|pending ...

  add      --profile P --model M --task T --tier Tn --role ROLE --grade G
           [--effort E] [--rounds N] [--blockers-found N] [--completed 0|1]
           [--notes TEXT] [--first-ci-green BOOL] [--reverted-within-7d BOOL]
           [--findings-by-severity JSON] [--time-to-merge-s N]
           [--experiment-id ID] [--experiment-arm baseline|downshift]
           [--input-tokens N] [--output-tokens N] [--dry-run] [--json]
  list     [--profile P] [--grade G] [--effort E] [--area A] [--limit N] [--json]
  compare  [--profile P] [--grade G] [--effort E] [--area A] [--limit N]
  pending  list | retry origin:N | drop origin:N`

var repsValueFlags = map[string]bool{
	"profile": true, "model": true, "effort": true, "task": true,
	"tier": true, "role": true, "grade": true, "rounds": true,
	"blockers-found": true, "completed": true, "notes": true,
	"first-ci-green": true, "reverted-within-7d": true,
	"findings-by-severity": true, "time-to-merge-s": true,
	"experiment-id": true, "experiment-arm": true,
	"input-tokens": true, "output-tokens": true,
	"area": true, "limit": true,
}

var repsBoolFlags = map[string]bool{"dry-run": true, "json": true}

// RepsMain is `panewire reps`. It writes to the same hk bench store pacelever
// uses and keeps pacelever's flags and receipt lines.
func RepsMain(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(errOut, repsUsage)
		return exitUsage
	}
	a, err := New(out, errOut)
	if err != nil {
		fmt.Fprintln(errOut, "panewire:", err)
		return featureExitCode(err)
	}
	if err := a.repsCommand(ctx, args, out, errOut); err != nil {
		fmt.Fprintln(errOut, "panewire:", err)
		return featureExitCode(err)
	}
	return 0
}

func featureExitCode(err error) int {
	var x *ExitError
	if errors.As(err, &x) {
		return x.Code
	}
	return 1
}

func (a *App) repsCommand(ctx context.Context, args []string, out, errOut io.Writer) error {
	switch args[0] {
	case "add":
		return a.repsAdd(ctx, args[1:], out)
	case "list", "compare":
		return a.repsList(ctx, args[0], args[1:], out, errOut)
	case "pending":
		return a.repsPending(ctx, args[1:], out)
	}
	return fail(exitUsage, "%s", repsUsage)
}

func (a *App) repsAdd(ctx context.Context, args []string, out io.Writer) error {
	values, bools, positional, err := parseFeatureFlags(args, repsValueFlags, repsBoolFlags)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	if len(positional) != 0 {
		return fail(exitUsage, "reps add takes flags only")
	}
	for _, key := range []string{"profile", "model", "task", "tier", "role", "grade"} {
		if values[key] == "" {
			return fail(exitUsage, "reps add requires --%s", key)
		}
	}
	grade := values["grade"]
	if !validRepGrade(grade) || grade == "unrated" {
		return fail(exitUsage, "invalid rep grade")
	}
	rounds, err := intFlag(values, "rounds", 0)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	blockers, err := intFlag(values, "blockers-found", 0)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	completed, err := intFlag(values, "completed", -1)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	r := Rep{
		Profile: values["profile"], ModelID: values["model"], TaskRef: values["task"],
		Tier: values["tier"], Role: values["role"], Grade: grade,
		Rounds: rounds, BlockersFound: blockers, Effort: values["effort"], Notes: values["notes"],
	}
	if values["completed"] != "" {
		r.Completed = &completed
	}
	if err := outcomeFlags(values, &r); err != nil {
		return fail(exitUsage, "%v", err)
	}
	if err := hydrateRepOutcomes(&r); err != nil {
		return fail(exitUsage, "%v", err)
	}
	if !hasRepOutcomeFields(r) {
		for _, key := range []string{"rounds", "blockers-found", "completed"} {
			if values[key] == "" {
				return fail(exitUsage, "reps add requires --%s when no outcome field is given", key)
			}
		}
	}
	for _, key := range []string{"input-tokens", "output-tokens"} {
		if values[key] == "" {
			continue
		}
		n, err := strconv.ParseInt(values[key], 10, 64)
		if err != nil || n < 0 {
			return fail(exitUsage, "invalid token count")
		}
		if key == "input-tokens" {
			r.InputTokens = &n
		} else {
			r.OutputTokens = &n
		}
	}
	if catalog, err := a.benchCatalog(ctx); err == nil {
		profile, effort := resolveRepRung(catalog, r)
		r.TableGrade = repTableGrade(catalog, profile, effort)
	}
	if bools["dry-run"] {
		return writeJSON(out, r)
	}
	store, err := a.benchStore()
	if err != nil {
		return fail(exitConfig, "%v", err)
	}
	record, err := store.AddRep(ctx, r)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	if bools["json"] {
		return writeJSON(out, map[string]any{"ref": repRef(record), "rep": record})
	}
	fmt.Fprintf(out, "recorded rep id=%s\n", repRef(record))
	return nil
}

func outcomeFlags(values map[string]string, r *Rep) error {
	for _, key := range []string{"first-ci-green", "reverted-within-7d"} {
		value := values[key]
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid --%s", key)
		}
		if key == "first-ci-green" {
			r.FirstCIGreen = &parsed
		} else {
			r.RevertedWithin7d = &parsed
		}
	}
	if value := values["findings-by-severity"]; value != "" {
		var findings map[string]int
		if json.Unmarshal([]byte(value), &findings) != nil || findings == nil {
			return fmt.Errorf("invalid --findings-by-severity")
		}
		r.FindingsBySeverity = findings
	}
	if value := values["time-to-merge-s"]; value != "" {
		n, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid --time-to-merge-s")
		}
		r.TimeToMergeS = &n
	}
	r.ExperimentID = values["experiment-id"]
	r.ExperimentArm = values["experiment-arm"]
	return nil
}

func (a *App) repsList(ctx context.Context, command string, args []string, out, errOut io.Writer) error {
	values, bools, positional, err := parseFeatureFlags(args, repsValueFlags, repsBoolFlags)
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	if len(positional) != 0 {
		return fail(exitUsage, "reps %s takes flags only", command)
	}
	store, err := a.benchStore()
	if err != nil {
		return fail(exitConfig, "%v", err)
	}
	rows, source, err := store.ReadReps(ctx, values["profile"])
	if err != nil {
		return fail(exitUsage, "%v", err)
	}
	var areas map[string]string
	if values["area"] != "" {
		if areas, err = store.ReadTaskAreas(ctx, rows); err != nil {
			return fail(exitUsage, "%v", err)
		}
	}
	filtered := make([]Rep, 0, len(rows))
	for _, r := range rows {
		if values["area"] != "" && areas[r.TaskRef] != values["area"] {
			continue
		}
		if values["grade"] != "" && values["grade"] != r.Grade {
			continue
		}
		if values["effort"] != "" && values["effort"] != r.Effort {
			continue
		}
		filtered = append(filtered, r)
	}
	if command == "compare" {
		stats := map[string]map[string]any{}
		for _, r := range filtered {
			key := r.Profile + "@" + r.Effort
			if stats[key] == nil {
				stats[key] = map[string]any{"count": 0, "completed": 0, "rounds": 0, "blockers": 0}
			}
			s := stats[key]
			s["count"] = s["count"].(int) + 1
			s["rounds"] = s["rounds"].(int) + r.Rounds
			s["blockers"] = s["blockers"].(int) + r.BlockersFound
			if r.Completed != nil && *r.Completed == 1 {
				s["completed"] = s["completed"].(int) + 1
			}
		}
		return writeJSON(out, map[string]any{"profiles": stats, "source": source})
	}
	limit, err := intFlag(values, "limit", len(filtered))
	if err != nil || limit < 0 {
		return fail(exitUsage, "invalid rep limit")
	}
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	if bools["json"] {
		return writeJSON(out, map[string]any{"reps": filtered, "source": source})
	}
	for _, r := range filtered {
		completed := "-"
		if r.Completed != nil {
			completed = strconv.Itoa(*r.Completed)
		}
		notes := r.Notes
		var obj map[string]any
		if json.Unmarshal([]byte(notes), &obj) == nil && obj != nil {
			data, _ := json.Marshal(obj)
			notes = string(data)
		} else {
			data, _ := json.Marshal(notes)
			notes = string(data)
		}
		pending := ""
		if r.Pending {
			pending = " pending=true"
		}
		fmt.Fprintf(out, "id=%s profile=%s model=%s effort=%s task=%s tier=%s role=%s grade=%s rounds=%d blockers_found=%d completed=%s recorded_at=%s%s notes=%s\n",
			repRef(r), r.Profile, r.ModelID, r.Effort, r.TaskRef, r.Tier, r.Role, r.Grade, r.Rounds, r.BlockersFound, completed, r.RecordedAt, pending, notes)
	}
	if strings.Contains(source, "truncated") {
		fmt.Fprintln(errOut, "warning: rep window is truncated")
	}
	return nil
}

func (a *App) repsPending(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fail(exitUsage, "reps pending requires list, retry or drop")
	}
	store, err := a.benchStore()
	if err != nil {
		return fail(exitConfig, "%v", err)
	}
	switch args[0] {
	case "list":
		if len(args) != 1 {
			return fail(exitUsage, "reps pending list takes no arguments")
		}
		pending, err := store.PendingReps()
		if err != nil {
			return fail(exitUsage, "%v", err)
		}
		return writeJSON(out, pending)
	case "retry", "drop":
		if len(args) != 2 {
			return fail(exitUsage, "reps pending %s needs origin:N", args[0])
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(args[1], "origin:"), 10, 64)
		if err != nil {
			return fail(exitUsage, "pending origin id required")
		}
		if args[0] == "drop" {
			if err := store.DropPendingRep(id); err != nil {
				return fail(exitUsage, "%v", err)
			}
			return nil
		}
		pending, err := store.PendingReps()
		if err != nil {
			return fail(exitUsage, "%v", err)
		}
		for _, r := range pending {
			if r.OriginID == id {
				record, err := store.retryRep(ctx, r)
				if err != nil {
					return fail(exitUsage, "%v", err)
				}
				return writeJSON(out, record)
			}
		}
		return fail(exitUsage, "pending origin not found")
	}
	return fail(exitUsage, "reps pending requires list, retry or drop")
}

func intFlag(values map[string]string, name string, def int) (int, error) {
	raw := values[name]
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s", name)
	}
	return n, nil
}

// parseFeatureFlags accepts --name value, --name=value, boolean flags and
// positionals in any order, so flags may follow the pool or job positional as
// pacelever's args helper allows.
func parseFeatureFlags(args []string, valueFlags, boolFlags map[string]bool) (map[string]string, map[string]bool, []string, error) {
	values := map[string]string{}
	bools := map[string]bool{}
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "--") || arg == "--" {
			positional = append(positional, arg)
			continue
		}
		name := strings.TrimPrefix(arg, "--")
		value := ""
		if key, v, ok := strings.Cut(name, "="); ok {
			name, value = key, v
		}
		if boolFlags[name] {
			if value == "" {
				bools[name] = true
			} else {
				parsed, err := strconv.ParseBool(value)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("invalid --%s", name)
				}
				bools[name] = parsed
			}
			continue
		}
		if !valueFlags[name] {
			return nil, nil, nil, fmt.Errorf("unknown option --%s", name)
		}
		if value == "" {
			if i+1 >= len(args) {
				return nil, nil, nil, fmt.Errorf("--%s requires a value", name)
			}
			i++
			value = args[i]
		}
		values[name] = value
	}
	return values, bools, positional, nil
}
