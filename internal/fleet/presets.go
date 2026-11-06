package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

func (a *App) workPreset(ctx context.Context, args []string) (Spawn, error) {
	if len(args) == 0 || !regexp.MustCompile(`^[0-9]+$`).MatchString(args[0]) {
		return Spawn{}, fail(exitUsage, "use panewire work TASK_ID followed by optional spawn flags")
	}
	id := args[0]
	r := a.call(ctx, "handoffkeep", "tasks", "show", id)
	var task Task
	if r.Code != 0 || json.Unmarshal([]byte(r.Out), &task) != nil || scalar(task.ID) != id {
		return Spawn{}, fail(exitConfig, "cannot read task %s; run handoffkeep tasks show %s and restore task access", id, id)
	}
	marker := taskMarker.FindStringSubmatch(task.Title)
	if marker == nil {
		return Spawn{}, fail(exitSeatRefused, "task %s needs a (grade X, Tn) title marker; update the task title before panewire work", id)
	}
	job := firstNonempty(str(task.Refs, "job_id"), "task"+id+"-work")
	cwd, err := os.Getwd()
	if err != nil {
		return Spawn{}, err
	}
	if ref, ok := task.Refs["worktree"].(map[string]any); ok {
		cwd = firstNonempty(str(ref, "path"), cwd)
	} else {
		cwd = firstNonempty(str(task.Refs, "worktree"), str(task.Refs, "cwd"), cwd)
	}
	caller := firstNonempty(os.Getenv("PANEWIRE_LANE"), str(a.Config.Local, "lane"))
	if caller == "" && os.Getenv("ARBITER_JOB") != "" {
		j, e := a.resolveParent(ctx, os.Getenv("ARBITER_JOB"))
		if e != nil {
			return Spawn{}, fail(exitStateConflict, "caller job metadata unavailable; set PANEWIRE_LANE or pass --owner/--parent: %v", e)
		}
		caller = firstNonempty(j.Label, j.Owner)
	}
	defaults := []string{"-c", cwd, "-m", "auto", "-p", firstNonempty(str(task.Refs, "brief_path"), filepath.Join(a.Jobs, job, "brief.md")), "-w", "workers", "-l", job, "--job", job, "--t", strings.ToUpper(marker[2]), "--task", id, "--host", "auto", "--report", firstNonempty(str(task.Refs, "report_path"), filepath.Join(a.Jobs, job, "report.md"))}
	defaults = append(defaults, "--role", "worker", "--owner", caller)
	s, err := parseSpawn(append(defaults, args[1:]...))
	if err != nil {
		return s, err
	}
	if s.Owner == "" {
		return s, fail(exitUsage, "caller lane unknown; set PANEWIRE_LANE or pass --owner LANE")
	}
	return s, nil
}

// Canonical host keys are hub machine ids. Aliases are configuration data,
// never a built-in mapping to a particular fleet.
func (a *App) resolveHost(name string) (string, error) {
	if name == "" || name == "auto" || name == "local" || name == a.machine() {
		return name, nil
	}
	if _, ok := a.Config.Hosts[name]; ok {
		return name, nil
	}
	matches := []string{}
	for id, host := range a.Config.Hosts {
		aliases := stringList(host["aliases"])
		if slices.Contains(aliases, name) || str(host, "ssh") == name {
			matches = append(matches, id)
		}
	}
	if slices.Contains(stringList(a.Config.Local["aliases"]), name) {
		matches = append(matches, "local")
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return "", fail(exitUsage, "host alias %q is ambiguous; pass the canonical hosts.<machine> key", name)
	}
	return "", fail(exitUsage, "unknown host %q; add it to hosts.<machine>.aliases or pass a configured machine id (panewire hosts)", name)
}

func stringList(value any) []string {
	if names, ok := value.([]string); ok {
		return names
	}
	var names []string
	if values, ok := value.([]any); ok {
		for _, v := range values {
			if name, ok := v.(string); ok {
				names = append(names, name)
			}
		}
	}
	return names
}

func (a *App) resolveWorkspace(ctx context.Context, value string) (string, error) {
	// Herdr ids are already unambiguous and remain usable when listing is unavailable.
	if regexp.MustCompile(`^w[A-Z0-9]+$`).MatchString(value) {
		return value, nil
	}
	r := a.call(ctx, "herdr", "workspace", "list")
	var raw map[string]any
	if r.Code != 0 || json.Unmarshal([]byte(r.Out), &raw) != nil {
		return "", fail(exitConfig, "cannot resolve workspace %q; run herdr workspace list on the executing host and pass -w WORKSPACE_ID", value)
	}
	raw = unwrap(raw)
	rows, ok := raw["workspaces"].([]any)
	if !ok {
		return "", fail(exitConfig, "workspace inventory missing; run herdr workspace list on the executing host and pass -w WORKSPACE_ID")
	}
	var matches []string
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := str(row, "workspace_id")
		if id == value {
			return id, nil
		}
		if str(row, "label") == value && id != "" {
			matches = append(matches, id)
		}
	}
	if len(matches) == 1 {
		fmt.Fprintf(a.Out, "workspace_label=%s workspace=%s\n", value, matches[0])
		return matches[0], nil
	}
	return "", fail(exitUsage, "workspace %q matched %d entries; run herdr workspace list on the executing host and pass -w WORKSPACE_ID", value, len(matches))
}
