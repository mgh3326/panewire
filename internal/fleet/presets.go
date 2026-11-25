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
	"time"
)

func (a *App) workPreset(ctx context.Context, args []string) (Spawn, error) {
	if len(args) == 0 || !regexp.MustCompile(`^[0-9]+$`).MatchString(args[0]) {
		return Spawn{}, fail(exitUsage, "use panewire work TASK_ID followed by optional spawn flags")
	}
	id := args[0]
	// The task comes over hk HTTP with the svc-panewire credential (hk 3607)
	// and is read as `handoffkeep tasks show` output was: it must name this
	// id. refs.worktree, refs.cwd and refs.brief_path are not task refs hk
	// keeps, so those fallbacks never fire; they stay for an answer that
	// carries them.
	b, err := hkTaskBody(ctx, id)
	var task Task
	if err != nil || json.Unmarshal(b, &task) != nil || scalar(task.ID) != id {
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
	defaults := []string{"-c", cwd, "-m", "auto", "-p", firstNonempty(str(task.Refs, "brief_path"), filepath.Join(a.Jobs, job, "brief.md")), "-w", "workers", "-l", job, "--job", job, "--t", strings.ToUpper(marker[2]), "--task", id, "--host", "auto", "--report", firstNonempty(str(task.Refs, "report_path"), filepath.Join(a.Jobs, job, "report.md"))}
	defaults = append(defaults, "--role", "implementer")
	s, err := parseSpawn(append(defaults, args[1:]...))
	if err != nil {
		return s, err
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

// excludedHostNames reads --exclude-host and names each entry by the hosts
// keys placement compares its candidates against (placement.go plan, the
// selection in select.go). An alias or ssh name stands for its configured
// host, as it does for --host in resolveHost; an exclusion never refuses an
// ambiguous name, so an alias that names two hosts excludes both.
func (a *App) excludedHostNames(raw string) ([]string, error) {
	names, err := parseHosts(raw)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		out = append(out, a.hostKeysNamed(name)...)
	}
	return out, nil
}

// hostKeysNamed is the hosts key(s) a name stands for: the name itself when it
// is a configured key or the local machine id, else every host whose aliases
// or ssh name it, else the name as written (no configured host it could mean).
func (a *App) hostKeysNamed(name string) []string {
	if _, ok := a.Config.Hosts[name]; ok || name == a.machine() {
		return []string{name}
	}
	var keys []string
	for id, host := range a.Config.Hosts {
		if slices.Contains(stringList(host["aliases"]), name) || str(host, "ssh") == name {
			keys = append(keys, id)
		}
	}
	if slices.Contains(stringList(a.Config.Local["aliases"]), name) {
		keys = append(keys, a.machine())
	}
	if len(keys) == 0 {
		return []string{name}
	}
	slices.Sort(keys)
	return keys
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

var herdrWorkspaceID = regexp.MustCompile(`^w[A-Z0-9]+$`)

// workspaceRows lists the executing host's workspaces in herdr's order.
func (a *App) workspaceRows(ctx context.Context, value string) ([]map[string]any, error) {
	r := a.call(ctx, "herdr", "workspace", "list")
	var raw map[string]any
	if r.Code != 0 || json.Unmarshal([]byte(r.Out), &raw) != nil {
		return nil, fail(exitConfig, "cannot resolve workspace %q; run herdr workspace list on the executing host and pass -w WORKSPACE_ID", value)
	}
	raw = unwrap(raw)
	items, ok := raw["workspaces"].([]any)
	if !ok {
		return nil, fail(exitConfig, "workspace inventory missing; run herdr workspace list on the executing host and pass -w WORKSPACE_ID")
	}
	var rows []map[string]any
	for _, item := range items {
		if row, ok := item.(map[string]any); ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// createdWorkspace is a workspace resolveWorkspace created. The tab and its root
// pane are ours: a failed spawn removes that tab and nothing else, so a
// workspace another spawn adopted keeps its own tab and live session.
type createdWorkspace struct {
	workspace string
	tab       string
	rootPane  string
}

// resolveWorkspace holds the (herdr session, label) workspace locks
// (lockWorkspace) across the list-then-create decision, so neither concurrent
// node spawns (hk 4578, one process) nor a node spawn and a local or SSH
// launch (separate processes) can each find the label absent and each create a
// workspace. The wait ends with the spawn's own context.
func (a *App) resolveWorkspace(ctx context.Context, value string) (string, *createdWorkspace, error) {
	release, err := a.lockWorkspace(ctx, value)
	if err != nil {
		return "", nil, err
	}
	defer release()
	return a.resolveWorkspaceLocked(ctx, value)
}

func (a *App) resolveWorkspaceLocked(ctx context.Context, value string) (string, *createdWorkspace, error) {
	rows, err := a.workspaceRows(ctx, value)
	if err != nil {
		// Herdr ids are unambiguous; keep them usable when listing is unavailable.
		if herdrWorkspaceID.MatchString(value) {
			return value, nil, nil
		}
		return "", nil, err
	}
	var matches []string
	for _, row := range rows {
		id := str(row, "workspace_id")
		if id == value {
			return id, nil, nil
		}
		if str(row, "label") == value && id != "" {
			matches = append(matches, id)
		}
	}
	if herdrWorkspaceID.MatchString(value) {
		return "", nil, fail(exitUsage, "workspace id %q is not in herdr's list; herdr deletes an empty workspace, so configure its label instead: workspace = \"workers\", not the id", value)
	}
	switch len(matches) {
	case 0:
		return a.createWorkspace(ctx, value)
	case 1:
		fmt.Fprintf(a.Out, "workspace_label=%s workspace=%s\n", value, matches[0])
		return matches[0], nil, nil
	}
	return "", nil, ambiguousWorkspace(value, len(matches))
}

// ambiguousWorkspace refuses a label that does not name exactly one workspace
// panewire can use: it never picks one.
func ambiguousWorkspace(label string, matches int) error {
	return fail(exitUsage, "workspace %q matched %d entries; run herdr workspace list on the executing host and pass -w WORKSPACE_ID", label, matches)
}

// labelledWorkspaces is the ids of the rows that carry label, in herdr's order.
func labelledWorkspaces(rows []map[string]any, label string) []string {
	var ids []string
	for _, row := range rows {
		if id := str(row, "workspace_id"); str(row, "label") == label && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// createWorkspace recreates a workspace whose label matched none. Herdr deletes
// a workspace when its last pane closes, and the spawn path is the only place
// that sees every such case. The workspace locks serialize panewire's own
// writers, but a hand-run herdr or an older binary under another HOME can still
// create the label meanwhile (settleDuplicateWorkspace).
func (a *App) createWorkspace(ctx context.Context, label string) (string, *createdWorkspace, error) {
	r := a.call(ctx, "herdr", "workspace", "create", "--label", label)
	var created struct {
		Result struct {
			Workspace struct {
				WorkspaceID string `json:"workspace_id"`
			} `json:"workspace"`
			Tab struct {
				TabID string `json:"tab_id"`
			} `json:"tab"`
			RootPane struct {
				PaneID string `json:"pane_id"`
			} `json:"root_pane"`
		} `json:"result"`
	}
	if r.Code != 0 || json.Unmarshal([]byte(r.Out), &created) != nil || created.Result.Workspace.WorkspaceID == "" {
		return "", nil, fail(exitConfig, "cannot create workspace %q; run herdr workspace create --label %s on the executing host", label, label)
	}
	own := &createdWorkspace{workspace: created.Result.Workspace.WorkspaceID, tab: created.Result.Tab.TabID, rootPane: created.Result.RootPane.PaneID}
	workspace := own.workspace
	if rows, err := a.workspaceRows(ctx, label); err == nil {
		switch matches := labelledWorkspaces(rows, label); {
		case len(matches) > 1:
			return a.settleDuplicateWorkspace(ctx, label, own)
		case len(matches) == 1:
			workspace = matches[0]
		}
	}
	fmt.Fprintf(a.Out, "workspace_created label=%s workspace=%s\n", label, workspace)
	return workspace, own, nil
}

// settleDuplicateWorkspace answers a create whose label more than one workspace
// now carries. It removes only the tab this call created, lists once more, and
// adopts the label's workspace when exactly one remains; otherwise it refuses.
// The created tab is reported to the caller only while it may still exist: when
// its close failed and the spawn goes on, a later failure closes it again.
func (a *App) settleDuplicateWorkspace(ctx context.Context, label string, own *createdWorkspace) (string, *createdWorkspace, error) {
	// A spawn whose context just ended still removes the tab it made.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	left := own
	if err := a.closeCreatedWorkspace(cleanup, own); err != nil {
		a.warn("created workspace cleanup failed; close tab %s (workspace %s) on the executing host: %v", own.tab, own.workspace, err)
	} else {
		left = nil
	}
	rows, err := a.workspaceRows(ctx, label)
	if err != nil {
		return "", nil, err
	}
	matches := labelledWorkspaces(rows, label)
	if len(matches) != 1 {
		return "", nil, ambiguousWorkspace(label, len(matches))
	}
	fmt.Fprintf(a.Out, "workspace_label=%s workspace=%s after_duplicate_create=%s\n", label, matches[0], own.workspace)
	return matches[0], left, nil
}

// closeCreatedWorkspace removes the tab this spawn created and never the
// workspace. Herdr closes a workspace when its last tab closes, so an unused
// created workspace still disappears, while a workspace another spawn adopted
// keeps that spawn's tab and live session. A tab that is already gone was
// cleaned up by someone else, so that refusal is not an error.
func (a *App) closeCreatedWorkspace(ctx context.Context, w *createdWorkspace) error {
	if w.tab == "" {
		// A response without a tab id can still name the root pane, which herdr
		// scopes to that tab and the workspace only when it was the last one.
		if w.rootPane == "" {
			return fail(exitConfig, "created workspace %s names no tab or pane; run herdr workspace close %s on the executing host", w.workspace, w.workspace)
		}
		return a.closeCreatedPane(ctx, w.rootPane)
	}
	r := a.call(ctx, "herdr", "tab", "close", w.tab)
	if r.Code == 0 || strings.Contains(strings.ToLower(r.Out+r.Err), "not_found") {
		return nil
	}
	// Herdr before 0.8.0 refuses a workspace's last tab. Closing the root pane is
	// the same atomic step there: herdr removes that tab, and the workspace only
	// when it was the last one, so another spawn's tab still survives.
	if strings.Contains(r.Out+r.Err, "cannot close the last tab") && w.rootPane != "" {
		return a.closeCreatedPane(ctx, w.rootPane)
	}
	return check(r, "herdr tab close")
}

func (a *App) closeCreatedPane(ctx context.Context, pane string) error {
	r := a.call(ctx, "herdr", "pane", "close", pane)
	if r.Code == 0 || strings.Contains(strings.ToLower(r.Out+r.Err), "not_found") {
		return nil
	}
	return check(r, "herdr pane close")
}

// workspaceGone reports a herdr refusal caused by the target workspace no longer
// existing. A spawn that adopted one can recreate it and retry the tab.
func workspaceGone(r Result) bool {
	text := strings.ToLower(r.Out + " " + r.Err)
	for _, marker := range []string{"workspace_not_found", "workspace not found", "workspace disappeared", "target workspace disappeared"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
