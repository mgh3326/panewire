package fleet

import (
	"context"
	"maps"
	"slices"
	"strings"
)

// workspaceDuplicate is the sweep's reason for a workspace label that more than
// one herdr workspace carries.
const workspaceDuplicate = "workspace_duplicate"

// reportWorkspaceDuplicates logs, on every pass, each label of the node's herdr
// session that more than one workspace carries: a spawn that names it refuses
// until one is left (docs/pane-sweep.md, "Duplicate workspaces"). It only
// reports; it closes and renames nothing. A herdr that cannot be listed gets no
// report on this pass.
func (s *TaskPaneSweeper) reportWorkspaceDuplicates(ctx context.Context, a *App) {
	rows, err := a.workspaceRows(ctx, "")
	if err != nil {
		return
	}
	labels := map[string]bool{}
	for _, row := range rows {
		if label := str(row, "label"); label != "" {
			labels[label] = true
		}
	}
	for _, label := range slices.Sorted(maps.Keys(labels)) {
		if ids := labelledWorkspaces(rows, label); len(ids) > 1 {
			s.info("workspace label duplicated", "reason", workspaceDuplicate, "label", label, "workspaces", strings.Join(ids, ","))
		}
	}
}
