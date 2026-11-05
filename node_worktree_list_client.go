package panewire

import (
	"context"
	"errors"
	"flag"
	"io"
)

type nodeWorktreeListLines []nodeListedWorktree

func runNodeWorktreeListCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	flags := flag.NewFlagSet("panewire node worktree-list", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	connect := nodeReapFlags(flags)
	connect.allowOperator = true
	flags.StringVar(connect.machine, "host", "", "machine hosting the repository")
	common := flags.String("common", "", "repository common git directory or checkout")
	all := flags.Bool("all", false, "read every page")
	if flags.Parse(args) != nil || flags.NArg() != 0 || !validNodeCommandPath(*common) {
		return ExitUsage
	}
	return connect.run(stdout, stderr, deps, func(_ context.Context, client *NodeReapClient, machine string) (any, error) {
		return collectNodeWorktrees(*common, *all, func(request nodeWorktreeListArgs) (nodeWorktreeListResult, error) {
			ctx, cancel := context.WithTimeout(context.Background(), nodeReapClientTimeout)
			defer cancel()
			var result nodeWorktreeListResult
			err := client.command(ctx, machine, nodeCommandWorktreeList, request, &result)
			return result, err
		})
	})
}

func collectNodeWorktrees(common string, all bool, call func(nodeWorktreeListArgs) (nodeWorktreeListResult, error)) (nodeWorktreeListLines, error) {
	request := nodeWorktreeListArgs{Common: common}
	rows := nodeWorktreeListLines{}
	seen := map[string]bool{}
	for {
		result, err := call(request)
		if err != nil {
			return nil, err
		}
		rows = append(rows, result.Worktrees...)
		if !all || result.NextCursor == "" {
			return rows, nil
		}
		if seen[result.NextCursor] {
			return nil, errors.New("repeated worktree cursor")
		}
		seen[result.NextCursor] = true
		request.Cursor = result.NextCursor
	}
}
