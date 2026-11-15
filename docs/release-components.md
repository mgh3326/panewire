# Release components

Every GitHub Release names the daemon components its code changed, in
`components.json`. A rollout restarts only the named daemons; the CLI is
replaced in every release.

## What a release publishes

`.github/workflows/release.yml` runs `cmd/release-components` after
`scripts/build-release.sh` and before verification. It writes
`components.json` into the release assets and appends its checksum to
`SHA256SUMS`, so hosts that verify assets against `SHA256SUMS` verify it too.
The file has this shape:

```json
{
  "version": "v0.0.300",
  "previous": "v0.0.299",
  "basis": "diff",
  "components": ["hub"],
  "cli_only": false,
  "summary": "changed since v0.0.299: hub",
  "changed_files": 2,
  "path_map": 1
}
```

- `components` lists the daemons to restart: `hub`, `node`, or both. It is
  empty when `cli_only` is true.
- `basis` is `diff` when the changed paths came from
  `git diff --name-only <previous> <release commit>`, and `unknown` when there
  is no previous release tag or the diff could not run. `unknown` lists every
  component.
- `previous` is the newest published, non-prerelease GitHub Release that sorts
  before this tag (`v0.0.0*` test tags are skipped). It is read from published
  releases, not from `git tag`, so an unpublished or prerelease tag is never a
  baseline. A rerun of a published release uses the same baseline.
- `path_map` is the version of the rules below. Bump `PathMapVersion` in
  `internal/releasecomponents` when a rule changes.

## The path map

Rules are applied to each changed path; a release's components are the union.
The first matching rule wins.

| Paths | Components |
|---|---|
| `*_test.go`, `*.md`, `docs/`, `testdata/` | none |
| `hub_ui.html` (embedded by the hub's HTTP handler) | hub |
| `deploy/dev.panewire.panewired*` (node LaunchAgent template) | node |
| anything else | hub and node |

CI workflows, actions, build scripts and everything else not listed (for
instance `go.mod`) map to both components, because they change how the
daemon binaries are built. The last row is the conservative default. Root-package Go files are one
package compiled into one binary that runs as the hub, the node daemon or the
CLI, so the map cannot yet prove that a Go change reaches only one daemon.
Narrowing that needs the code split into packages per daemon; until then a Go
change restarts both.

Source of truth: `internal/releasecomponents/components.go`. Its tests
(`internal/releasecomponents/components_test.go`) pin the rules above.

## Related

- [`releasing.md`](releasing.md): how a release is cut and published.
- `release_components_workflow_test.go` pins the workflow steps that publish
  this file.
