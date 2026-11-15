# Releasing panewire

## Cut a release

1. Merge the change to `main`.
2. Fetch `main` so that `origin/main` is the merge commit, then tag it and
   push the tag:
   ```sh
   git fetch origin main
   git tag vMAJOR.MINOR.PATCH origin/main
   git push origin vMAJOR.MINOR.PATCH
   ```
3. `.github/workflows/release.yml` first waits for the main-stage gate on the
   release commit (`ci-ok` today; set the `PANEWIRE_RELEASE_GATE_CONTEXT`
   repository variable to `deploy-ok` when §4-3 switches the gate). It then
   calls `scripts/build-release.sh dist/release
   <tag>`. The script builds CGO-disabled (`CGO_ENABLED=0`, `-trimpath`) binaries
   named `panewire_<tag>_<os>_<arch>` for linux/amd64, linux/arm64,
   darwin/amd64 and darwin/arm64, and writes `SHA256SUMS`. It then writes
   `components.json`, which names the daemons (hub, node) the release changed;
   see [`release-components.md`](release-components.md). The workflow
   checks that `panewire version` prints the tag. It runs the tests. Then it
   publishes the GitHub Release with `GITHUB_TOKEN`.

Pushing a test tag `v0.0.0-<anything>` (for example `v0.0.0-test.1`) runs
the same build and checks on a real tag and publishes nothing.

Every pull request runs the same build and checks for the dry-run tag
`v0.0.0-dryrun.<run>`, and publishes nothing. A build without a version
argument (`sh scripts/build-release.sh dist/release`) still stamps
`pw-<first 7 hex of HEAD>`.

## How hosts get it

No workflow connects to a host. On each host, the fleet updater
(`fleet-update`) reads the latest release, or the tag pinned for that host.
It verifies the asset against `SHA256SUMS` and swaps the binary atomically.
It keeps the old binary as `panewire.prev` and restarts the owning service.
If the version or service check fails, it restores `panewire.prev`. The hub's
`panewire update publish` remains a separate, operator-run path. Its release
notes show the command for each tag.

A release that stops reading a config key says so in its release notes, naming
the key and the file. An unknown key in a config file is ignored with one
warning per key, not refused, so an upgraded hub's service check still passes
while the key stays in the file. A malformed value for a known key still
refuses the file. The one exception is `runtug_path` in spawn.json, which still
refuses, because ignoring it would run a different binary than the one
configured. Operators delete a retired key once the notes name it.

## Pin, hold, roll back

- **Pin or hold:** set `version=vX.Y.Z` on the host's line in the fleet-update
  manifest. Remove the pin to follow `latest` again.
- **Roll back the fleet:** pin the previous good tag. Alternatively, mark the
  bad release as a pre-release or delete it, so that `latest` points to the
  previous release on the next run.
- **Roll back one host now:** copy the previous binary to a temporary file in
  the same directory and rename it over `panewire`. A rename works while the
  service still runs the old file, whereas copying onto a running executable
  fails on Linux with `ETXTBSY`:
  ```sh
  cp -p ~/.local/bin/panewire.prev ~/.local/bin/panewire.new
  mv -f ~/.local/bin/panewire.new ~/.local/bin/panewire
  ```
  Restart the owning service and check that it is active and that the
  version command prints the old tag. Then pin that tag, so that the next
  run does not reinstall the bad release.
