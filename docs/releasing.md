# Releasing panewire

## Cut a release

1. Merge the change to `main`.
2. Tag the merge commit and push the tag:
   ```sh
   git tag vMAJOR.MINOR.PATCH origin/main
   git push origin vMAJOR.MINOR.PATCH
   ```
3. `.github/workflows/release.yml` calls `scripts/build-release.sh dist/release
   <tag>`. The script builds static (`CGO_ENABLED=0`, `-trimpath`) binaries
   named `panewire_<tag>_<os>_<arch>` for linux/amd64, linux/arm64,
   darwin/amd64 and darwin/arm64, and writes `SHA256SUMS`. The workflow
   checks that `panewire version` prints the tag. It runs the tests. Then it
   publishes the GitHub Release with `GITHUB_TOKEN`.

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

## Pin, hold, roll back

- **Pin or hold:** set `version=vX.Y.Z` on the host's line in the fleet-update
  manifest. Remove the pin to follow `latest` again.
- **Roll back the fleet:** pin the previous good tag. Alternatively, mark the
  bad release as a pre-release or delete it, so that `latest` points to the
  previous release on the next run.
- **Roll back one host now:** copy `panewire.prev` over `panewire`, restart the
  service, then pin the tag so that the next run does not reinstall the bad
  release.
