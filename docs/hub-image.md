# Hub container image

`.github/workflows/image.yml` builds `Dockerfile` into a container image for
the hub. Node daemons are unchanged: they keep installing the release
binaries from `scripts/build-release.sh`.

| Event | What happens |
|---|---|
| pull request, manual run | build linux/amd64, smoke-test; nothing is pushed |
| push to `main` | the same, then a separate `publish` job — the only one holding `packages: write` — pushes linux/amd64 + linux/arm64 to this repository's GitHub Container Registry package (lowercased name), tagged with the full commit SHA and `main` |

There is no deploy step; rolling a host onto the image is a separate,
operator-run change.

## Image

- One static binary (`CGO_ENABLED=0`) at `/panewire` on
  `distroless/static-debian12:nonroot`: no shell, no source, no env files or
  secrets. It runs as `nonroot` (uid 65532).
- `ENTRYPOINT ["/panewire"]`, `CMD ["hub"]`: arguments after the image name
  replace `hub`, so `docker run IMAGE hub --hub-auth /run/secrets/hub.env …`
  passes hub flags and `docker run IMAGE version` prints the stamp.
- `panewire version` prints `pw-<first 7 hex of the commit>`, the same form as
  the release binaries and `hub-status`. The smoke test checks it, and that
  the hub refuses to start without `--hub-auth` (exit 2).

## Running the hub from it

- Credentials and config files (`--hub-auth`, `--lanes`, `--handoffkeep-env`,
  …) are mounted read-only; they must stay mode 0600 and readable by uid 65532.
- The hub listens only on a loopback or tailnet address
  (`hubListenAddress`), so the container needs the host's network namespace
  (`--network host`) to be reachable as today.
- The inbox store needs persistent storage: either set
  `PANEWIRE_HUB_DATABASE_URL` (see [hub inbox on Postgres](hub-postgres.md)) or
  mount a volume at the `--inbox-store` path. The default path inside the
  container's home is not persistent.
