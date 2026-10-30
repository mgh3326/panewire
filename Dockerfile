# syntax=docker/dockerfile:1

# Hub container image (docs/hub-image.md). Node daemons stay release binaries
# (scripts/build-release.sh); this image is for the hub only.
#
# The build context keeps .git so go build stamps vcs.revision, as in the
# release binaries; .dockerignore must not exclude it.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
# PANEWIRE_VERSION follows the release form pw-<7 hex of HEAD>; image.yml
# passes it and the smoke test reads it back with `panewire version`.
ARG PANEWIRE_VERSION=panewire-dev
# CGO_ENABLED=0: the SQLite driver is pure Go, so the binary is fully static
# for the distroless runtime stage.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-X main.version=${PANEWIRE_VERSION}" -o /out/panewire ./cmd/panewire

# Final stage: one static binary, nonroot, no shell, no source, no env files,
# no secrets. distroless/static ships CA certs and the nonroot user (65532).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/panewire /panewire
USER nonroot
ENTRYPOINT ["/panewire"]
CMD ["hub"]
