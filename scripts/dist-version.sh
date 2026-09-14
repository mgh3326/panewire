#!/bin/sh
# Prints the version stamped into release builds of ./cmd/panewire.
#
# The value is $PANEWIRE_DIST_VERSION when set, otherwise
# `git describe --always --dirty`. The result must satisfy the hub version
# pattern ^[A-Za-z0-9._-]{1,64}$ (hub.go): anything outside it is rejected here
# so an out-of-pattern string can never be injected into a node binary —
# MainWithVersion would silently replace it with "panewire-dev" and the node
# would report the placeholder again.
set -eu

if [ "${PANEWIRE_DIST_VERSION+x}" = x ]; then
	version=$PANEWIRE_DIST_VERSION
else
	repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
	version=$(git -C "$repo_root" describe --always --dirty)
fi

case $version in
"" | *[!A-Za-z0-9._-]*)
	echo "dist-version: version '$version' is outside ^[A-Za-z0-9._-]{1,64}\$" >&2
	exit 1
	;;
esac
if [ "${#version}" -gt 64 ]; then
	echo "dist-version: version '$version' exceeds 64 characters" >&2
	exit 1
fi
printf '%s\n' "$version"
