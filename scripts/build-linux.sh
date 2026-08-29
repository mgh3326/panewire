#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
output=${1:-"$repo_root/dist/panewire-linux-amd64"}
case "$output" in
	/*) ;;
	*) output="$(pwd)/$output" ;;
esac

mkdir -p "$(dirname -- "$output")"
(
	cd "$repo_root"
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$output" ./cmd/panewire
)
file "$output"
