#!/bin/sh
# Builds the GitHub Release assets for the checked-out commit:
#   panewire_<version>_<os>_<arch> for each target, plus SHA256SUMS.
#
# <version> is pw-<first 7 hex of HEAD>, the form hub-status already shows
# (pw-05667f4). It is stamped with -X main.version, named in every asset, and is
# the string `update publish --version` must carry: the hub requires the asset
# name to contain it and the node's pre-rename smoke run requires `version` to
# print it. dist-version.sh validates it against the hub version pattern.
#
# PANEWIRE_RELEASE_TARGETS overrides the target list (tests build a subset).
# Prints the version on stdout.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
output=${1:-"$repo_root/dist/release"}
case "$output" in
	/*) ;;
	*) output="$(pwd)/$output" ;;
esac
targets=${PANEWIRE_RELEASE_TARGETS:-"darwin/amd64 darwin/arm64 linux/amd64 linux/arm64"}

sha=$(git -C "$repo_root" rev-parse HEAD | cut -c1-7)
version=$(PANEWIRE_DIST_VERSION="pw-$sha" "$script_dir/dist-version.sh")

mkdir -p "$output"
assets=
for target in $targets; do
	os=${target%/*}
	arch=${target#*/}
	case "$os/$arch" in
		darwin/amd64 | darwin/arm64 | linux/amd64 | linux/arm64) ;;
		*)
			echo "build-release: unsupported target '$target'" >&2
			exit 1
			;;
	esac
	asset="panewire_${version}_${os}_${arch}"
	(
		cd "$repo_root"
		GOOS=$os GOARCH=$arch CGO_ENABLED=0 go build -ldflags "-X main.version=$version" -o "$output/$asset" ./cmd/panewire
	)
	assets="$assets $asset"
done

(
	cd "$output"
	# shellcheck disable=SC2086 # asset names contain no whitespace
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum $assets >SHA256SUMS
	else
		shasum -a 256 $assets >SHA256SUMS
	fi
)
printf '%s\n' "$version"
