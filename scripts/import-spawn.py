#!/usr/bin/env python3
"""Reproduce the initial fleet import from the pinned former launcher source.

Run once on a clean migration worktree. Later edits are ordinary panewire code;
--check verifies the initial import manifest without overwriting those edits.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

SOURCE = "6519bc855fa5597cf21cd7648e097e2b2fde809f"
OLD = "github.com/mgh3326/runtug"
NEW = "github.com/mgh3326/panewire"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    names = subprocess.check_output(
        ["git", "-C", str(args.source), "ls-tree", "-r", "--name-only", SOURCE, "internal"], text=True
    ).splitlines()
    if not names:
        raise SystemExit("pinned source has no internal packages")
    manifest = {"source_revision": SOURCE, "files": {}}
    for name in names:
        raw = subprocess.check_output(["git", "-C", str(args.source), "show", f"{SOURCE}:{name}"])
        try:
            text = raw.decode().replace(OLD + "/internal/runtug", NEW + "/internal/fleet")
            text = text.replace(OLD, NEW).replace("package runtug", "package fleet")
            text = text.replace("runtug", "panewire").replace("RUNTUG", "PANEWIRE")
            payload = text.encode()
        except UnicodeDecodeError:
            payload = raw
        target = name.replace("internal/runtug/", "internal/fleet/")
        manifest["files"][target] = {"source_sha256": hashlib.sha256(raw).hexdigest(), "import_sha256": hashlib.sha256(payload).hexdigest()}
        destination = root / target
        if not args.check:
            if destination.exists() and destination.read_bytes() != payload:
                raise SystemExit(f"refusing to replace modified file: {target}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
    output = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    path = root / "docs" / "spawn-import.json"
    if args.check:
        if path.read_text() != output:
            raise SystemExit("source import manifest differs")
        print(f"verified {len(names)} source files at {SOURCE}")
    else:
        path.write_text(output)


if __name__ == "__main__":
    main()
