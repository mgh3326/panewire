#!/usr/bin/env python3
"""Compare real fake-backed dry-run output for every imported profile and role.

Only the tool name and environment-variable prefix are normalized. Missing
snapshots or nonzero test commands are errors. Both comparison controls must
fail before the source/head comparison is accepted.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def normalized(text):
    return text.replace("runtug", "panewire").replace("RUNTUG", "PANEWIRE")


def compare(left, right):
    if not left or not right:
        raise ValueError("empty dry-run matrix")
    return [key for key in sorted(left.keys() | right.keys()) if left.get(key) != right.get(key)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path, help="unchanged pinned source worktree")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    template = (root / "scripts/spawn-parity.go.template").read_text()
    revision = subprocess.check_output(["git", "-C", str(args.source), "rev-parse", "HEAD"], text=True).strip()
    pinned = json.loads((root / "docs/spawn-import.json").read_text())["source_revision"]
    if revision != pinned:
        raise SystemExit(f"source head {revision} differs from pinned {pinned}")
    snapshots = []
    with tempfile.TemporaryDirectory(prefix="spawn-parity-") as temporary:
        for label, checkout, package in [("source", args.source, "runtug"), ("head", root, "fleet")]:
            harness = checkout / "internal" / package / "migration_parity_test.go"
            output = Path(temporary) / f"{label}.json"
            with harness.open("x") as handle:
                handle.write(template.replace("PACKAGE", package))
            try:
                env = dict(os.environ, SPAWN_PARITY_OUTPUT=str(output))
                subprocess.run(["go", "test", "-p", "2", f"./internal/{package}", "-run", "^TestMigrationDryRunGolden$", "-count=1"], cwd=checkout, env=env, check=True)
                snapshots.append(json.loads(normalized(output.read_text())))
            finally:
                harness.unlink()
    left, right = snapshots
    # Controls: output mutation and role assignment swap must both be detected.
    keys = sorted(left)
    changed = json.loads(json.dumps(left))
    changed[keys[0]]["Output"] += "known bad mutation"
    if not compare(left, changed):
        raise SystemExit("output mutation control was not detected")
    different = next((key for key in keys if left[key] != left[keys[0]]), None)
    if different is None:
        raise SystemExit("no distinct snapshots for role-assignment control")
    swapped = dict(left)
    swapped[keys[0]], swapped[different] = swapped[different], swapped[keys[0]]
    if not compare(left, swapped):
        raise SystemExit("assignment mutation control was not detected")
    failures = compare(left, right)
    args.output.write_text(json.dumps({"source_revision": pinned, "cases": len(left), "controls": "PASS", "mismatches": failures, "source": left, "head": right}, indent=2) + "\n")
    if failures:
        raise SystemExit("dry-run mismatches: " + ", ".join(failures))
    print(f"PASS: {len(left)} dry-run plans and exit codes match; both mutation controls detected")


if __name__ == "__main__":
    main()
