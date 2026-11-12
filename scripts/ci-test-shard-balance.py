#!/usr/bin/env python3
"""Balance tests across shards using recorded test durations (operator decision 178).

Usage:
    ci-test-shard-balance.py INDEX TOTAL [DURATIONS_FILE]

Reads test names from standard input (one per line).
Outputs the test names assigned to shard INDEX (0-based) of TOTAL.
Assignment uses deterministic Largest Processing Time (LPT) bin packing:
- Tests are sorted descending by recorded duration, with ties broken by test name.
- Tests absent from DURATIONS_FILE use the mean of all measured durations.
- If total measured durations is 0, a fallback of 0.01s is used.
- Tests are placed in the shard with the minimum accumulated duration (ties broken
  by lowest shard index).
"""

import json
import os
import sys

def main() -> None:
    if len(sys.argv) < 3:
        sys.stderr.write("usage: ci-test-shard-balance.py INDEX TOTAL [DURATIONS_FILE]\n")
        sys.exit(2)

    try:
        index = int(sys.argv[1])
        total = int(sys.argv[2])
    except ValueError:
        sys.stderr.write(f"bad shard spec: {sys.argv[1]}/{sys.argv[2]}\n")
        sys.exit(2)

    if total < 1 or index < 0 or index >= total:
        sys.stderr.write(f"shard {index}/{total} out of range\n")
        sys.exit(2)

    durations_path = sys.argv[3] if len(sys.argv) > 3 else os.path.join(
        os.path.dirname(__file__), "ci-test-durations.json"
    )

    durations = {}
    if os.path.isfile(durations_path):
        try:
            with open(durations_path, "r", encoding="utf-8") as f:
                durations = json.load(f)
        except Exception as e:
            sys.stderr.write(f"warning: failed to read {durations_path}: {e}\n")

    # Read names from stdin
    names = []
    for line in sys.stdin:
        name = line.strip()
        if name and not name.startswith("ok "):
            names.append(name)

    if not names:
        return

    # Calculate fallback duration for tests not in durations file
    measured = [float(durations[n]) for n in names if n in durations and isinstance(durations[n], (int, float))]
    fallback = (sum(measured) / len(measured)) if measured else 0.01

    # LPT: sort items descending by duration, tie-break by name
    items = []
    for n in names:
        d = float(durations[n]) if n in durations and isinstance(durations[n], (int, float)) else fallback
        items.append((d, n))
    items.sort(key=lambda x: (-x[0], x[1]))

    shard_loads = [0.0] * total
    assigned = []

    for d, n in items:
        min_s = 0
        min_load = shard_loads[0]
        for s in range(1, total):
            if shard_loads[s] < min_load:
                min_load = shard_loads[s]
                min_s = s
        shard_loads[min_s] += d
        if min_s == index:
            assigned.append(n)

    # Output assigned names (sorted alphabetically for deterministic order)
    assigned.sort()
    for n in assigned:
        sys.stdout.write(f"{n}\n")

if __name__ == "__main__":
    main()
