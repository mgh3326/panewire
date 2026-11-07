"""Packs pacelever's golden verdicts into panewire's testdata: one shared catalog."""
import json, sys
d = json.load(open(sys.argv[1]))
catalog = d[0]["catalog"]
assert all(s["catalog"] == catalog for s in d)
for s in d:
    del s["catalog"]
    p = s["pacelever"]
    for k in ("missing_windows", "alternatives"):
        if p[k] is None: del p[k]
out = {"source": "pacelever v0.3.28 (9cd47a6) Gate via the CLI --account-key flow; regenerate with generator/regen.sh",
       "catalog": catalog, "scenarios": d}
json.dump(out, open(sys.argv[2], "w"), indent=1)
open(sys.argv[2], "a").write("\n")
