# R18 report relay compatibility

R18's report relay is superseded by the R19 lane contract in
[r19-lanes.md](r19-lanes.md). Existing `--report-relay-routes` deployments and
files using the `routes` JSON key remain readable, but new deployments should
use `--lanes /etc/panewire/lanes.json` and the `lanes` key.
