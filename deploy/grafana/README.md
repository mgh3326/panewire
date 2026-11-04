# Fleet observability dashboard and alerts

Grafana as code for the fleet events that Alloy ships to Loki (see
`../alloy/README.md`, "Fleet events"). Provisioning is manual: the desk applies
these files once after merge and again whenever they change.

| File | What |
| --- | --- |
| `fleet-dashboard.json` | Dashboard `fleet-observability`; the panels are listed below. |
| `fleet-alert-rules.json` | Rule group `fleet-observability` in folder `fleet-observability`; the rules are listed below. |
| `apply.sh` | Idempotent apply: creates the folder (tolerates an existing one), then overwrites the dashboard and the rule group by uid. |

The dashboard has these panels:

- spawn refusals by host and reason;
- placements by chosen host and ranking metric (`raw_load` marks the fallback);
- review outcomes;
- reviewer failures by reviewer;
- node spawn refusals by reason;
- quota observation age at refusal;
- a log panel of recent failures.

The rule group has these rules:

| Rule | Fires when |
| --- | --- |
| Repeated spawn refusals | More than 3 refusals for the same host and reason within 30 m. |
| Review missing | No reviewer produced a pass within 1 h. |
| Reviewer failing repeatedly | The same reviewer on the same fleet host fails 3 or more times within 2 h. |
| Quota observation stale | A refusing quota observation is older than 30 min (`age_min`). |
| Fleet events pipeline silent | No fleet event reached Loki in 6 h. |

"Reviewer failing repeatedly" groups by `host` and `reviewer`. `host` is the
fleet machine id of the mergewake host that recorded the failure (see
`../alloy/README.md`). `reviewer` comes from the event's `detail`
(`reviewer=`). For a headless reviewer it is the mergewake review profile,
which ran on that host. For a panewire pane it is the transport, `panewire`
(`runtug` in older events): mergewake spawns the pane with `-m auto` and
`--host auto`, and its event does not record the profile or the host that
panewire chose. Keying pane failures by profile and pane host needs mergewake
to add both to the reviewer event's `detail` when the spawn result names them.

The rules count events, so "no data" is the healthy state for the first four,
and they keep `noDataState: OK`. A dead pipeline (alloy down, missing grants,
a writer bug) would leave all of them OK forever. The fifth rule catches that
case with `absent_over_time`, which returns a value only when nothing arrived.
Grouping labels that a line lacks become `unspecified`, never an empty
string.

```sh
./apply.sh --dry-run    # prints each request and body; reads no credentials
GRAFANA_CLOUD_URL=https://<stack>.grafana.net GRAFANA_CLOUD_SA_TOKEN=<token> ./apply.sh
```

`GRAFANA_CLOUD_SA_TOKEN` is a Grafana service-account token. The writes need
the Editor role; with a lower role, `apply.sh` stops at the first write and
says so. `apply.sh` passes the token to curl through a mode-0600 header file,
never on the command line.

The Loki data source uid is looked up with `GET /api/datasources`:

- with one data source of type `loki`, that one is used;
- with several, the one named `grafanacloud-*-logs` is used;
- otherwise the script stops and lists the names it found.

`apply.sh` needs `curl` and `python3`.

Thresholds (3, 1 h, 2 h, 30 min) are plain numbers in `fleet-alert-rules.json`.
Edit them there and re-apply.
