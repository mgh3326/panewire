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
| Reviewer failing repeatedly | The same reviewer fails 3 or more times within 2 h. |
| Quota observation stale | A refusing quota observation is older than 30 min (`age_min`). |

```sh
./apply.sh --dry-run    # prints each request and body; reads no credentials
GRAFANA_URL=https://<stack>.grafana.net GRAFANA_TOKEN=<token> LOKI_DATASOURCE_UID=<uid> ./apply.sh
```

`GRAFANA_TOKEN` is a Grafana service-account token with the Editor role.
`apply.sh` passes it to curl through a mode-0600 header file, never on the
command line. `LOKI_DATASOURCE_UID` is the uid of the stack's Loki data source,
which is the last path segment of its page under Connections > Data sources.

Thresholds (3, 1 h, 2 h, 30 min) are plain numbers in `fleet-alert-rules.json`.
Edit them there and re-apply.
