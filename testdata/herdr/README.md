# herdr CLI fixtures (verbatim shapes)

Captured once from the real `herdr` CLI on an operator machine (read-only
commands only), with pane ids, session ids, lane names, and machine ids
replaced by neutral placeholders. The *shape* — key names, nesting, and the
`agent_status` vocabulary — is verbatim and is what the node parses.

| file | command | stream | exit |
|---|---|---|---|
| `agent-get-<status>.json` | `herdr agent get <pane>` | stdout | 0 |
| `agent-get-not-found.json` | `herdr agent get <missing pane>` | stdout | 1 |
| `agent-wait-idle.json` / `agent-wait-done.json` | `herdr agent wait <pane> --until idle --until done --timeout <ms>` | stdout | 0 |
| `agent-wait-timeout.json` | same, deadline reached | **stderr** | 1 |

Two contract facts the shapes do not show on their own:

- `herdr agent wait --timeout` is in **milliseconds**. A `max_wait` expressed
  in seconds must be converted before it reaches argv.
- Without `--until`, `wait` matches idle, done, **or blocked**. The node always
  passes `--until idle --until done` explicitly, so a session that returns to
  `blocked` does not release a held injection.

`agent_status` vocabulary: `idle | working | blocked | done | unknown`.
