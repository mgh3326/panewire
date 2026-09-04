# R18 report relay local round trip

The hub loads its operator-owned route map from
`/etc/panewire/report-relay.json` (or `panewire hub --report-relay-routes`).
The file contains identifiers only:

```json
{"routes":{"<owner-lane>":{"machine":"<machine-id>","pane":"<test-pane-id>"}}}
```

For a safe desktop check, create a fresh herdr test pane, point a local
httptest hub/node at that route, then write a `job.completed` event with a
report path. Confirm the node receives `relay.inject`, the fake herdr records
`agent prompt`, and the hub receives `relay.delivered`. Do not target an
interactive operator pane. `panewire relay routes --routes <path>` prints the
effective map; `panewire relay test --routes <path> --lane <lane>` checks that
the requested route exists before that authenticated round trip.

The node first prompts and then reads the pane. A visible pasted-text chip is
reported as `relay.unconfirmed`; no automatic duplicate prompt is attempted.
