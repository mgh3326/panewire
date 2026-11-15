# Long lane.event text (hk 4835)

A `lane.event` longer than the typed-line limit is kept whole in the lane inbox
and typed into the pane as a head plus a pointer. The pane line never exceeds
the typed limit, and an older hub or node never receives text it refuses.

## Bounds

| Bound | Value | Where it lives |
|---|---|---|
| Typed-line limit (`laneEventTextLimit`) | 2048 bytes | `emit.go` |
| Sink limit (`laneEventTextLimitSink`) | 8192 bytes | `emit.go` |
| Ceiling (`laneEventTextCeiling`) | 16 KiB | `text_bounds.go` |
| Frame maximum (`laneEventFrameMax`) | 104 KiB | `text_bounds.go` |

The ceiling is 16 KiB, not 64 KiB, because every record and frame that carries
it must fit `laneEventFrameMax`. encoding/json writes `<`, `>` and `&` as six-byte
escapes, so 16 KiB of text can encode to 96 KiB. The hub's websocket read limit
and the node's event-file scan cap both take that frame maximum. The
heartbeat budget (`hubMaxMessageBytes`, 32 KiB) is unchanged.

## Producer (`panewire emit --kind lane.event`)

- Without `--sink`, text up to the ceiling is written whole into the event file.
  Longer text is refused with `emit: --text is N bytes; the lane.event ceiling is 16384`.
- With `--sink`, text is cut to 8192 bytes at emit, as before. A sink lane types
  nothing, so its record is the cut text.

## Hub

- A node sender's text up to the ceiling is accepted for any route. Text over the
  ceiling is refused with `relay.rejected` reason `text_too_long`.
- A sink route with node text over 8192 bytes is cut to 8192 with `[truncated]`,
  not refused, so the producer's record is kept.
- HTTP ingress keeps its own contract (`docs/r24-http-ingress.md`): 2048 bytes for
  a pane route, 8192 for a sink route.
- `receiveInbox` stores the whole event in the lane inbox row. It sets
  `hubJobEventPayload.inboxID` to that row, which the typed line then names.

## Typed line (`laneEventRelayText`)

- Text that fits the typed limit is typed whole, as before.
- Longer text is typed as a head, a space, and
  `[full text: panewire inbox read <id>]`, where `<id>` is the inbox row. The whole
  line is at most 2048 bytes. With no inbox row, the marker is `[truncated]`.

## Gate: a hub without `lane_event_long`

A hub that advertises `lane_event_long` in `hub.features` accepts the whole text.
A node connection whose hub did not advertise it sends the cut form instead:
text over the typed limit (or over 8192 for a sink event) is cut with
`[truncated]`, and the payload carries `truncated: true`
(`laneEventPayloadForOldHub`, applied in `relayWire`).

Consequences:

- Nothing is refused by a mixed-version hub, so there is no retry loop.
- The hub keeps the cut text. The producer's event file keeps the whole text
  and is the record if the full text is needed.
- The typed line to a node is always within 2048 bytes, so no node receives a
  long frame and the gate is not needed on the hub-to-node path.

## Deployment order

- Hub first: a hub that advertises `lane_event_long` before its nodes upgrade is
  what lets a node send whole text. A node upgraded before its hub sends the
  cut form, which every hub accepts.
- Restart a host's node daemon with the upgraded binary before its producers emit
  text over 8192 bytes. Until it restarts, `panewire emit` on that host reports
  `daemon rejected the event: invalid emit request` (exit 2). The durable event
  file is already written, and the upgraded daemon relays it after the restart.
  The CLI cannot tell the daemon's version, so it reports the refusal as it is.
