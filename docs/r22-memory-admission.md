# R22 memory-aware placement admission

R22 adds bounded memory telemetry to node heartbeats so the hub can keep a
memory-pressured candidate out of a placement decision. The telemetry contains
only measurements; command output is parsed locally and discarded.

## Heartbeat field

`host_memory` is an optional top-level heartbeat object. Its numeric fields are
nullable Go `*float64` values and are always serialized: a measurement that was
not obtained is represented by JSON `null`, not by a fabricated zero or by an
omitted numeric field.

| field | JSON type | `null` meaning | local collection |
| --- | --- | --- | --- |
| `free_pct` | number or null | free-memory percentage was unavailable | Darwin: `memory_pressure`, then `vm_stat`; Linux: `/proc/meminfo` |
| `compressed_mb` | number or null | compressor measurement was unavailable | Darwin: compressor pages from `memory_pressure` or `vm_stat`; Linux: null because it has no macOS-compressor equivalent |
| `swap_used_mb` | number or null | swap-use measurement was unavailable | Darwin: `sysctl -n vm.swapusage`; Linux: `/proc/meminfo` |
| `psi_some_avg10` | number or null | pressure-stall metric is unavailable | Linux: `/proc/pressure/memory`; null on Darwin and on Linux systems without PSI |
| `source` | string | never null | `memory_pressure`, `vm_stat`, or `proc_meminfo` |

The hub rejects a present `host_memory` object with an unknown `source`, an
out-of-range `free_pct`, a negative non-null measurement, or NaN/Infinity.
An absent `host_memory` remains valid for older nodes.

## Admission thresholds

The operator-owned placement policy accepts two optional fields:

| field | default | exclusion boundary |
| --- | ---: | --- |
| `memory_free_pct_min` | 30 | exclude only when `free_pct < memory_free_pct_min` |
| `swap_used_mb_max` | 1536 | exclude only when `swap_used_mb > swap_used_mb_max` |

Therefore free memory equal to the minimum passes, and swap use equal to the
maximum passes. An explicit policy value of zero is distinct from an omitted
field; omission receives the defaults above. A valid changed policy file
hot-reloads, so an operator can adjust these thresholds without restarting the
hub.

When either known signal crosses its boundary, the candidate reason includes
`not_accepting(memory_pressure)`. This uses the existing non-accepting
selection path: a safe spill target is selected in policy order. If the local
candidate is pressured and no usable spill target exists, the decision is
`unavailable`. This is intentionally fail-closed to protect the local host;
the mitigation is to hot-reload an adjusted threshold policy.

When `host_memory` is absent, or either admission signal is null, placement is
fail-open: it does not exclude the candidate or change its score. The candidate
can record `memory_unknown` as evidence. This preserves behavior for partial
collection failures and nodes that predate R22.

## Deployment order and compatibility

Deploy the hub before deploying nodes. Older hubs reject an unknown top-level
heartbeat field, while the new hub accepts heartbeats from older nodes that do
not send `host_memory` and treats their memory as unknown/fail-open. After the
hub is updated, nodes may be rolled out normally; no fleet-wide simultaneous
redeploy is required.

Example policy identifiers are deliberately generic:

```json
{"local_machine":"host-a","spill_targets":["host-b"],"max_active_jobs":10,"load_ratio":0.6,"memory_free_pct_min":30,"swap_used_mb_max":1536}
```
