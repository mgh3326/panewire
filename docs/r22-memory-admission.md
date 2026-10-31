# R22 memory-aware placement admission

R22 adds bounded memory telemetry to node heartbeats so the hub can keep a
memory-pressured candidate out of a placement decision. The telemetry contains
only measurements; command output is parsed locally and discarded.

## Heartbeat field

`host_memory` is an optional top-level heartbeat object. Its original numeric
fields are nullable Go `*float64` values and are serialized as JSON `null` when
unavailable. The additive live fields `swap_in_per_s` and `pressure` are
omitted when unavailable; neither reports an invented zero.

| field | JSON type | `null` meaning | local collection |
| --- | --- | --- | --- |
| `free_pct` | number or null | free-memory percentage was unavailable | Darwin: `memory_pressure`, then `vm_stat`; Linux: `/proc/meminfo` |
| `compressed_mb` | number or null | compressor measurement was unavailable | Darwin: compressor pages from `memory_pressure` or `vm_stat`; Linux: null because it has no macOS-compressor equivalent |
| `swap_used_mb` | number or null | swap-use measurement was unavailable | Darwin: `sysctl -n vm.swapusage`; Linux: `/proc/meminfo` |
| `psi_some_avg10` | number or null | pressure-stall metric is unavailable | Linux: `/proc/pressure/memory`; null on Darwin and on Linux systems without PSI |
| `swap_in_per_s` | number or null | no sample interval or swap-in counter is unavailable | Linux: delta of `/proc/vmstat` `pswpin` pages; Darwin: delta of `Swapins` pages from `vm_stat` |
| `pressure` | number or null | pressure signal is unavailable | Linux: PSI `some avg10` percent; Darwin: `kern.memorystatus_vm_pressure_level` normalized from kernel levels 1/2/4 to 0 normal, 1 warn, 2 critical |
| `source` | string | never null | `memory_pressure`, `vm_stat`, or `proc_meminfo` |

The hub rejects a present `host_memory` object with an unknown `source`, an
out-of-range `free_pct`, a negative non-null measurement, or NaN/Infinity.
An absent `host_memory` remains valid for older nodes.

## Admission thresholds

The operator-owned placement policy accepts these optional fields:

| field | default | exclusion boundary |
| --- | ---: | --- |
| `memory_free_pct_min` | 30 | exclude only when `free_pct < memory_free_pct_min` |
| `swap_used_mb_max` | 1536 | exclude only when `swap_used_mb > swap_used_mb_max` |
| `swap_in_per_s_max` | 10 | live swap-in rate above pages/second threshold excludes |
| `psi_some_avg10_max` | 1 | Linux live PSI `some avg10` percent above threshold excludes |
| `pressure_level_min` | 1 | Darwin pressure level at or above threshold excludes |

Therefore free memory equal to the minimum passes, and swap use equal to the
maximum passes. An explicit policy value of zero is distinct from an omitted
field; omission receives the defaults above. A valid changed policy file
hot-reloads, so an operator can adjust these thresholds without restarting the
hub.

When free memory is below its minimum, or a live signal crosses its configured
boundary, the candidate reason includes
`not_accepting(memory_pressure)`. This uses the existing non-accepting
selection path: a safe spill target is selected in policy order. If the local
candidate is pressured and no usable spill target exists, the decision is
`unavailable`. This is intentionally fail-closed to protect the local host;
the mitigation is to hot-reload an adjusted threshold policy.

Because memory pressure follows the non-accepting path, `wake_on_spill` does
not wake a disconnected spill target for memory pressure: the decision remains
`unavailable`. Capacity-pressure spill behavior is unchanged.

When either additive live field (`swap_in_per_s` or `pressure`) is present,
swap used alone is ignored because it can remain high after pressure ends.
Older Linux nodes may already send `psi_some_avg10`; that existing field alone
does not disable the previous `swap_used_mb_max` fallback. Nodes with neither
new field preserve the old behavior. Missing signals otherwise remain
fail-open.

## Deployment order and compatibility

Deploy the hub before deploying nodes. The updated hub accepts heartbeats from
older nodes that do not send the live fields and applies the old swap-used
fallback. After the hub is updated, nodes may be rolled out normally; no
fleet-wide simultaneous redeploy is required.

Example policy identifiers are deliberately generic:

```json
{"local_machine":"host-a","spill_targets":["host-b"],"max_active_jobs":10,"load_ratio":0.6,"memory_free_pct_min":30,"swap_used_mb_max":1536,"swap_in_per_s_max":10,"psi_some_avg10_max":1,"pressure_level_min":1,"capacity_weight":{"host-a":1,"host-b":2}}
```
