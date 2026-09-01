# Burst policy

The hub owns no mutable burst configuration: the regular JSON file passed to
`panewire hub --burst-policy /etc/panewire/burst.json` is the source of truth.
It is read on start and checked on every maintenance/heartbeat pass. Invalid
replacement files are rejected with a warning and the last valid policy stays
active.

```json
{
  "source_machine": "mac-personal",
  "swap_gb": 8,
  "load5": 6,
  "consecutive": 3,
  "wake_via": "rpi",
  "wake_mac": "02:1a:2b:3c:4d:5e",
  "target_machine": "desktop",
  "idle_minutes": 30,
  "cooldown_minutes": 20
}
```

Either `swap_gb` or `load5` at its exact threshold counts as overload. The
source must report it for `consecutive` heartbeats. `wake_via` receives a
closed `burst` up event and sends WoL; the event includes no command text.
When the target reports zero `codex|claude` processes continuously for
`idle_minutes`, it receives a down event. A nonzero worker count resets idle
time and can never trigger a down event. The cooldown suppresses repeated up
or down sends.

Use a mode-0600 regular file (not a symlink). Change the requested knobs
without restarting the hub:

```sh
panewire burst show --burst-policy /etc/panewire/burst.json
panewire burst set --burst-policy /etc/panewire/burst.json --swap-gb 8 --load5 6 --consecutive 3 --idle-min 30
```

For the running hub's current policy plus its recent counter/load judgment,
use the operator credential:

```sh
panewire burst show --hub-url https://hub.example --hub-token-env /etc/panewire/hub-operator.env
```

Run the RPi node with `--burst-wake-mac 02:1a:2b:3c:4d:5e`, matching the
policy, or its existing paired `--failover-wake-on` and
`--failover-wake-mac` flags; the latter MAC is shared for burst wake. Existing
failover's paired-flag rejection remains in force. Run the desktop node with `--burst-poweroff-allowed` only after
passwordless `sudo -n /usr/sbin/poweroff` has been deliberately configured.
It is off by default. Hub Telegram notifications are one line per up/down
event and contain only phase and measurements.
