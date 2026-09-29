# Hub log access on NCP — reading hub logs without root

The hub logs to stderr, which under systemd lands in the journal. On the NCP
host the journal is root-only by default, so an operator who is not root sees
nothing — including the replay and lane-stall lines the hub emits to explain
what it re-injected after a restart. There are two desk-side ways to give a
non-root operator read access; both are desk actions, not repo changes.

## Option A — journald group membership

Add the operator's user to the `systemd-journal` group:

```sh
sudo usermod -aG systemd-journal <user>
# the user must log out and back in for the group to apply
journalctl -u panewire-hub.service
```

This is the least moving parts: no new file, no rotation to manage, and the
hub unit stays unchanged. It also grants the user read access to **every**
unit's journal on the host, not just the hub's — on a single-purpose NCP hub
host that is usually the intent anyway.

## Option B — `--log-file` on the hub unit

Point the hub at a group-readable file. The flag appends to (or creates) the
file and forces mode `0640` on every startup, so the file is group-readable
and never world-readable even if it was created permissively before.

1. Create the directory and group-owned file:

   ```sh
   sudo install -d -o root -g <operator-group> -m 0750 /var/log/panewire
   sudo install -o root -g <operator-group> -m 0640 /dev/null /var/log/panewire/hub.log
   ```

2. Add `--log-file /var/log/panewire/hub.log` to the `ExecStart` of
   `panewire-hub.service` and restart the unit (a normal desk deploy
   restart).
3. The operator, a member of `<operator-group>`, reads
   `/var/log/panewire/hub.log` directly.

Notes:

- The hub forces `0640` on each start, so a manual `chmod` or a permissive
  pre-existing file does not silently widen access; it also means a stray
  `umask` can never make the file world-readable.
- Stderr/journald output is unchanged byte-for-byte — the file is a copy,
  not a redirect, so journald remains the authority for timestamps and
  unit metadata.
- The hub logger never writes token or secret values; the file therefore
  contains none. It can still be handed to an operator as-is.
- If the path is unwritable the hub refuses to start rather than dropping
  the copy silently — check `journalctl -u panewire-hub.service` (as root or
  via option A) for the rejection line.
- logrotate or equivalent is a desk concern; the hub only ever appends.
