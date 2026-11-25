# Hub log access on oci

The hub runs on host `oci` as the systemd unit `panewire-hub.service`
(user `opc`). It logs to stderr, which lands in the unit's journal. Read the
hub's logs on oci with:

```sh
journalctl -u panewire-hub
```

This needs the host's journal read rights. Without them `journalctl` shows
no entries for the unit. This page does not record how those rights are
granted on oci; ask the operator for them.

The hub's `--log-file` flag, which writes a group-readable copy of the log,
is described in [docs/hub-r6.md](../hub-r6.md); its `--log-file` paragraph
carries the operating constraints (the hub service user must own the file,
and log rotation must use `copytruncate` or a unit restart).

History: this page used to describe reading the hub's logs on the former
hub host ncp, which was returned on 2026-11-24. Those NCP-specific steps are
not restated here.
