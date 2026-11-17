# Triage patterns

Learned skip patterns for review findings on this repository (see the
`fleet-review` method). A finding that matches a pattern below is dismissed
with a pointer to the pattern; a finding that falls outside a pattern's
scope is triaged normally.

## Old-binary watcher across a node crash or restart (I8/I9)

**Matches:** a finding that an older `panewire` watcher, one that ignores
`job.hub_close_intent` or does not hold the v3 watcher lock, can run for a job
with an open hub-close intent after the node crashes or restarts (the kernel
lock on `.panewire-watch.lock` is released), and so re-prompt or end the job
during a close. PR #662 (hk 4632) round 7 lists 16 such outside-diff ids.

**Disposition: dismiss.**

- **Precondition.** No older binary executes on a host after the close-ops
  barrier. This is the design decision recorded as the panewire-design
  verdict on consult 4632-4b1a6a8e
  (https://github.com/mgh3326/panewire/pull/662#issuecomment-6938699662).
  A crash-surviving on-disk fence cannot exclude an older binary, because
  that binary reads no such fence.
- **Unreachable in #662 (I9).** An intent exists only after the hub sends
  `pane.close_op`, and no hub code in #662 sends it: `authorizeNodeCommand`
  (`hub_node_command.go`) forbids `pane.close_op`, `pane.close_status` and
  `pane.close_release` on the public node-command endpoint, and there is no
  issuer. See consult `evidence/2026-11-17/pw-4632-r7-consult/report.md`.
- **Gate moved.** The crash window is a rollout risk of hk 4745's hub issuer:
  `node close-ops advertise` runs only after the hub guard (I1/I2,
  `docs/node-commands.md` "hub issuer obligations") is deployed.
- **Bounded.** If the precondition is broken anyway, settlement keeps another
  terminal record ("kept") and never writes over one.

**Re-open when:** hub code starts issuing `pane.close_op`, or a finding shows
a violation that does not need an older binary to run after the barrier.
