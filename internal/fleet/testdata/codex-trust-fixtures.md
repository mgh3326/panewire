The trust dialog fixtures are terminal-render snapshots from OpenAI Codex:

- `codex-trust-current.txt`: commit `c9d13e8c757cd330a572e9a39e365866069aef07`.
- `codex-trust-legacy.txt`: tag `rust-v0.120.0`.

Both use the upstream path
`codex-rs/tui/src/onboarding/snapshots/codex_tui__onboarding__trust_directory__tests__renders_snapshot_for_git_repo.snap`.
The insta metadata header was removed and trailing screen padding was trimmed. Source:
https://github.com/openai/codex/blob/c9d13e8c757cd330a572e9a39e365866069aef07/codex-rs/tui/src/onboarding/trust_directory.rs
(Apache-2.0, https://github.com/openai/codex/blob/main/LICENSE).

These are upstream rendered fixtures, not a capture of an incident host.
The current local CLI did not reproduce the trust dialog in a temporary directory.
`TestCodexTrustPromptFixtures` requires both the dialog question and its choices;
an idle composer alone is not a trust prompt. Detection refuses with
`WORKSPACE_TRUST_REQUIRED` (90), before brief injection, and captures/cleans up
the failed pane using the normal spawn failure path. Runtug already seeds trust
for the selected worktree, but an existing explicit project setting is preserved.
No Enter is sent to accept a trust dialog automatically.
