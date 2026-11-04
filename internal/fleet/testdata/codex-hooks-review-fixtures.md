The "Hooks need review" fixtures are terminal-render snapshots from OpenAI Codex
at tag `rust-v0.166.0`, the release the fleet runs (hk 2802):

- `codex-hooks-review-40col.txt`: `startup_hooks_review_prompt`, rendered at 40 columns,
  so the explanatory line and the third choice wrap.
- `codex-hooks-review-62col-trust-error.txt`: `startup_hooks_review_prompt_with_trust_error`,
  rendered at 62 columns, with the red "Failed to trust hooks" line.

Both come from
`codex-rs/tui/src/snapshots/codex_tui__startup_hooks_review__tests__<name>.snap`.
The insta metadata header was removed. The strings are built in
`codex-rs/tui/src/startup_hooks_review.rs` (`selection_view_params`):
https://github.com/openai/codex/blob/rust-v0.166.0/codex-rs/tui/src/startup_hooks_review.rs
(Apache-2.0, https://github.com/openai/codex/blob/main/LICENSE).

`codexHooksReviewPrompt` matches three phrases: `Hooks need review`,
`Trust all and continue` and `Continue without trusting`. Detection refuses with
`HOOKS_REVIEW_REQUIRED` (90) before brief injection. panewire sends no key to the
screen: hook trust is the host owner's decision.
