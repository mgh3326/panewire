package panewire

// Branch spec checkout, feature checkout_branch_v1 (hk 5118).
//
// An implementer spec spawn on refs/heads/<branch> asks the node for a local
// branch at the pinned commit instead of a detached HEAD, so a successor
// continues the pushed branch with a plain `git push`. The checkout travels as
// mode "successor"; the node answers checkout_mode "branch".
//
// The feature has two levels, as checkout_sparse_v1 does:
//
//   - The hub advertises checkout_branch_v1 in GET /v1/spawns/features, the
//     list a client reads before it sends the successor checkout. A hub that
//     does not list it refuses the mode, so the submitter states the cause
//     instead of reading a generic invalid_request.
//   - The hub names it in the once-per-connection hub.features frame too: a
//     node sends checkout_mode "branch" only to a hub that listed it, because
//     an older hub's strict result parser drops the whole answer for one
//     unknown value.
//   - A node from before the feature refuses the unknown mode as invalid_args,
//     so a request that reaches one is a visible refusal, never a detached
//     spawn that cannot push.
const hubCapabilityCheckoutBranch = "checkout_branch_v1"

// hubFeatureCheckoutBranch is the same name in the hub.features frame: the
// feature a hub lists once per connection so a node knows the hub's spawn
// result accepts checkout_mode "branch".
const hubFeatureCheckoutBranch = hubCapabilityCheckoutBranch
