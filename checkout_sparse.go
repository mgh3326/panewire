package panewire

import (
	"context"
	"time"
)

// Sparse checkout, feature checkout_sparse_v1 (hk 5095, hk 5098).
//
// A spec spawn may name the paths to leave out of its worktree and the changed
// paths to keep in it. The lists travel on the checkout spec (exclude[],
// include[]); the node builds the gitignore pattern list itself and applies it
// with `git sparse-checkout set --no-cone --stdin`. The feature has two
// levels, as close_ops_v1 does:
//
//   - The hub advertises checkout_sparse_v1 in GET /v1/spawns/features, the
//     list a client such as mergewake reads before it sends the fields. A hub
//     that does not list it drops the fields, so a submitter refuses to send a
//     sparse request there.
//   - A node advertises it in its hello only while its git is 2.37 or newer,
//     the release whose `sparse-checkout set --no-cone` the node uses. The hub
//     sends exclude/include only to a node that advertises it (a strict older
//     decoder would refuse the frame); every other node prepares the full
//     checkout, a superset that is never wrong.
//   - The spawn answer and the job.spawned record carry checkout_mode
//     ("sparse" or "full"), so a reader can tell that the absent paths are
//     absent on purpose.
const hubCapabilityCheckoutSparse = "checkout_sparse_v1"

// hubFeatureCheckoutSparse is the same name in the hub.features frame: the
// feature a hub lists once per connection so a node knows the hub's spawn
// result accepts checkout_mode. An older hub's result parser rejects the
// unknown key and drops the whole answer, so the node sends the field only to
// a hub that named it (hk 5098 review).
const hubFeatureCheckoutSparse = hubCapabilityCheckoutSparse

// checkoutSparseGitVersion is the git release the node's sparse checkout
// needs. 2.37 made cone mode the default for `sparse-checkout set`; the node
// always passes --no-cone, so the floor is the release that ships the flag.
const (
	checkoutSparseGitMajor = 2
	checkoutSparseGitMinor = 37
)

// checkoutSparseProbeBound bounds the git --version probe a hello runs. A git
// that does not answer in time is treated as too old: the node then prepares
// full checkouts, which is always correct.
const checkoutSparseProbeBound = 5 * time.Second

// checkoutSparseReady reports whether this node may advertise checkout_sparse_v1
// now: its git is new enough to run the sparse commands.
func (client *HubClient) checkoutSparseReady(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, checkoutSparseProbeBound)
	defer cancel()
	ok, err := nodeGitVersionAtLeast(probeCtx, client.nodeGitRunner(), checkoutSparseGitMajor, checkoutSparseGitMinor)
	return err == nil && ok
}

// sparseCheckoutForNode returns the checkout the node may receive. A node that
// does not advertise checkout_sparse_v1 never sees exclude/include: its strict
// decoder would refuse them, and without them it prepares the full checkout.
// The request keeps the lists, so a replay to a capable node still carries
// them.
func sparseCheckoutForNode(checkout *hubSpawnCheckout, capabilities map[string]bool) *hubSpawnCheckout {
	if checkout == nil || capabilities[hubCapabilityCheckoutSparse] {
		return checkout
	}
	if len(checkout.Exclude) == 0 && len(checkout.Include) == 0 {
		return checkout
	}
	copied := *checkout
	copied.Exclude, copied.Include = nil, nil
	return &copied
}
