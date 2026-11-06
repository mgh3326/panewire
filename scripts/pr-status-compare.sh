#!/usr/bin/env bash
# Compare `panewire pr status|findings` with gh for one or more PRs
# (docs/pr-status.md#comparing-against-gh). Prints a header and one
# tab-separated row per PR:
#
#   pr panewire_state gh_state state_agrees panewire_checks gh_checks checks_agree
#   panewire_findings gh_threads threads_agree panewire_ms gh_ms gh_requests panewire_source
#   panewire_origin hk_state hk_state_agrees hk_checks hk_checks_agree hk_findings
#   hk_threads_agree hk_ms
#
# The panewire columns are `panewire pr status|findings` as a worker runs them,
# so they measure whichever source answered (panewire_origin: hk, last_pass, ...).
# The hk columns read handoffkeep's `changes show` directly, with no node, hub or
# freshness rule between, so they isolate how accurate hk's record is against gh.
#
# Read-only. Needs jq, python3 (millisecond clock), gh logged in, and either the
# local node daemon (the default) or a hub connection in PANEWIRE_HUB_* environment.
# The hk columns also need the handoffkeep CLI and read "-" without it.
# PANEWIRE_BIN, GH_BIN and HK_BIN override the binaries.
#
# gh_requests is the rise in the GitHub core+graphql rate-limit counters over
# this script's own gh calls (reading the counters is free). panewire and hk are
# not counted: without --fresh neither reads GitHub.
set -euo pipefail

usage() {
  echo "usage: pr-status-compare.sh <owner/repo> <n> [<n>...]" >&2
  exit 2
}
[ "$#" -ge 2 ] || usage
repo=$1
shift
case "$repo" in */*) ;; *) usage ;; esac

panewire_bin=${PANEWIRE_BIN:-panewire}
gh_bin=${GH_BIN:-gh}
hk_bin=${HK_BIN:-handoffkeep}
for tool in jq python3 "$panewire_bin" "$gh_bin"; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "pr-status-compare: missing $tool" >&2
    exit 2
  }
done

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
requests_used() { "$gh_bin" api rate_limit --jq '[.resources.core.used, .resources.graphql.used] | add'; }

# bucket names from `gh pr checks --json bucket` onto mergewake's vocabulary.
gh_check_counts() {
  jq -r '[.[] | .bucket | if . == "pass" then "green" elif . == "fail" then "red" elif . == "pending" then "pending" elif . == "skipping" then "skipped" elif . == "cancel" then "cancelled" else . end]
         | group_by(.) | map("\(.[0]):\(length)") | sort | join(",")'
}
panewire_check_counts() {
  jq -r '[(.snapshot // .mergewake).checks[].state] | group_by(.) | map("\(.[0]):\(length)") | sort | join(",")'
}
# hk's record onto mergewake's vocabulary: checks of the current head (hk keeps
# earlier heads' rows) that are required or not yet known to be optional (`required` is null on a webhook write),
# state as the open, merged or closed class, open findings as unresolved threads.
hk_class() { jq -r '.change.state | if . == "merged" then "MERGED" elif . == "closed" then "CLOSED" else "OPEN" end'; }
hk_check_counts() {
  jq -r '.change.head_sha as $head | [.checks[] | select(.head_sha == $head and (.required != false) and (.gone | not)) | (.conclusion // .state | ascii_downcase)
          | if . == "success" then "green" elif . == "failure" or . == "error" or . == "timed_out" or . == "startup_failure" or . == "action_required" then "red" elif . == "cancelled" then "cancelled" elif . == "skipped" or . == "neutral" then "skipped" else "pending" end]
         | group_by(.) | map("\(.[0]):\(length)") | sort | join(",")'
}
hk_thread_count() { jq '[.threads[] | select(.resolved | not)] | length'; }

# gh before 2.50 has `pr checks --required` but no `--json`: read the second
# tab-separated column of its table, which is the same bucket word.
gh_required_buckets() {
  "$gh_bin" pr checks "$1" --repo "$repo" --required --json bucket 2>/dev/null && return 0
  { "$gh_bin" pr checks "$1" --repo "$repo" --required 2>/dev/null || true; } | awk -F'\t' 'NF >= 2 { print $2 }' |
    jq -R -s -c 'split("\n") | map(select(length > 0) | {bucket: .})'
}

owner=${repo%%/*}
name=${repo#*/}
threads_query='query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){reviewThreads(first:100){nodes{isResolved}}}}}'

printf 'pr\tpanewire_state\tgh_state\tstate_agrees\tpanewire_checks\tgh_checks\tchecks_agree\tpanewire_findings\tgh_threads\tthreads_agree\tpanewire_ms\tgh_ms\tgh_requests\tpanewire_source\tpanewire_origin\thk_state\thk_state_agrees\thk_checks\thk_checks_agree\thk_findings\thk_threads_agree\thk_ms\n'
for n in "$@"; do
  case "$n" in '' | *[!0-9]*) usage ;; esac

  start=$(now_ms)
  status_json=$("$panewire_bin" pr status "$repo" "$n" --json 2>/dev/null) || status_json='{}'
  findings_json=$("$panewire_bin" pr findings "$repo" "$n" --json 2>/dev/null) || findings_json='{}'
  panewire_ms=$(($(now_ms) - start))
  source=$(jq -r '.source // "none"' <<<"$status_json")

  used_before=$(requests_used)
  start=$(now_ms)
  view_json=$("$gh_bin" pr view "$n" --repo "$repo" --json state)
  checks_json=$(gh_required_buckets "$n")
  threads_json=$("$gh_bin" api graphql -f query="$threads_query" -f owner="$owner" -f name="$name" -F number="$n")
  gh_ms=$(($(now_ms) - start))
  gh_requests=$(($(requests_used) - used_before))

  gh_state=$(jq -r '.state' <<<"$view_json")
  gh_checks=$(gh_check_counts <<<"$checks_json")
  gh_threads=$(jq '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved | not)] | length' <<<"$threads_json")

  origin=$(jq -r '.snapshot.source // .mergewake.source // "-"' <<<"$status_json")

  # hk read directly; a missing CLI, a missing record or an error leaves "-".
  hk_state=- hk_checks=- hk_findings=- hk_state_agrees=n/a hk_checks_agree=n/a hk_threads_agree=n/a hk_ms=-
  if command -v "$hk_bin" >/dev/null 2>&1; then
    start=$(now_ms)
    hk_json=$("$hk_bin" changes show --repo "$repo" --pr "$n" 2>/dev/null) || hk_json=''
    hk_ms=$(($(now_ms) - start))
    if jq -e '.change.state' >/dev/null 2>&1 <<<"$hk_json"; then
      hk_state=$(hk_class <<<"$hk_json")
      hk_checks=$(hk_check_counts <<<"$hk_json")
      hk_findings=$(hk_thread_count <<<"$hk_json")
      hk_state_agrees=$([ "$hk_state" = "$gh_state" ] && echo yes || echo no)
      hk_checks_agree=$([ "$hk_checks" = "$gh_checks" ] && echo yes || echo no)
      hk_threads_agree=$([ "$hk_findings" = "$gh_threads" ] && echo yes || echo no)
    else
      hk_ms=-
    fi
  fi

  if [ "$source" = "panewire" ] && [ "$(jq -r '(.snapshot // .mergewake) != null' <<<"$status_json")" = "true" ]; then
    panewire_state=$(jq -r '(.snapshot // .mergewake).state' <<<"$status_json")
    class=$(case "$panewire_state" in merged) echo MERGED ;; closed) echo CLOSED ;; *) echo OPEN ;; esac)
    panewire_checks=$(panewire_check_counts <<<"$status_json")
    panewire_findings=$(jq '(.findings | length) + (.findings_omitted // 0)' <<<"$findings_json")
    state_agrees=$([ "$class" = "$gh_state" ] && echo yes || echo no)
    checks_agree=$([ "$panewire_checks" = "$gh_checks" ] && echo yes || echo no)
    threads_agree=$([ "$panewire_findings" = "$gh_threads" ] && echo yes || echo no)
  else
    # No mergewake snapshot answered (gh fallback, hk only, or nothing): there
    # is no panewire view to compare, and the row says so.
    panewire_state=- panewire_checks=- panewire_findings=-
    state_agrees=n/a checks_agree=n/a threads_agree=n/a
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$n" "$panewire_state" "$gh_state" "$state_agrees" "${panewire_checks:-}" "${gh_checks:-}" "$checks_agree" \
    "$panewire_findings" "$gh_threads" "$threads_agree" "$panewire_ms" "$gh_ms" "$gh_requests" "$source" "$origin" \
    "$hk_state" "$hk_state_agrees" "${hk_checks:-}" "$hk_checks_agree" "$hk_findings" "$hk_threads_agree" "$hk_ms"
done
