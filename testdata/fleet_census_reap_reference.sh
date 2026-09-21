#!/usr/bin/env bash
# Reference oracle for the fleet-census reap-equivalence test.
#
# Everything between the ===== markers is vendored VERBATIM from
# agent-skills commit ae1f544 (bin/wrk, PR #508): parse_duration_s,
# reap_candidates, reap_tab_pane_counts, reap_tab_verdict and
# herdr_agent_probe are copied unmodified; reference_reap is the dry-run
# half of reap_cmd with the --apply branch deleted and --lane fixed empty.
# This script can only print — it has no close/apply path and writes no
# files — which makes it safe as the test oracle.
#
# Usage: fleet_census_reap_reference.sh GRACE_SECONDS
#   env: ARBITER_INBOX_ROOT  fixture jobs root (jobs/<id>/events/*.json)
#        HERDR_BIN           fixture herdr stub (agent get / tab list)
set -euo pipefail

HERDR="${HERDR_BIN:?HERDR_BIN must name the fixture herdr stub}"
SENTINEL_GONE_CODES="agent_not_found pane_not_found tab_not_found terminal_not_found no_such_agent no_such_pane"
wrk_jobs_root() { printf '%s\n' "${ARBITER_INBOX_ROOT:?ARBITER_INBOX_ROOT must name the fixture jobs root}"; }

# ===== vendored from ae1f544 bin/wrk =====
parse_duration_s() {
  local value="$1"
  python3 - "$value" <<'PY'
import re, sys
text = sys.argv[1].strip()
match = re.fullmatch(r"(\d+)([smhd]?)", text)
if not match:
    raise SystemExit(1)
scale = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
print(int(match.group(1)) * scale)
PY
}

reap_candidates() {
  local root="$1" lane="$2" grace="$3" include_builders="$4"
  python3 - "$root" "$lane" "$grace" "$include_builders" <<'PY'
import datetime, json, os, sys, time

root, lane, grace, include_builders = sys.argv[1:5]
grace = int(grace)
include_builders = include_builders == "1"
TERMINAL = {"job.completed", "job.joined", "job.revoked"}
# 종료 뒤에 이 중 하나가 오면 그 잡은 다시 살아난 것이다(완료 → 재클레임 → 작업 중
# 잠깐 idle). 상태 검사(idle|done)는 긴 턴 중 오표시가 실측돼 있어 마지막 방어선이
# 못 된다 — 판정은 인박스 순서로 한다. job.lost·quota_pool.* 는 되살림이 아니다
# (넣으면 completed → lost 인 정상 종결 잡이 영구히 빠진다). job.reprompted 는 아직
# 없는 종류지만(#113 ①) 생기면 같은 뜻이다.
REVIVE = {"job.claim", "job.reclaim", "job.spawned", "job.reprompted"}
now = time.time()


def moment_of(value, fallback):
    if not isinstance(value, str) or not value.strip():
        return fallback
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.timestamp()


try:
    jobs = sorted(os.listdir(root))
except OSError:
    raise SystemExit(0)

for job in jobs:
    events_dir = os.path.join(root, job, "events")
    if not os.path.isdir(events_dir):
        continue
    owner = pane = tab = role = ""
    spawned_pane = spawned_tab = ""
    saw_spawn = False
    terminal_at = None
    revived = False
    reaped = False
    try:
        names = sorted(os.listdir(events_dir))
    except OSError:
        continue
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(events_dir, name)
        try:
            with open(path, encoding="utf-8") as handle:
                document = json.load(handle)
        except (OSError, ValueError):
            continue
        if not isinstance(document, dict):
            continue
        payload = document.get("payload", document)
        if not isinstance(payload, dict):
            payload = {}
        kind = document.get("kind")
        if kind in ("job.claim", "job.reclaim"):
            # 소유는 최신 claim 이 정한다(released 된 잡을 다른 레인이 다시 잡는다).
            owner = payload.get("owner_lane") if isinstance(payload.get("owner_lane"), str) else ""
            # 역할은 누적이다: `--role` 없는 reclaim 은 기본값 worker 를 payload 에
            # 남기므로, 덮어쓰면 재클레임 한 번에 빌더 표식이 지워진다. 디스크에
            # 남은 legacy captain claim도 동일한 builder 급 pane으로 보호한다.
            if payload.get("role") in ("builder", "captain"):
                role = "builder"
        for key in ("pane_id", "tab_id"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                if key == "pane_id":
                    pane = value
                else:
                    tab = value
        if kind == "job.spawned":
            # 가장 늦은 스폰 영수증이 pane·tab 을 **한 벌로** 정한다. 필드별로 마지막
            # 값을 남기면 재스폰 영수증(tab_id 없음)의 pane 과 이전 스폰의 tab 이 섞여
            # 엉뚱한 탭을 닫는다(#508 tester 실증: pane=w1:p9 인데 tab close w1:t1).
            saw_spawn = True
            spawned_pane = payload.get("pane_id") if isinstance(payload.get("pane_id"), str) else ""
            spawned_tab = payload.get("tab_id") if isinstance(payload.get("tab_id"), str) else ""
        if kind == "job.reaped":
            reaped = True
        if kind in REVIVE and terminal_at is not None:
            terminal_at = None
            revived = True
        if kind in TERMINAL:
            revived = False
            try:
                fallback = os.path.getmtime(path)
            except OSError:
                fallback = now
            terminal_at = moment_of(document.get("created_at") or payload.get("at"), fallback)
    # 스폰 영수증이 pane/tab 의 정본이다. 완료 이벤트의 pane_id 를 그대로 믿으면
    # be4dad1 이전 `wrk done` 이 남긴 오염 레코드("w1:p1\t\tworker")를 집는다 —
    # 실 인박스에 23건 남아 있다.
    # 영수증이 하나라도 있으면 가장 늦은 영수증만 믿는다 — 그 영수증이 깨졌으면 앞선
    # 영수증이나 완료 이벤트의 값으로 되돌아가지 않고 malformed 로 건너뛴다.
    malformed_receipt = False
    if saw_spawn:
        pane, tab = spawned_pane, spawned_tab
        malformed_receipt = not pane
    if reaped or (not pane and not malformed_receipt):
        continue
    if lane and owner != lane:
        continue
    if role == "builder" and not include_builders:
        continue
    if terminal_at is None:
        # 종료 뒤에 되살아난 잡은 이유를 남기고 건너뛴다. 종료가 아예 없던 잡은
        # 지금처럼 출력하지 않는다.
        if revived and not any(c.isspace() for c in job):
            print(job, "-", "-", "-", "-", "reclaimed-after-terminal", sep="\t")
        continue
    age = int(now - terminal_at)
    if age < grace:
        continue
    # 어떤 필드에도 공백이 있어서는 안 된다. 있으면 그 잡의 증거가 깨진 것이고,
    # 회수는 추측하지 않는다 — pane 을 자리표시자로 내보내 skip 으로 남긴다.
    if malformed_receipt or any(any(c.isspace() for c in value) for value in (job, owner, pane, tab)):
        print(job.split()[0] if job.split() else job, "-", "-", "-", age, "-", sep="\t")
        continue
    print(job, owner or "-", pane, tab or "-", age, "-", sep="\t")
PY
}

reap_tab_pane_counts() {
  local out
  out="$("$HERDR" tab list 2>/dev/null)" || return 0
  python3 -c '
import json, sys
try:
    tabs = json.loads(sys.argv[1])["result"]["tabs"]
except (ValueError, KeyError, TypeError):
    raise SystemExit(0)
if not isinstance(tabs, list):
    raise SystemExit(0)
for tab in tabs:
    if isinstance(tab, dict) and isinstance(tab.get("tab_id"), str):
        count = tab.get("pane_count")
        ok = isinstance(count, int) and not isinstance(count, bool)
        print(tab["tab_id"], count if ok else "?", sep="\t")
' "$out" 2>/dev/null || true
}

reap_tab_verdict() {
  local tab="$1" counts="$2" rows
  rows="$(awk -v want="$tab" -F'\t' '$1==want{print $2}' <<<"$counts")"
  if [[ -z "$rows" || "$rows" == *$'\n'* || ! "$rows" =~ ^[0-9]+$ ]]; then
    echo unknown
  elif [[ "$rows" == 1 ]]; then
    echo single
  elif (( 10#$rows > 1 )); then
    echo "shared:$((10#$rows))"
  else
    echo unknown
  fi
}

herdr_agent_probe() {
  local pane="$1" out="" rc=0
  set +e
  # 🔴 두 스트림을 함께 읽는다. herdr 는 에러 봉투를 **stderr** 로 내보내고 1 로
  # 끝낸다(2026-09-05 실측: `{"error":{"code":"agent_not_found",…}}` 가 stderr).
  # stdout 만 보면 확정 소멸이 영원히 "빈 값" 으로 보인다.
  out="$("$HERDR" agent get "$pane" 2>&1)"
  rc=$?
  set -e
  python3 - "$rc" "$SENTINEL_GONE_CODES" "$out" <<'PY' 2>/dev/null || printf 'transient\terr:probe\t\n'
import json, sys

rc, gone_codes, raw = sys.argv[1], set(sys.argv[2].split()), sys.argv[3].strip()


def emit(kind, token, tab=""):
    print(kind, token, tab, sep="\t")
    raise SystemExit(0)


def documents(text):
    """봉투가 로그 줄과 섞여 와도 JSON 만 골라낸다."""
    try:
        yield json.loads(text)
        return
    except ValueError:
        pass
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except ValueError:
            continue


def status_and_tab(document):
    # 실 herdr: {"result":{"agent":{"agent_status":…,"tab_id":…}}}.
    # 옛 응답(result 바로 아래)과 agent_info 변형도 함께 받는다.
    result = document.get("result", document)
    if not isinstance(result, dict):
        return "", ""
    scopes = [result]
    for key in ("agent", "agent_info", "pane"):
        value = result.get(key)
        if isinstance(value, dict):
            scopes.append(value)
    for scope in scopes:
        status = scope.get("agent_status")
        if isinstance(status, str) and status.strip():
            tab = scope.get("tab_id") or result.get("tab_id") or ""
            return status.strip(), tab if isinstance(tab, str) else ""
    return "", ""


if not raw:
    emit("transient", "empty" if rc == "0" else "err:exit%s" % rc)
for document in documents(raw):
    if not isinstance(document, dict):
        continue
    error = document.get("error")
    if isinstance(error, str):
        error = {"code": error}
    if isinstance(error, dict):
        code = error.get("code") or error.get("message") or "unknown"
        code = "_".join(str(code).split())[:64]
        emit("gone" if code in gone_codes else "transient", "err:%s" % code)
    status, tab = status_and_tab(document)
    if status:
        emit("ok", status, tab)
emit("transient", "err:parse")
PY
}

# ===== end vendored =====

# Dry-run half of reap_cmd: same loop, same skip reasons, apply removed.
reference_reap() {
  local grace_s="$1" candidates
  candidates="$(reap_candidates "$(wrk_jobs_root)" "" "$grace_s" "0")"
  local tab_counts
  local job owner pane tab age note probe class token probe_tab verdict acted=0
  while IFS=$'\t' read -r job owner pane tab age note; do
    [[ -n "$job" ]] || continue
    if [[ -n "$note" && "$note" != - ]]; then
      echo "skip job=$job reason=$note"
      continue
    fi
    [[ "$owner" != - ]] || owner=""
    [[ "$tab" != - ]] || tab=""
    if [[ "$pane" == - ]]; then
      echo "skip job=$job reason=malformed-record"
      continue
    fi
    probe="$(herdr_agent_probe "$pane")"
    IFS=$'\t' read -r class token probe_tab <<<"$probe"
    if [[ "$class" != ok ]]; then
      echo "skip job=$job pane=$pane reason=pane-unresolved($token)"
      continue
    fi
    case "$token" in
      idle|done) ;;
      *) echo "skip job=$job pane=$pane reason=status=$token"; continue ;;
    esac
    [[ -n "$tab" ]] || tab="$probe_tab"
    if [[ -z "$tab" ]]; then
      echo "skip job=$job pane=$pane reason=tab-unresolved"
      continue
    fi
    if [[ -n "$probe_tab" && "$probe_tab" != "$tab" ]]; then
      echo "skip job=$job pane=$pane tab=$tab reason=tab-mismatch(pane-in=$probe_tab)"
      continue
    fi
    tab_counts="$(reap_tab_pane_counts)"
    verdict="$(reap_tab_verdict "$tab" "$tab_counts")"
    case "$verdict" in
      single) ;;
      shared:*)
        echo "skip job=$job pane=$pane tab=$tab reason=tab-shared(panes=${verdict#shared:})"
        continue ;;
      *)
        echo "skip job=$job pane=$pane tab=$tab reason=tab-count-unknown"
        continue ;;
    esac
    echo "would-close job=$job pane=$pane tab=$tab status=$token age=${age}s"
    acted=$(( acted + 1 ))
  done <<<"$candidates"
  echo "reap: $acted candidate(s) (dry-run; re-run with --apply to close)"
}

reference_reap "$1"
