"""Builds panewire's admission golden scenarios (inputs only); the pacelever
side is filled in by running pacelever's own gate over them."""
import json, sys
NOW = "2026-11-07T03:00:00Z"
def t(h=0, d=0, m=0):
    from datetime import datetime, timedelta, timezone
    base = datetime(2026, 11, 7, 3, 0, 0, tzinfo=timezone.utc)
    return (base + timedelta(hours=h, days=d, minutes=m)).strftime("%Y-%m-%dT%H:%M:%SZ")
def row(profile, effort, grade, pool, gate="default", model="m"):
    return {"profile": profile, "effort": effort, "model_id": model, "pool": pool, "grade": grade, "gate": gate}
CATALOG = [
    row("codex-sol", "xhigh", "S+", "codex", "escalation", "gpt-6.1-sol"),
    row("codex-sol", "max", "S+", "codex", model="gpt-6.1-sol"),
    row("codex-luna", "low", "C", "codex", model="gpt-5.6-luna"),
    row("codex-luna", "medium", "B", "codex", model="gpt-6-luna"),
    row("codex-luna", "high", "A", "codex", model="gpt-6-luna"),
    row("codex-luna", "xhigh", "A+", "codex", model="gpt-6-luna"),
    row("codex-luna-max", "", "A+", "codex", model="gpt-6-luna"),
    row("codex-astra", "", "S+", "codex", "consult_only", "gpt-6-astra"),
    row("devin-fusion-sol61", "", "B", "codex", model="fusion-gpt-6-1-sol-high-sidekick-swe-2-medium"),
    row("codex-terra61", "max", "C", "codex", model="gpt-6.1-terra"),
    row("opus", "low", "S", "claude", model="claude-opus-5-5"),
    row("opus", "medium", "S+", "claude", model="claude-opus-5-5"),
    row("opus", "high", "S+", "claude", model="claude-opus-5-5"),
    row("opus", "xhigh", "S+", "claude", model="claude-opus-5-5"),
    row("opus", "max", "S+", "claude", "escalation", "claude-opus-5-5"),
    row("sonnet", "high", "A", "claude", model="claude-sonnet-5-5"),
    row("sonnet", "xhigh", "A+", "claude", "escalation", "claude-sonnet-5-5"),
    row("haiku", "low", "C", "claude", model="claude-haiku-5-5"),
    row("fable", "", "S+", "claude", "consult_only", "claude-fable-5-1"),
    row("cmd-dsflash41", "high", "A+", "commandcode", model="deepseek/deepseek-v4.1-flash"),
    row("devin-swe2", "", "A+", "devin", model="swe-2"),
    row("devin-swe2", "high", "A+", "devin", model="swe-2"),
    row("devin-ds41", "", "A+", "devin", model="deepseek-v4-1-flash-high"),
    row("devin-swe2-medium", "", "A", "devin", model="swe-2-medium"),
    row("pi-sonnet55", "high", "A", "anthropic-api", model="anthropic/claude-sonnet-5-5"),
    row("pi-sonnet55", "xhigh", "A+", "anthropic-api", model="anthropic/claude-sonnet-5-5"),
    row("pi-sonnet55", "max", "A+", "anthropic-api", "consult_only", "anthropic/claude-sonnet-5-5"),
    row("grok", "medium", "A+", "grok", model="grok-4.7"),
    row("agy-flash", "", "A+", "agy", model="gemini-3.7-flash"),
]
FP = "0123456789abcdef"
CONFIG = '[pools.grok]\nclass = "exclude"\n'
def b(window, used, reset=None, scope="account", model=None):
    x = {"window": window, "used_pct": used, "scope": scope}
    if reset: x["resets_at"] = reset
    if model: x["model"] = model
    return x
def auto(buckets, plan="", measured=None, free=None):
    o = {"id": 7, "kind": "auto", "measured_at": measured or t(m=-1), "buckets": buckets}
    if plan: o["plan"] = plan
    if free: o["free_bucket"] = free
    return o
def manual(window, used, id=41, expires=None, reset=None):
    bk = b(window, used, reset)
    return {"id": id, "kind": "manual", "window": window, "measured_at": t(h=-1), "expires_at": expires or t(d=2), "buckets": [bk]}
CLAUDE_OK = [b("5h", 20, t(h=3)), b("7d", 40, t(d=5))]
S = []
def sc(name, req, latest, config=CONFIG, note=None, native=None):
    pool = req.pop("pool")
    req.setdefault("role", "implementer")
    req["account_key"] = req.get("account_key", pool + ":" + FP)
    x = {"name": name, "now": NOW, "request": req, "config": config, "catalog": CATALOG, "latest": latest}
    if note: x["disagreement"] = note
    if native: x["native"] = native
    S.append(x)
# claude: cutoffs, policy class, staleness, windows
sc("claude-admit", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK)})
sc("claude-5h-exhausted", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 99.5, t(h=3)), b("7d", 40, t(d=5))])})
sc("claude-preserve-cutoff-90", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 10, t(h=3)), b("7d", 91, t(d=5))])}, CONFIG + '[pools.claude]\nclass = "preserve"\nuntil = 2026-12-01\n')
sc("claude-preserve-until-passed", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 10, t(h=3)), b("7d", 91, t(d=5))])}, CONFIG + '[pools.claude]\nclass = "preserve"\nuntil = "2026-11-01"\n')
sc("claude-configured-cutoff", {"pool": "claude", "profile": "sonnet", "effort": "high"}, {"observation": auto([b("5h", 85, t(h=3)), b("7d", 40, t(d=5))])}, CONFIG + '[pools.claude]\ncutoff = 80\n')
sc("claude-stale-reading", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK, measured=t(m=-20))})
sc("claude-fresh-within-15m", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK, measured=t(m=-14))})
sc("claude-confirmed-at-refreshes", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": dict(auto(CLAUDE_OK, measured=t(h=-2)), confirmed_at=t(m=-2))})
sc("claude-malformed-scope", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 20, t(h=3), scope="weekly")])})
sc("claude-no-reading", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": None})
sc("claude-reset-passed", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 20, t(m=-5)), b("7d", 40, t(d=5))])})
sc("claude-missing-5h-admits", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("7d", 40, t(d=5))])})
sc("claude-model-windows-do-not-gate", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("7d", 99.9, t(d=5), scope="model", model="opus")])})
sc("claude-pace-warning", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto([b("5h", 10, t(h=3)), b("7d", 60, t(d=3, h=12))])})
sc("claude-unsubscribed-pool", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK)}, CONFIG + '[pools.claude]\nsubscribed = false\n')
sc("claude-profile-subscription-wins", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK)}, CONFIG + '[pools.claude]\nsubscribed = false\n[profiles.opus]\nsubscribed = true\n')
sc("claude-inflight-reserve-unknown", {"pool": "claude", "profile": "opus", "effort": "high"}, {"observation": auto(CLAUDE_OK)}, CONFIG + '[pools.claude.inflight_reserve_pct]\n5h = 5\n')
sc("claude-escalation-operator-override", {"pool": "claude", "profile": "sonnet", "effort": "xhigh", "operator_request": "hk:task/3248", "requested_by": "desk"}, {"observation": auto(CLAUDE_OK)})
sc("claude-escalation-default-request-refused", {"pool": "claude", "profile": "opus", "effort": "high", "operator_request": "hk:task/3248"}, {"observation": auto(CLAUDE_OK)})
sc("requested-by-needs-operator-request", {"pool": "claude", "profile": "opus", "effort": "high", "requested_by": "desk"}, {"observation": auto(CLAUDE_OK)})
sc("operator-request-ref-invalid", {"pool": "claude", "profile": "sonnet", "effort": "xhigh", "operator_request": "task 12"}, {"observation": auto(CLAUDE_OK)})
sc("fable-implementer-consult-gate", {"pool": "claude", "profile": "fable"}, {"observation": auto(CLAUDE_OK)},
   note="Operator decision (hk 4212): fable holds implementer seats like other profiles; panewire no longer refuses the role. The recorded catalog row is consult_only, so the gate refuses on that row's rule (no --operator-request). pacelever v0.3.28 still refuses the role.",
   native={"ok": False, "exit_code": 3, "provider_id": "claude", "reason": "consult_only: pass --operator-request"})
sc("fable-builder-consult-gate", {"pool": "claude", "profile": "fable", "role": "builder"}, {"observation": auto(CLAUDE_OK)},
   note="Operator decision (hk 4212): fable holds builder seats like other profiles; panewire no longer refuses the role. The recorded catalog row is consult_only, so the gate refuses on that row's rule (no --operator-request). pacelever v0.3.28 still refuses the role.",
   native={"ok": False, "exit_code": 3, "provider_id": "claude", "reason": "consult_only: pass --operator-request"})
sc("fable-consult-only-needs-request", {"pool": "claude", "profile": "fable", "role": "lead"}, {"observation": auto(CLAUDE_OK)})
sc("codex-astra-lead-without-purpose", {"pool": "codex", "profile": "codex-astra", "role": "lead"}, {"observation": auto([b("5h", 10, t(h=3)), b("7d", 10, t(d=5))], plan="pro")},
   note="Operator decision (hk 4212): codex-astra's purpose limit is removed; panewire admits a lead request without --purpose. pacelever v0.3.28 still refuses it.",
   native={"ok": True, "exit_code": 0, "provider_id": "codex", "reason": "codex-astra pool=codex used=10% class=spend"})
sc("codex-astra-director-admit", {"pool": "codex", "profile": "codex-astra", "role": "lead", "purpose": "director"}, {"observation": auto([b("5h", 10, t(h=3)), b("7d", 10, t(d=5))], plan="pro")},
   note="Same verdict; only the reason's rung suffix differs. pacelever names the effortless rung after its bundled default effort (codex-astra@xhigh); the bench catalog JSON carries no defaults, so panewire's reason names no rung. The admitted row is the same.",
   native={"ok": True, "exit_code": 0, "provider_id": "codex", "reason": "codex-astra pool=codex used=10% class=spend"})
# codex required windows (low plans)
sc("codex-plus-missing-5h", {"pool": "codex", "profile": "codex-sol", "effort": "max"}, {"observation": auto([b("7d", 30, t(d=5))], plan="plus")})
sc("codex-prolite-7d-only-admits", {"pool": "codex", "profile": "codex-sol", "effort": "max"}, {"observation": auto([b("7d", 30, t(d=5))], plan="prolite")})
sc("codex-free-missing-30d", {"pool": "codex", "profile": "codex-luna", "effort": "low"}, {"observation": auto([b("5h", 30, t(h=2))], plan="free")})
sc("codex-pro-missing-5h-admits", {"pool": "codex", "profile": "codex-sol", "effort": "max"}, {"observation": auto([b("7d", 30, t(d=5))], plan="pro")})
sc("codex-7d-exhausted-alternatives", {"pool": "codex", "profile": "codex-luna", "effort": "xhigh"}, {"observation": auto([b("5h", 10, t(h=2)), b("7d", 99, t(d=5))], plan="pro")})
sc("fusion-billing-fp-matches", {"pool": "codex", "profile": "devin-fusion-sol61"}, {"observation": auto([b("5h", 10, t(h=2)), b("7d", 20, t(d=5))], plan="pro")}, CONFIG + '[pools.codex]\nfusion_billing_fp = "%s"\n' % FP)
sc("fusion-billing-fp-differs", {"pool": "codex", "profile": "devin-fusion-sol61"}, {"observation": auto([b("5h", 10, t(h=2)), b("7d", 20, t(d=5))], plan="pro")}, CONFIG + '[pools.codex]\nfusion_billing_fp = "fedcba9876543210"\n')
sc("codex-terra61-catalog-placement", {"pool": "codex", "profile": "codex-terra61", "effort": "max"}, {"observation": auto([b("5h", 10, t(h=2)), b("7d", 20, t(d=5))], plan="pro")},
   note="pacelever's catalog decoder keeps its bundled snapshot's placed=false for codex-terra61 (an E6 measurement rung); the bench catalog JSON panewire reads carries no placed flag, so a graded rung reads as placed. Catalog placement moves with hk 3249; until then `pacelever policy launch`, which stays in the spawn path before the gate, refuses the rung with the same e6_arm_required.",
   native={"ok": True, "exit_code": 0, "provider_id": "codex", "reason": "codex-terra61 pool=codex used=20% class=spend [rung codex-terra61@max]"})
# commandcode 30d
CC_OK = [b("5h", 10, t(h=2)), b("7d", 20, t(d=5)), b("30d", 30, t(d=20))]
sc("commandcode-30d-present-admits", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "high"}, {"observation": auto(CC_OK)})
sc("commandcode-30d-missing", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "high"}, {"observation": auto(CC_OK[:2])},
   note="New rule (decision/2026-11-06/commandcode-30d-required-in-panewire-gate): a commandcode reading without its 30d window is unmeasurable in panewire's gate. pacelever admits it.",
   native={"ok": False, "exit_code": 4, "provider_id": "commandcode", "reason": "commandcode_required_window_missing:30d", "missing_windows": ["30d"]})
sc("commandcode-30d-exhausted", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "high"}, {"observation": auto([b("5h", 10, t(h=2)), b("7d", 20, t(d=5)), b("30d", 100, t(d=20))])})
sc("cmd-dsflash41-reviewer-denied", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "high", "role": "reviewer"}, {"observation": auto(CC_OK)})
sc("cmd-dsflash41-builder-denied", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "high", "role": "builder"}, {"observation": auto(CC_OK)},
   note="Operator decision 2026-11-07 (hk 3248 comment 11914): cmd-dsflash41 seats only an implementer. pacelever v0.3.28 also seats a builder on a placed default rung.",
   native={"ok": False, "exit_code": 5, "provider_id": "commandcode", "reason": "role_restricted: cmd-dsflash41 seats only an implementer"})
sc("cmd-dsflash41-needs-effort", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": ""}, {"observation": auto(CC_OK)})
sc("cmd-dsflash41-effort-not-placed", {"pool": "commandcode", "profile": "cmd-dsflash41", "effort": "medium"}, {"observation": auto(CC_OK)})
# credit pool: operator entries
sc("pi-reviewer-denied", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high", "role": "reviewer"}, {"observation": None, "manual": [manual("credit", 40)]})
sc("pi-implementer-manual-credit-admit", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high"}, {"observation": None, "manual": [manual("credit", 40)]})
sc("pi-implementer-no-credit-entry", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high"}, {"observation": None, "manual": []})
sc("pi-implementer-credit-expired-entry", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high"}, {"observation": None, "manual": [manual("credit", 40, expires=t(h=-1))]})
sc("pi-implementer-credit-exhausted", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high"}, {"observation": None, "manual": [manual("credit", 99.5)]})
sc("pi-latest-credit-entry-wins", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "high"}, {"observation": None, "manual": [manual("credit", 99.5, id=40), manual("credit", 30, id=41)]})
sc("pi-consult-only-needs-request", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "max"}, {"observation": None, "manual": [manual("credit", 40)]})
sc("pi-consult-only-with-request", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "max", "operator_request": "hk:doc/decision/x", "requested_by": "desk"}, {"observation": None, "manual": [manual("credit", 40)]})
sc("pi-builder-placed-default", {"pool": "anthropic-api", "profile": "pi-sonnet55", "effort": "xhigh", "role": "builder"}, {"observation": None, "manual": [manual("credit", 40)]})
# devin free bucket
DEVIN_PAID = [b("7d", 50, t(d=5))]
sc("devin-swe2-free-available", {"pool": "devin", "profile": "devin-swe2", "effort": "high"}, {"observation": auto(DEVIN_PAID, free={"state": "available"})})
sc("devin-swe2-free-exhausted", {"pool": "devin", "profile": "devin-swe2", "effort": "high"}, {"observation": auto(DEVIN_PAID, free={"state": "exhausted", "resets_at": t(h=5)})})
sc("devin-swe2-free-reset-passed", {"pool": "devin", "profile": "devin-swe2", "effort": "high"}, {"observation": auto(DEVIN_PAID, free={"state": "exhausted", "resets_at": t(h=-1)})})
sc("devin-swe2-free-unknown", {"pool": "devin", "profile": "devin-swe2", "effort": "high"}, {"observation": auto(DEVIN_PAID, free={"state": "unknown"})})
sc("devin-swe2-no-free-bucket", {"pool": "devin", "profile": "devin-swe2", "effort": "high"}, {"observation": auto(DEVIN_PAID)})
sc("devin-ds41-paid-admit", {"pool": "devin", "profile": "devin-ds41"}, {"observation": auto(DEVIN_PAID, free={"state": "available"})})
sc("devin-ds41-paid-exhausted-alternatives", {"pool": "devin", "profile": "devin-ds41"}, {"observation": auto([b("7d", 99.2, t(d=5))], free={"state": "available"})})
# agy groups, excluded pools, catalog refusals
sc("agy-group-window-gates-its-group", {"pool": "agy", "profile": "agy-flash"}, {"observation": auto([b("5h", 30, t(h=2), scope="group", model="gemini"), b("7d", 30, t(d=5), scope="group", model="gemini"), b("5h", 99.5, t(h=2), scope="group", model="3p")])})
sc("grok-excluded-pool", {"pool": "grok", "profile": "grok", "effort": "medium"}, {"observation": auto([b("7d", 10, t(d=5))])})
sc("account-key-pool-mismatch", {"pool": "codex", "profile": "opus", "effort": "high", "account_key": "codex:" + FP}, {"observation": auto(CLAUDE_OK)})
sc("unknown-profile-account-key", {"pool": "claude", "profile": "opus-legacy", "effort": "high"}, {"observation": None})
json.dump(S, sys.stdout, indent=2)
