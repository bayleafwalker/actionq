#!/usr/bin/env python3
"""Re-runnable model-binding probe for the native harness adapters.

Answers the A19 question for each installed vendor CLI: **can the caller
establish which model actually ran, and at what point relative to consuming
subscription usage?**

For each harness this runs up to three noninteractive turns in a disposable
worktree -- default model, an explicitly requested model, and a deliberately
bogus model -- and reports, per turn: whether a model id was recoverable, at
what phase (pre-flight / post-hoc / not at all), whether usage was consumed,
and whether the declared output format actually parsed.

It never asserts. It prints a table; a human reads it into an evidence
record. Usage is reported in each vendor's own unit (Claude: notional
`costUSD`; Copilot: premium requests). No dollar figure here is money --
this estate holds subscriptions only.

    python3 verification/probe_native_harness_binding.py            # all
    python3 verification/probe_native_harness_binding.py codex      # one
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROMPT = "Reply with exactly the word: ok"
BOGUS = "definitely-not-a-model-xyz"
TIMEOUT = 180

PRE, POST, NONE = "pre-flight", "post-hoc", "none"


def run(argv: list[str], cwd: Path, stdin: str | None) -> tuple[int, str, str, bool]:
    try:
        p = subprocess.run(argv, cwd=cwd, input=stdin, text=True,
                           capture_output=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired as exc:
        return -1, exc.stdout or "", exc.stderr or "", True
    return p.returncode, p.stdout, p.stderr, False


def jsonl(text: str) -> tuple[list[dict], int]:
    """Parse JSONL, returning (objects, count_of_unparseable_nonblank_lines)."""
    objs, bad = [], 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            objs.append(json.loads(line))
        except json.JSONDecodeError:
            bad += 1
    return objs, bad


# --- per-harness probes: each returns a dict of observations ---------------

def probe_claude(cwd: Path, model: str | None) -> dict:
    argv = ["claude", "-p", "--output-format", "json",
            "--no-session-persistence", "--add-dir", str(cwd)]
    if model:
        argv += ["--model", model]
    code, out, err, to = run(argv, cwd, PROMPT)
    if to:
        return {"exit": "timeout", "readback": NONE, "usage": "?", "note": "no output"}
    try:
        env = json.loads(out)
    except json.JSONDecodeError:
        return {"exit": code, "readback": NONE, "usage": "?", "note": "envelope unparseable"}
    usage = env.get("modelUsage") or {}
    return {
        "exit": code,
        "readback": POST if usage else NONE,
        "models": ",".join(usage) or "-",
        "usage": f"{sum(u.get('inputTokens', 0) for u in usage.values())} in-tok",
        "note": f"is_error={env.get('is_error')}",
    }


def probe_codex(cwd: Path, model: str | None) -> dict:
    argv = ["codex", "exec", "--skip-git-repo-check", "--json",
            "--sandbox", "read-only", "-C", str(cwd)]
    if model:
        argv += ["--model", model]
    argv.append("-")
    code, out, err, to = run(argv, cwd, PROMPT)
    if to:
        return {"exit": "timeout", "readback": NONE, "usage": "?", "note": "no output"}
    objs, bad = jsonl(out)
    thread = next((o["thread_id"] for o in objs if o.get("type") == "thread.started"), None)
    used = next((o.get("usage") for o in objs if o.get("type") == "turn.completed"), None)
    # The stream never names the model. The only read-back is the persisted
    # rollout file -- which exists only because the adapter omits --ephemeral.
    found = "-"
    if thread:
        hits = list(Path.home().joinpath(".codex/sessions").rglob(f"*{thread}*.jsonl"))
        if hits:
            for line in hits[0].read_text(errors="replace").splitlines():
                if '"model"' in line:
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    m = json.dumps(d)
                    import re
                    mm = re.search(r'"model":\s*"([^"]+)"', m)
                    if mm:
                        found = mm.group(1) + " (rollout file: resolved REQUEST, not served)"
                        break
    return {
        "exit": code,
        # Not a true read-back: the rollout echoes the resolved request even
        # for a model that never ran (see the bogus case).
        "readback": NONE if found == "-" else "request echo, off-stream",
        "models": found,
        "usage": (f"{used.get('input_tokens')} in-tok" if used
                  else ("0 (failed closed)" if code != 0 else "not reported")),
        "note": f"{bad} unparseable line(s)" if bad else "stream names no model",
    }


def probe_opencode(cwd: Path, model: str | None) -> dict:
    argv = ["opencode", "run", "--format", "json"]
    if model:
        argv += ["--model", model]
    argv.append(PROMPT)
    code, out, err, to = run(argv, cwd, None)
    if to:
        return {"exit": "timeout", "readback": NONE, "models": "-", "usage": "?",
                "note": f"hung; stdout={len(out)}B stderr={len(err)}B"}
    objs, bad = jsonl(out)
    return {"exit": code, "readback": NONE if not objs else "unclassified",
            "models": "-", "usage": "?",
            "note": f"{len(objs)} events, {bad} unparseable"}


def probe_copilot(cwd: Path, model: str | None) -> dict:
    argv = ["copilot", "-p", PROMPT, "--allow-all-tools", "--no-color",
            "--output-format", "json", "--add-dir", str(cwd)]
    if model:
        argv += ["--model", model]
    code, out, err, to = run(argv, cwd, None)
    if to:
        return {"exit": "timeout", "readback": NONE, "models": "-", "usage": "?",
                "note": "no output"}
    objs, bad = jsonl(out)
    def data(t):
        return next((o.get("data", {}) for o in objs if o.get("type") == t), None)
    chosen = (data("session.auto_mode_resolved") or {}).get("chosenModel")
    started = (data("model.call_start") or {}).get("model")
    msg = (data("assistant.message") or {}).get("model")
    ck = data("session.usage_checkpoint") or {}
    if chosen or started:
        phase = PRE            # named before the call is issued
    elif msg:
        phase = POST
    else:
        phase = NONE
    return {
        "exit": code,
        "readback": phase,
        "models": chosen or started or msg or "-",
        "usage": (f"{ck.get('totalPremiumRequests')} premium req" if ck
                  else ("0 (failed closed)" if code != 0 else "not reported")),
        "note": f"{bad} PLAIN-TEXT line(s) on json stream" if bad else "clean JSONL",
    }


HARNESSES = {
    "claude": (probe_claude, None),
    "codex": (probe_codex, None),
    "opencode": (probe_opencode, "opencode/deepseek-v4-flash-free"),
    "copilot": (probe_copilot, "auto"),
}


def main() -> int:
    want = sys.argv[1:] or list(HARNESSES)
    root = Path(tempfile.mkdtemp(prefix="harness-probe-"))
    (root / "README.md").write_text("probe\n")
    print(f"worktree: {root}\n")
    header = f"{'harness':9} {'case':10} {'exit':>7}  {'read-back':22} {'model':38} {'usage':18} note"
    print(header)
    print("-" * len(header))
    try:
        for name in want:
            if name not in HARNESSES:
                print(f"unknown harness: {name}", file=sys.stderr)
                continue
            probe, good = HARNESSES[name]
            if shutil.which(name if name != "copilot" else "copilot") is None:
                print(f"{name:9} {'-':10} {'absent':>7}  CLI not installed")
                continue
            cases = [("default", None), ("bogus", BOGUS)]
            if good:
                cases.insert(1, ("explicit", good))
            for case, model in cases:
                r = probe(root, model)
                print(f"{name:9} {case:10} {str(r.get('exit')):>7}  "
                      f"{r.get('readback', '?'):22} {r.get('models', '-')[:38]:38} "
                      f"{str(r.get('usage', '?')):18} {r.get('note', '')}")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
