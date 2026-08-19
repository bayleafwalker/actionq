#!/usr/bin/env python3
"""Can Claude Code ACP be bound to a model VERIFIABLY, despite A13?

A13 established that session/set_model accepts any string and cannot be read back,
so its success proves nothing. That killed one binding channel. This probes a
second one, which the package source says should work differently:

  * SettingsManager reads `<cwd>/.claude/settings.json` (and settings.local.json,
    higher precedence) -- and `cwd` is already something ExecutionEnvelope
    controls, because session/new takes it.
  * getAvailableModels() resolves settings.model against the advertised list and
    calls query.setModel() with the RESOLVED value.
  * session/new then returns models.currentModelId.

If currentModelId tracks what we wrote, that is a real read-back: a binding that
can be verified before any prompt is sent, which is exactly what set_model lacks.

Two failure modes to prove, not assume:
  * an unmatched settings.model silently falls back to models[0] (Opus/default)
    -- silent, but detectable via currentModelId;
  * the matcher does substring matching in BOTH directions, so `sonnet-5` should
    resolve to `sonnet` (Sonnet 4.5). If that is confirmed, only exact advertised
    ids are safe to write.

Run from a plain shell, not inside a Claude Code session (A10 nesting guard).
"""
from __future__ import annotations

import json, os, subprocess, sys, tempfile, threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/evidence/acp-agents/claude-code-acp-0.16.2.settings-binding-probe.json"
AGENT = ["npx", "-y", "@zed-industries/claude-code-acp@0.16.2"]

CASES = [
    ("no-settings-file",      None,                    "expect models[0] default"),
    ("exact-sonnet",          "sonnet",                "expect sonnet"),
    ("exact-haiku",           "haiku",                 "expect haiku"),
    ("exact-default",         "default",               "expect default"),
    ("longer-name-sonnet-5",  "sonnet-5",              "substring trap: expect sonnet (4.5)"),
    ("unmatchable",           "totally-unknown-model", "expect silent fallback to models[0]"),
]


class Rpc:
    def __init__(self, p): self.p, self._id = p, 0
    def _read(self, t=60.0):
        box = [None]
        def r(): box[0] = self.p.stdout.readline()
        th = threading.Thread(target=r, daemon=True); th.start(); th.join(t)
        if th.is_alive() or not box[0]: return None
        s = box[0].strip()
        if not s: return {}
        try: return json.loads(s)
        except json.JSONDecodeError: return {"_unparsed": s}
    def req(self, method, params=None, t=60.0):
        self._id += 1; rid = self._id
        m = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None: m["params"] = params
        self.p.stdin.write(json.dumps(m) + "\n"); self.p.stdin.flush()
        while True:
            r = self._read(t)
            if r is None: return {"_error": "timeout"}
            if not r: continue
            if r.get("id") == rid: return r
            if "method" in r and "id" in r:
                self.p.stdin.write(json.dumps({"jsonrpc":"2.0","id":r["id"],
                    "result":{"outcome":{"outcome":"cancelled"}}})+"\n"); self.p.stdin.flush()


def run_case(label, model_value):
    ws = Path(tempfile.mkdtemp(prefix=f"acp-bind-{label}-"))
    (ws / "README.md").write_text("probe\n")
    if model_value is not None:
        (ws / ".claude").mkdir()
        (ws / ".claude" / "settings.json").write_text(
            json.dumps({"model": model_value}), encoding="utf-8")
    p = subprocess.Popen(AGENT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, bufsize=1, cwd=str(ws))
    rpc = Rpc(p)
    try:
        rpc.req("initialize", {"protocolVersion": 1,
                "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}}})
        new = rpc.req("session/new", {"cwd": str(ws), "mcpServers": []})
        models = (new.get("result") or {}).get("models", {})
        return {"wrote_model": model_value,
                "currentModelId": models.get("currentModelId"),
                "available": [m["modelId"] for m in models.get("availableModels", [])]}
    finally:
        try: p.stdin.close()
        except Exception: pass
        p.terminate()
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill()


def main() -> int:
    if os.environ.get("CLAUDECODE"):
        print("REFUSING: run from a plain shell (A10 nesting guard).", file=sys.stderr); return 2
    out = {"agent": "@zed-industries/claude-code-acp@0.16.2", "cases": {}}
    for label, value, note in CASES:
        print(f"-> {label} (settings.model={value!r})  [{note}]")
        try:
            res = run_case(label, value)
        except Exception as exc:
            res = {"error": str(exc)}
        out["cases"][label] = res
        print(f"     currentModelId = {res.get('currentModelId')!r}")

    tracks = all(
        out["cases"][k].get("currentModelId") == k.split("exact-")[1]
        for k in ("exact-sonnet", "exact-haiku", "exact-default")
        if k in out["cases"] and "error" not in out["cases"][k]
    )
    out["verdict"] = (
        "settings.model + session/new currentModelId is a VERIFIABLE binding channel"
        if tracks else
        "currentModelId does not track settings.model; no verifiable channel found"
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nVERDICT: {out['verdict']}")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
