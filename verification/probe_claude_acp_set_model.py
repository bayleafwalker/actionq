#!/usr/bin/env python3
"""Does Claude Code ACP's session/set_model mean anything?

The first capture sent modelId="claude-sonnet-4-5" -- an id NOT in the agent's own
advertised list (default/sonnet/haiku) -- and got {"result":{}}. If obvious garbage
is also accepted, then a successful set_model response carries no information about
what the session is actually bound to, and neither session/status nor session/info
exists to check (both -32601).

That would put Claude Code ACP squarely in the evidence-A4 family: an execution
that reports success while running on a model nobody chose. Worth being certain
about rather than inferring from one observation -- the A8 mistake was exactly
generalising a single plausible reading.

Run from a plain shell, not inside a Claude Code session.
"""
from __future__ import annotations

import json, os, subprocess, sys, tempfile, threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/evidence/acp-agents/claude-code-acp-0.16.2.set-model-probe.json"
AGENT = ["npx", "-y", "@zed-industries/claude-code-acp@0.16.2"]

CANDIDATES = [
    ("advertised-sonnet",        "sonnet"),
    ("advertised-haiku",         "haiku"),
    ("plausible-but-unlisted",   "claude-sonnet-4-5"),
    ("other-vendor",             "gpt-5.6-sol[low]"),
    ("local-profile",            "local3090/worker-fast"),
    ("obvious-garbage",          "definitely-not-a-model-9999"),
    ("empty-string",             ""),
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


def main() -> int:
    if os.environ.get("CLAUDECODE"):
        print("REFUSING: run from a plain shell (nesting guard, A10).", file=sys.stderr); return 2
    ws = Path(tempfile.mkdtemp(prefix="acp-setmodel-")); (ws/"README.md").write_text("probe\n")
    p = subprocess.Popen(AGENT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, bufsize=1, cwd=str(ws))
    rpc = Rpc(p); out = {"agent": "@zed-industries/claude-code-acp@0.16.2", "probes": {}}
    try:
        rpc.req("initialize", {"protocolVersion": 1,
                               "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}}})
        new = rpc.req("session/new", {"cwd": str(ws), "mcpServers": []})
        sid = (new.get("result") or {}).get("sessionId")
        out["advertised"] = [m["modelId"] for m in
                             (new.get("result") or {}).get("models", {}).get("availableModels", [])]
        out["currentModelId"] = (new.get("result") or {}).get("models", {}).get("currentModelId")
        if not sid:
            out["error"] = "no sessionId"; return 1
        for label, mid in CANDIDATES:
            r = rpc.req("session/set_model", {"sessionId": sid, "modelId": mid}, t=45)
            if "error" in r:
                out["probes"][label] = {"modelId": mid, "accepted": False,
                                        "code": r["error"].get("code"),
                                        "message": r["error"].get("message", "")[:120]}
            else:
                out["probes"][label] = {"modelId": mid, "accepted": True, "result": r.get("result")}
            print(f"  {label:<24} {mid!r:<32} -> "
                  f"{'ACCEPTED' if out['probes'][label]['accepted'] else 'rejected'}")
    finally:
        try: p.stdin.close()
        except Exception: pass
        p.terminate()
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired: p.kill()

    acc = [k for k, v in out["probes"].items() if v["accepted"]]
    unlisted_accepted = [k for k in acc if out["probes"][k]["modelId"] not in out["advertised"]]
    out["verdict"] = ("set_model accepts unadvertised model ids and cannot be read back; "
                      "a success response is not evidence of binding"
                      if unlisted_accepted else
                      "set_model validates against the advertised list")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nadvertised: {out['advertised']}")
    print(f"accepted-but-unadvertised: {unlisted_accepted}")
    print(f"VERDICT: {out['verdict']}")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
