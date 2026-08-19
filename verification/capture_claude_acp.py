#!/usr/bin/env python3
"""Capture what Claude Code ACP exposes past `initialize`.

MUST be run from a plain shell, NOT inside a Claude Code session: the agent has a
nesting guard (evidence A10), and defeating it programmatically is explicitly not
something this repo does. If CLAUDECODE is set, this refuses.

Answers the questions the Claude ACP plan is blocked on:
  - does `session/set_model` exist?   (if not, the adapter fails closed and the
    honest outcome is a documented refusal, not a workaround)
  - what does `session/new` return -- configOptions, models.availableModels, both,
    neither?
  - is the bound model readable back anywhere?
  - what is the mode vocabulary?
  - does the live session appear in `session/list`?

Writes docs/evidence/acp-agents/claude-code-acp-0.16.2.session-new.json.
Deliberately not named *.initialize.json: tests/test_acp_agent_divergence.py globs
that pattern and asserts exactly three capability sets.

Usage:  python3 verification/capture_claude_acp.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs/evidence/acp-agents/claude-code-acp-0.16.2.session-new.json"
AGENT = ["npx", "-y", "@zed-industries/claude-code-acp@0.16.2"]
TIMEOUT = 90.0


class Rpc:
    def __init__(self, proc):
        self.proc = proc
        self._id = 0
        self.notifications: list[dict] = []

    def _read_message(self, timeout: float):
        """Read one JSON line. Returns None on EOF/timeout."""
        box: list[str | None] = [None]

        def _r():
            box[0] = self.proc.stdout.readline()

        t = threading.Thread(target=_r, daemon=True)
        t.start()
        t.join(timeout)
        if t.is_alive() or not box[0]:
            return None
        line = box[0].strip()
        if not line:
            return {}
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return {"_unparsed": line}

    def request(self, method: str, params: dict | None = None, timeout: float = TIMEOUT):
        self._id += 1
        rid = self._id
        msg = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            msg["params"] = params
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()
        while True:
            m = self._read_message(timeout)
            if m is None:
                return {"_error": "timeout-or-eof", "method": method}
            if not m:
                continue
            if m.get("id") == rid:
                return m
            if "method" in m:
                self.notifications.append(m)
                # Agent-initiated requests must be answered or it deadlocks.
                if "id" in m:
                    self.proc.stdin.write(json.dumps({
                        "jsonrpc": "2.0", "id": m["id"],
                        "result": {"outcome": {"outcome": "cancelled"}},
                    }) + "\n")
                    self.proc.stdin.flush()


def summarize(label, resp):
    if "_error" in resp:
        return {"status": resp["_error"]}
    if "error" in resp:
        e = resp["error"]
        return {"status": "error", "code": e.get("code"), "message": e.get("message"),
                "method_not_found": e.get("code") == -32601}
    return {"status": "ok", "result": resp.get("result")}


def main() -> int:
    if os.environ.get("CLAUDECODE"):
        print("REFUSING: CLAUDECODE is set -- run this from a plain shell.\n"
              "The agent's nesting guard (A10) exists for a reason; this script "
              "will not unset it.", file=sys.stderr)
        return 2

    workspace = Path(tempfile.mkdtemp(prefix="claude-acp-capture-"))
    (workspace / "README.md").write_text("capture workspace\n")

    print(f"launching: {' '.join(AGENT)}")
    proc = subprocess.Popen(
        AGENT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, bufsize=1, cwd=str(workspace),
    )
    rpc = Rpc(proc)
    capture: dict = {"agent": "@zed-industries/claude-code-acp@0.16.2"}

    try:
        print("-> initialize")
        init = rpc.request("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}},
        })
        capture["initialize"] = summarize("initialize", init)

        print("-> session/new")
        new = rpc.request("session/new", {"cwd": str(workspace), "mcpServers": []})
        capture["session_new"] = summarize("session/new", new)

        session_id = (new.get("result") or {}).get("sessionId")
        capture["session_id_present"] = bool(session_id)

        if session_id:
            # THE question the plan is blocked on.
            print("-> session/set_model")
            capture["set_model"] = summarize("set_model", rpc.request(
                "session/set_model", {"sessionId": session_id, "modelId": "claude-sonnet-4-5"}))

            for probe in ("session/status", "session/info", "session/list"):
                print(f"-> {probe}")
                params = {} if probe == "session/list" else {"sessionId": session_id}
                capture[probe.replace("/", "_")] = summarize(probe, rpc.request(probe, params, timeout=30))

            print("-> session/set_mode (probing vocabulary)")
            capture["set_mode_probe"] = summarize("set_mode", rpc.request(
                "session/set_mode", {"sessionId": session_id, "modeId": "__probe__"}, timeout=30))

        capture["notifications_seen"] = [n.get("method") for n in rpc.notifications][:20]
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        err = proc.stderr.read() if proc.stderr else ""
        if err.strip():
            capture["stderr_tail"] = err.strip().splitlines()[-15:]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(capture, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nwrote {OUT}")
    print(json.dumps(capture, indent=2, sort_keys=True)[:2500])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
