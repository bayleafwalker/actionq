"""actionq-server: thin HTTP facade over the actionq Postgres queue.

Exposes POST /dispatch and GET /health. No external framework — stdlib only.
Routing: COCKPIT_ACTIONQ_SERVER_URL -> this server -> actionq pg.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from . import db as _db

CONTRACT_VERSION = "v1"
_KIND_TO_ACTION_TYPE = {
    "implement": "scope-iterate",
    "review": "scope-iterate",
    "test": "scope-iterate",
    "investigate": "scope-iterate",
    "document": "scope-iterate",
    "custom": "scope-iterate",
}


def _schema() -> str:
    return os.environ.get("ACTIONQ_SCHEMA", "actionq")


def _dispatch(payload: dict) -> dict:
    contract = payload.get("contract_version")
    if contract != CONTRACT_VERSION:
        raise ValueError(f"unsupported contract_version: {contract!r}; expected {CONTRACT_VERSION!r}")

    repo_id = (payload.get("repo_id") or "").strip()
    if not repo_id or repo_id == "ALL":
        raise ValueError("repo_id must name one concrete repo")

    kind = (payload.get("kind") or "").strip()
    action_type = _KIND_TO_ACTION_TYPE.get(kind)
    if not action_type:
        raise ValueError(f"kind must be one of: {', '.join(_KIND_TO_ACTION_TYPE)}")

    title = (payload.get("title") or "").strip()
    if not title:
        raise ValueError("title is required")

    priority_label = (payload.get("priority") or "normal").strip()
    priority = 50 if priority_label == "high" else 100

    source_refs = list(payload.get("refs") or [])
    target_ref = (payload.get("work_item_id") or "").strip() or None
    created_by = (payload.get("requested_by") or "operator:cockpit").strip() or "operator:cockpit"

    with _db.connect() as conn:
        schema = _schema()
        action = _db.enqueue(
            conn,
            schema,
            action_type=action_type,
            project=repo_id,
            target_ref=target_ref,
            source_refs=source_refs,
            priority=priority,
            parent_id=None,
            created_by=created_by,
        )
        meta: dict = {
            "title": title,
            "harness": (payload.get("harness") or "").strip() or None,
            "model": (payload.get("model") or "").strip() or None,
            "prompt": (payload.get("prompt") or "").strip() or None,
            "sprint_id": payload.get("sprint_id"),
        }
        _db.insert_event(
            conn,
            schema,
            action_id=action["id"],
            event_type="dispatch.requested",
            actor=created_by,
            payload=meta,
        )
    return action


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        print(format % args, file=sys.stderr, flush=True)

    def _send_json(self, status: int, body: object) -> None:
        data = json.dumps(body, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/dispatch":
            self._send_json(404, {"error": "not found"})
            return

        contract_header = self.headers.get("x-actionq-dispatch-contract", "")
        if contract_header and contract_header != CONTRACT_VERSION:
            self._send_json(400, {"error": f"unsupported dispatch contract: {contract_header!r}"})
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            self._send_json(400, {"error": f"invalid JSON: {exc}"})
            return

        try:
            action = _dispatch(payload)
        except (ValueError, _db.ActionQError) as exc:
            self._send_json(400, {"error": str(exc)})
            return
        except Exception as exc:
            print(f"dispatch error: {exc}", file=sys.stderr, flush=True)
            self._send_json(500, {"error": "internal server error"})
            return

        self._send_json(200, action)


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"actionq-server listening on {host}:{port}", file=sys.stderr, flush=True)
    HTTPServer((host, port), _Handler).serve_forever()
