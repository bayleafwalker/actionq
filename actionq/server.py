"""actionq-server: thin HTTP facade over the actionq Postgres queue.

Exposes GET /health, GET /sessions, GET /dispatches, POST /dispatch. No external framework — stdlib only.
Routing: COCKPIT_ACTIONQ_SERVER_URL -> this server -> actionq pg.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from . import db as _db
from . import schema as _schema_contract
from .application import ActionQApplication

CONTRACT_VERSION = "v1"


def _schema() -> str:
    return os.environ.get("ACTIONQ_SCHEMA", "actionq")


def _compatibility() -> dict:
    return ActionQApplication(schema=_schema()).compatibility()


def _require_runtime_compatibility() -> dict:
    """Fail service startup closed; this path deliberately performs no DDL."""

    with _db.connect() as conn:
        return _db.require_compatible(conn, _schema()).as_dict()


def _dispatch(payload: dict) -> dict:
    return ActionQApplication(schema=_schema()).dispatch(payload)


def _sessions(query_string: str) -> list:
    params = parse_qs(query_string or "")
    raw_active = params.get("active_only", ["false"])[0].lower()
    active_only = raw_active in ("true", "1", "yes")
    limit = min(int(params.get("limit", ["500"])[0]), 1000)
    project = (params.get("project", [None])[0] or "").strip() or None
    return ActionQApplication(schema=_schema()).list_sessions(
        project=project,
        active_only=active_only,
        limit=limit,
    )


def _dispatches(query_string: str) -> list:
    params = parse_qs(query_string or "")
    limit = min(int(params.get("limit", ["100"])[0]), 500)
    project = (params.get("project", [None])[0] or "").strip() or None
    status = (params.get("status", [None])[0] or "").strip() or None
    return ActionQApplication(schema=_schema()).list_dispatches(
        project=project,
        status=status,
        limit=limit,
    )


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        print(format % args, file=sys.stderr, flush=True)

    def _send_json(self, status: int, body: object) -> None:
        data = json.dumps(body, default=_db.json_default).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"ok": True})
        elif parsed.path == "/compatibility":
            try:
                compatibility = _compatibility()
            except Exception as exc:
                print(f"compatibility error: {exc}", file=sys.stderr, flush=True)
                self._send_json(503, {"error": "schema compatibility unavailable"})
                return
            self._send_json(200 if compatibility["compatible"] else 503, compatibility)
        elif parsed.path == "/sessions":
            try:
                sessions = _sessions(parsed.query)
            except _schema_contract.SchemaCompatibilityError as exc:
                print(f"sessions refused: {exc}", file=sys.stderr, flush=True)
                self._send_json(503, {"error": "schema incompatible"})
                return
            except Exception as exc:
                print(f"sessions error: {exc}", file=sys.stderr, flush=True)
                self._send_json(500, {"error": "internal server error"})
                return
            self._send_json(200, sessions)
        elif parsed.path == "/dispatches":
            try:
                dispatches = _dispatches(parsed.query)
            except _schema_contract.SchemaCompatibilityError as exc:
                print(f"dispatches refused: {exc}", file=sys.stderr, flush=True)
                self._send_json(503, {"error": "schema incompatible"})
                return
            except Exception as exc:
                print(f"dispatches error: {exc}", file=sys.stderr, flush=True)
                self._send_json(500, {"error": "internal server error"})
                return
            self._send_json(200, dispatches)
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
        except _schema_contract.SchemaCompatibilityError as exc:
            print(f"dispatch refused: {exc}", file=sys.stderr, flush=True)
            self._send_json(503, {"error": "schema incompatible"})
            return
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
    try:
        compatibility = _require_runtime_compatibility()
    except Exception as exc:
        print(
            f"actionq-server startup refused: schema compatibility check failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(3) from None
    print(
        "actionq schema compatibility "
        f"{compatibility['state']} version={compatibility['observed_schema_version']}",
        file=sys.stderr,
        flush=True,
    )
    print(f"actionq-server listening on {host}:{port}", file=sys.stderr, flush=True)
    HTTPServer((host, port), _Handler).serve_forever()
