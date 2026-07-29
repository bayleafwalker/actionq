"""actionq-server: thin HTTP facade over the actionq Postgres queue.

Exposes GET /health, GET /sessions, GET /dispatches, POST /dispatch. No external framework — stdlib only.
Routing: COCKPIT_ACTIONQ_SERVER_URL -> this server -> actionq pg.
"""
from __future__ import annotations

import json
import os
import sys
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from . import db as _db
from . import schema as _schema_contract
from .application import ActionQApplication, InvocationProvenance

CONTRACT_VERSION = "v1"
V2_CONTRACT_VERSION = "v2"


class AuthenticatedDispatchIdentity:
    """Trusted upstream result; request headers never define an identity."""
    def __init__(self, actor: str, environment: str, repositories: tuple[str, ...]):
        self.actor, self.environment, self.repositories = actor, environment, repositories


def _no_authenticator(_headers) -> AuthenticatedDispatchIdentity:
    raise _db.ActionQError("v2 dispatch authentication is not configured")


_served_authenticator = _no_authenticator


def set_served_authenticator_for_testing(authenticator) -> None:
    """Inject a trusted middleware bridge in tests or deployment composition."""
    global _served_authenticator
    _served_authenticator = authenticator


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


def _request_provenance(headers) -> InvocationProvenance:
    identity = _served_authenticator(headers)
    key = headers.get("idempotency-key", "").strip()
    if not identity.actor or not identity.environment or not identity.repositories or not key:
        raise _db.ActionQError("v2 enqueue requires authenticated repository-scoped identity and Idempotency-Key")
    return InvocationProvenance(actor=identity.actor, environment=identity.environment, request_id=headers.get("x-request-id", key), catalog_revision="http-v2", idempotency_key=key, authorized_repositories=tuple(identity.repositories))


def _served_authorizer(provenance: InvocationProvenance, resource: str, _verb: str) -> bool:
    prefix = "execution.dispatch.repo:"
    return resource.startswith(prefix) and resource[len(prefix):] in provenance.authorized_repositories


def _dispatch_v2(payload: dict, headers) -> dict:
    if "requested_by" in payload:
        raise _db.ActionQError("requested_by is derived from authenticated identity and must not be supplied")
    provenance = _request_provenance(headers)
    return ActionQApplication(schema=_schema(), authorizer=_served_authorizer).enqueue_dispatch_v2({**payload, "requested_by": provenance.actor}, provenance=provenance)


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
        action_match = re.fullmatch(r"/v2/dispatch/actions/(\d+)", parsed.path)
        changes_match = re.fullmatch(r"/v2/dispatch/actions/(\d+)/changes", parsed.path)
        request_match = re.fullmatch(r"/v2/dispatch/requests/(req:[A-Za-z0-9_-]+)", parsed.path)
        if action_match or changes_match or request_match:
            try:
                provenance = _request_provenance(self.headers)
                params = parse_qs(parsed.query or "")
                if changes_match:
                    body = ActionQApplication(schema=_schema(), authorizer=_served_authorizer).dispatch_action_changes(int(changes_match.group(1)), cursor=params.get("cursor", [None])[0], wait_seconds=min(int(params.get("wait_seconds", ["0"])[0]), 30), provenance=provenance)
                elif request_match:
                    body = ActionQApplication(schema=_schema(), authorizer=_served_authorizer).resolve_dispatch_request(request_match.group(1), provenance=provenance)
                else:
                    body = ActionQApplication(schema=_schema(), authorizer=_served_authorizer).dispatch_action_snapshot(int(action_match.group(1)), provenance=provenance)
                self._send_json(200, body)
            except (ValueError, _db.ActionQError) as exc:
                self._send_json(400, {"error": str(exc)})
            except Exception as exc:
                print(f"dispatch v2 read error: {exc}", file=sys.stderr, flush=True)
                self._send_json(500, {"error": "internal server error"})
        elif parsed.path == "/health":
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
        parsed = urlparse(self.path)
        if parsed.path not in {"/dispatch", "/v2/dispatch"}:
            self._send_json(404, {"error": "not found"})
            return

        contract_header = self.headers.get("x-actionq-dispatch-contract", "")
        expected_contract = V2_CONTRACT_VERSION if parsed.path == "/v2/dispatch" else CONTRACT_VERSION
        if contract_header and contract_header != expected_contract:
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
            action = _dispatch_v2(payload, self.headers) if parsed.path == "/v2/dispatch" else _dispatch(payload)
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
