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
# This is a permanent quarantine contract, not a compatibility redirect.
# Every v2 dispatch path returns these exact values before authentication,
# request parsing, application construction, or database access.
V2_DISPATCH_QUARANTINE_STATUS = 404
V2_DISPATCH_QUARANTINE_BODY = {
    "error": {"code": "legacy_route_not_found", "message": "route not found"},
    "schema_version": "actionq-quarantine/v1",
}
V2_DISPATCH_QUARANTINE_BYTES = b'{"error":{"code":"legacy_route_not_found","message":"route not found"},"schema_version":"actionq-quarantine/v1"}\n'


def _is_v2_dispatch_path(path: str) -> bool:
    return path == "/v2/dispatch" or path.startswith("/v2/dispatch/")


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

    def _quarantine_v2_dispatch(self) -> bool:
        # ``self.path`` retains the raw request-target spelling.  Classify its
        # path octets before percent decoding or any path normalization.
        if not _is_v2_dispatch_path(self.path.split("?", 1)[0]):
            return False
        # Do not leave an unread hostile request body on a keep-alive socket.
        # This response is emitted from parse_request before any route, auth,
        # application, or database path can run.
        self.close_connection = True
        self.send_response(V2_DISPATCH_QUARANTINE_STATUS)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(V2_DISPATCH_QUARANTINE_BYTES)))
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(V2_DISPATCH_QUARANTINE_BYTES)
        return True

    def parse_request(self) -> bool:
        """Centralize the v2 quarantine for every HTTP method.

        BaseHTTPRequestHandler calls this after parsing only the request line
        and headers, before dispatching to ``do_*`` or reading a body. Returning
        false also covers methods for which it has no built-in ``do_*`` method.
        """

        if not super().parse_request():
            return False
        return not self._quarantine_v2_dispatch()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._quarantine_v2_dispatch():
            return
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
        parsed = urlparse(self.path)
        if self._quarantine_v2_dispatch():
            return
        if parsed.path != "/dispatch":
            self._send_json(404, {"error": "not found"})
            return

        contract_header = self.headers.get("x-actionq-dispatch-contract", "")
        expected_contract = CONTRACT_VERSION
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

    def _unsupported_method(self) -> None:
        if self._quarantine_v2_dispatch():
            return
        self._send_json(404, {"error": "not found"})

    def do_HEAD(self) -> None:  # noqa: N802
        self._unsupported_method()

    def do_PUT(self) -> None:  # noqa: N802
        self._unsupported_method()

    def do_PATCH(self) -> None:  # noqa: N802
        self._unsupported_method()

    def do_DELETE(self) -> None:  # noqa: N802
        self._unsupported_method()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._unsupported_method()


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
