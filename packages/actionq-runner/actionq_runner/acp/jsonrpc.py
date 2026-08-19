"""Line-delimited JSON-RPC 2.0 over a child process's stdio.

Transport only. Nothing here knows what ACP is; the codec layer supplies methods and
interprets results. Keeping the split means a second protocol version is a new codec
rather than a new process supervisor.
"""
from __future__ import annotations

import json
import subprocess
import threading
from queue import Empty, Queue
from typing import Any, Callable, Iterator


class JsonRpcError(RuntimeError):
    """A peer returned a JSON-RPC error object."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.data = data

    @property
    def method_not_found(self) -> bool:
        return self.code == -32601


class JsonRpcTimeout(RuntimeError):
    """The peer did not answer a request within its deadline."""


class StdioJsonRpc:
    """Speaks JSON-RPC to a subprocess over stdin/stdout, one JSON object per line.

    Incoming traffic is demultiplexed by a reader thread into two streams: responses,
    keyed by request id, and everything else (notifications and peer-initiated requests),
    which is handed to ``on_inbound``.
    """

    def __init__(
        self,
        command: list[str],
        *,
        cwd: str,
        env: dict[str, str],
        on_inbound: Callable[[dict[str, Any]], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
    ) -> None:
        self._proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
            env=env,
            text=True,
            bufsize=1,
        )
        self._next_id = 0
        self._lock = threading.Lock()
        self._responses: dict[int, Queue[dict[str, Any]]] = {}
        self._on_inbound = on_inbound
        self._on_stderr = on_stderr
        self._closed = threading.Event()
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._reader.start()
        self._errs = threading.Thread(target=self._read_stderr, daemon=True)
        self._errs.start()

    # -- lifecycle ---------------------------------------------------------

    @property
    def alive(self) -> bool:
        return self._proc.poll() is None

    def close(self, *, grace_seconds: float = 5.0) -> int | None:
        self._closed.set()
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=grace_seconds)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=grace_seconds)
        return self._proc.returncode

    # -- reading -----------------------------------------------------------

    def _read_stdout(self) -> None:
        assert self._proc.stdout is not None
        for line in self._proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(message, dict) and "id" in message and "method" not in message:
                with self._lock:
                    slot = self._responses.get(message["id"])
                if slot is not None:
                    slot.put(message)
                continue
            if self._on_inbound is not None and isinstance(message, dict):
                self._on_inbound(message)
        self._closed.set()
        # Unblock anyone still waiting on a reply that can no longer arrive.
        with self._lock:
            waiters = list(self._responses.values())
        for slot in waiters:
            slot.put({"__eof__": True})

    def _read_stderr(self) -> None:
        assert self._proc.stderr is not None
        for line in self._proc.stderr:
            if self._on_stderr is not None:
                self._on_stderr(line.rstrip("\n"))

    # -- writing -----------------------------------------------------------

    def _write(self, message: dict[str, Any]) -> None:
        if self._proc.stdin is None or self._proc.poll() is not None:
            raise JsonRpcTimeout("agent process is not accepting input")
        self._proc.stdin.write(json.dumps(message) + "\n")
        self._proc.stdin.flush()

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self._write({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def respond(self, request_id: Any, result: dict[str, Any]) -> None:
        self._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    def respond_error(self, request_id: Any, code: int, message: str) -> None:
        self._write({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}})

    def request(
        self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 30.0
    ) -> dict[str, Any]:
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            slot: Queue[dict[str, Any]] = Queue()
            self._responses[request_id] = slot
        try:
            self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})
            try:
                message = slot.get(timeout=timeout)
            except Empty:
                raise JsonRpcTimeout(f"{method} did not answer within {timeout}s") from None
            if message.get("__eof__"):
                raise JsonRpcTimeout(f"agent exited before answering {method}")
            if "error" in message:
                error = message["error"]
                raise JsonRpcError(error.get("code", 0), error.get("message", ""), error.get("data"))
            return message.get("result") or {}
        finally:
            with self._lock:
                self._responses.pop(request_id, None)
