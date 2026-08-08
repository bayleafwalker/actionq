"""Temporary Codex Luna multi-agent catalog compatibility wrapper.

The wrapper deliberately runs inside the harness child boundary.  That keeps
the generated catalog owned by the same identity that runs Codex and makes its
lifetime match one dispatch, including contained-worker launches.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from typing import Any, Sequence


LUNA_V2_CATALOG_WORKAROUND = "luna-v1-to-v2"
_LUNA_MODEL = "gpt-5.6-luna"
_CATALOG_TIMEOUT_SECONDS = 30.0
_active_child: subprocess.Popen[bytes] | None = None
_received_signal: int | None = None


class CatalogWorkaroundError(RuntimeError):
    """The requested compatibility catalog could not be proven safe."""


def _changed_paths(before: Any, after: Any, prefix: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    if type(before) is not type(after):
        return [prefix]
    if isinstance(before, dict):
        if before.keys() != after.keys():
            return [prefix]
        changed: list[tuple[object, ...]] = []
        for key in before:
            changed.extend(_changed_paths(before[key], after[key], (*prefix, key)))
        return changed
    if isinstance(before, list):
        if len(before) != len(after):
            return [prefix]
        changed = []
        for index, (old, new) in enumerate(zip(before, after, strict=True)):
            changed.extend(_changed_paths(old, new, (*prefix, index)))
        return changed
    return [] if before == after else [prefix]


def prepare_luna_v2_catalog(catalog: Any) -> tuple[str, dict[str, Any] | None]:
    """Return ``(status, patched_catalog)`` after structural validation.

    ``native-v2-bypass`` returns no replacement catalog.  ``applied`` changes
    exactly one Luna record's ``multi_agent_version`` from ``v1`` to ``v2``.
    Every ambiguous or unexpected catalog fails closed.
    """
    if not isinstance(catalog, dict) or not isinstance(catalog.get("models"), list):
        raise CatalogWorkaroundError("Codex model catalog must contain a models list")
    luna = [
        (index, model)
        for index, model in enumerate(catalog["models"])
        if isinstance(model, dict) and model.get("slug") == _LUNA_MODEL
    ]
    if len(luna) != 1:
        raise CatalogWorkaroundError("Codex model catalog must contain exactly one Luna record")
    index, record = luna[0]
    version = record.get("multi_agent_version")
    if version == "v2":
        return "native-v2-bypass", None
    if version != "v1":
        raise CatalogWorkaroundError("Luna has an unsupported multi-agent catalog version")

    patched = copy.deepcopy(catalog)
    patched["models"][index]["multi_agent_version"] = "v2"
    expected_path = ("models", index, "multi_agent_version")
    if _changed_paths(catalog, patched) != [expected_path]:
        raise CatalogWorkaroundError("catalog mutation was not exactly Luna v1 to v2")
    return "applied", patched


def _forward_signal(signum: int, _frame: object) -> None:
    global _received_signal
    _received_signal = signum
    child = _active_child
    if child is not None and child.poll() is None:
        child.send_signal(signum)


def _run_child(argv: Sequence[str], *, capture: bool = False) -> tuple[int, bytes, bytes]:
    global _active_child
    if _received_signal is not None:
        return -_received_signal, b"", b""
    try:
        _active_child = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL if capture else None,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
        if capture:
            try:
                stdout, stderr = _active_child.communicate(timeout=_CATALOG_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as exc:
                _active_child.kill()
                _active_child.wait()
                raise CatalogWorkaroundError("Codex model catalog refresh timed out") from exc
            return int(_active_child.returncode or 0), stdout or b"", stderr or b""
        return int(_active_child.wait()), b"", b""
    finally:
        _active_child = None


def _write_catalog(catalog: dict[str, Any]) -> Path:
    descriptor, raw_path = tempfile.mkstemp(prefix="actionq-codex-models-", suffix=".json")
    path = Path(raw_path)
    try:
        os.fchmod(descriptor, 0o600)
        handle = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = -1
        with handle:
            json.dump(catalog, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
    return path


def run(codex_bin: str, codex_argv: Sequence[str], workaround: str) -> int:
    if workaround != LUNA_V2_CATALOG_WORKAROUND:
        raise CatalogWorkaroundError("unsupported Codex catalog workaround")
    catalog_path: Path | None = None
    try:
        return_code, stdout, _stderr = _run_child([codex_bin, "debug", "models"], capture=True)
        if return_code != 0:
            raise CatalogWorkaroundError("Codex model catalog refresh failed")
        try:
            catalog = json.loads(stdout)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CatalogWorkaroundError("Codex model catalog was not valid JSON") from exc
        status, patched = prepare_luna_v2_catalog(catalog)
        if patched is None:
            command = [codex_bin, *codex_argv]
        else:
            catalog_path = _write_catalog(patched)
            override = f"model_catalog_json={json.dumps(str(catalog_path))}"
            command = [codex_bin, "-c", override, *codex_argv]
        print(
            json.dumps(
                {"actionq_codex_catalog_workaround": workaround, "status": status},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
            flush=True,
        )
        return _run_child(command)[0]
    finally:
        if catalog_path is not None:
            catalog_path.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Codex with a temporary compatibility catalog")
    parser.add_argument("--codex-bin", required=True)
    parser.add_argument("--workaround", required=True)
    parser.add_argument("codex_argv", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    codex_argv = args.codex_argv[1:] if args.codex_argv[:1] == ["--"] else args.codex_argv
    if not codex_argv:
        parser.error("Codex argv is required after --")
    handlers = {}
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        handlers[signum] = signal.signal(signum, _forward_signal)
    try:
        return run(args.codex_bin, codex_argv, args.workaround)
    except (CatalogWorkaroundError, OSError) as exc:
        print(f"actionq Codex catalog workaround failed closed: {exc}", file=sys.stderr)
        return 78
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
