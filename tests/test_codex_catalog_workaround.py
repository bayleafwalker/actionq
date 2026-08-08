from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

import pytest

from actionq.harnesses import CodexAdapter, HarnessInvocation
from actionq.harnesses.codex_catalog import (
    CatalogWorkaroundError,
    LUNA_V2_CATALOG_WORKAROUND,
    prepare_luna_v2_catalog,
)


def _catalog(version: str = "v1") -> dict[str, object]:
    return {
        "models": [
            {"slug": "gpt-5.6-sol", "multi_agent_version": "v2", "metadata": {"tier": "sol"}},
            {"slug": "gpt-5.6-luna", "multi_agent_version": version, "metadata": {"tier": "luna"}},
        ],
        "metadata": {"generation": 9},
    }


def _fake_codex(tmp_path: Path, name: str = "fake codex") -> Path:
    binary = tmp_path / name
    binary.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, signal, stat, sys, time\n"
        "capture = pathlib.Path(os.environ['FAKE_CAPTURE'])\n"
        "version = os.environ.get('FAKE_LUNA_VERSION', 'v1')\n"
        "if sys.argv[1:] == ['debug', 'models']:\n"
        "    if os.environ.get('FAKE_DELETE_AFTER_DEBUG'):\n"
        "        pathlib.Path(sys.argv[0]).unlink()\n"
        "    if os.environ.get('FAKE_BAD_JSON'):\n"
        "        print('not-json')\n"
        "    else:\n"
        "        models = [\n"
        "            {'slug':'gpt-5.6-sol','multi_agent_version':'v2','metadata':{'tier':'sol'}},\n"
        "            {'slug':'gpt-5.6-luna','multi_agent_version':version,'metadata':{'tier':'luna'}},\n"
        "        ]\n"
        "        if os.environ.get('FAKE_DUPLICATE_LUNA'):\n"
        "            models.append(dict(models[-1]))\n"
        "        print(json.dumps({'models':models,'metadata':{'generation':9}}))\n"
        "    raise SystemExit(int(os.environ.get('FAKE_DEBUG_EXIT', '0')))\n"
        "argv = sys.argv[1:]\n"
        "catalog_path = None\n"
        "catalog = None\n"
        "mode = None\n"
        "if argv[:1] == ['-c']:\n"
        "    key, value = argv[1].split('=', 1)\n"
        "    assert key == 'model_catalog_json'\n"
        "    catalog_path = pathlib.Path(json.loads(value))\n"
        "    mode = stat.S_IMODE(catalog_path.stat().st_mode)\n"
        "    catalog = json.loads(catalog_path.read_text(encoding='utf-8'))\n"
        "capture.write_text(json.dumps({\n"
        "    'argv': argv, 'stdin': sys.stdin.read(), 'catalog_path': str(catalog_path) if catalog_path else None,\n"
        "    'catalog': catalog, 'mode': mode,\n"
        "}), encoding='utf-8')\n"
        "marker = os.environ.get('FAKE_STARTED')\n"
        "if marker:\n"
        "    pathlib.Path(marker).write_text('started', encoding='utf-8')\n"
        "if os.environ.get('FAKE_SLEEP'):\n"
        "    time.sleep(30)\n"
        "print('fake-codex-ok')\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    return binary


def _invocation(tmp_path: Path, binary: Path, **extra_env: str) -> tuple[CodexAdapter, HarnessInvocation]:
    capture = tmp_path / "capture.json"
    adapter = CodexAdapter(
        bin_path=str(binary),
        catalog_workaround=LUNA_V2_CATALOG_WORKAROUND,
    )
    invocation = HarnessInvocation(
        prompt="do the thing",
        worktree=tmp_path,
        model="gpt-5.6-sol",
        extra_env={"FAKE_CAPTURE": str(capture), **extra_env},
    )
    return adapter, invocation


def test_structural_patch_changes_exactly_one_luna_field():
    original = _catalog()
    status, patched = prepare_luna_v2_catalog(original)

    assert status == "applied"
    assert patched is not None
    assert original["models"][1]["multi_agent_version"] == "v1"  # type: ignore[index]
    assert patched["models"][1]["multi_agent_version"] == "v2"  # type: ignore[index]
    patched["models"][1]["multi_agent_version"] = "v1"  # type: ignore[index]
    assert patched == original


def test_native_v2_bypasses_replacement_catalog():
    assert prepare_luna_v2_catalog(_catalog("v2")) == ("native-v2-bypass", None)


@pytest.mark.parametrize(
    "catalog",
    [
        {},
        {"models": []},
        {"models": [_catalog()["models"][1], _catalog()["models"][1]]},
        _catalog("v3"),
    ],
)
def test_ambiguous_or_unknown_catalog_fails_closed(catalog):
    with pytest.raises(CatalogWorkaroundError):
        prepare_luna_v2_catalog(catalog)


def test_adapter_applies_fresh_0600_catalog_and_cleans_it(tmp_path: Path, monkeypatch):
    temp_dir = tmp_path / "catalog space"
    temp_dir.mkdir()
    monkeypatch.setenv("TMPDIR", str(temp_dir))
    adapter, invocation = _invocation(tmp_path, _fake_codex(tmp_path))

    result = adapter.invoke(invocation)

    assert result.exit_code == 0
    assert '"status":"applied"' in result.stderr
    capture = json.loads((tmp_path / "capture.json").read_text(encoding="utf-8"))
    assert capture["stdin"] == "do the thing"
    assert capture["mode"] == 0o600
    assert capture["catalog"]["models"][1]["multi_agent_version"] == "v2"
    assert capture["catalog"]["models"][0]["multi_agent_version"] == "v2"
    assert not Path(capture["catalog_path"]).exists()
    assert capture["argv"][0] == "-c"
    assert capture["argv"][2:4] == ["exec", "--skip-git-repo-check"]


def test_adapter_auto_retires_override_for_native_v2(tmp_path: Path):
    adapter, invocation = _invocation(
        tmp_path,
        _fake_codex(tmp_path),
        FAKE_LUNA_VERSION="v2",
    )

    result = adapter.invoke(invocation)

    assert result.exit_code == 0
    assert '"status":"native-v2-bypass"' in result.stderr
    capture = json.loads((tmp_path / "capture.json").read_text(encoding="utf-8"))
    assert capture["catalog_path"] is None
    assert capture["argv"][:2] == ["exec", "--skip-git-repo-check"]


@pytest.mark.parametrize(
    "extra_env",
    [
        {"FAKE_BAD_JSON": "1"},
        {"FAKE_DUPLICATE_LUNA": "1"},
        {"FAKE_DEBUG_EXIT": "3"},
    ],
)
def test_wrapper_never_launches_codex_with_unproven_catalog(tmp_path: Path, extra_env):
    adapter, invocation = _invocation(tmp_path, _fake_codex(tmp_path), **extra_env)

    result = adapter.invoke(invocation)

    assert result.exit_code == 78
    assert "failed closed" in result.stderr
    assert not (tmp_path / "capture.json").exists()


def test_start_failure_removes_catalog(tmp_path: Path, monkeypatch):
    temp_dir = tmp_path / "catalog-tmp"
    temp_dir.mkdir()
    monkeypatch.setenv("TMPDIR", str(temp_dir))
    adapter, invocation = _invocation(
        tmp_path,
        _fake_codex(tmp_path),
        FAKE_DELETE_AFTER_DEBUG="1",
    )

    result = adapter.invoke(invocation)

    assert result.exit_code == 78
    assert "failed closed" in result.stderr
    assert list(temp_dir.iterdir()) == []


def test_signal_forwards_to_codex_and_removes_catalog(tmp_path: Path):
    temp_dir = tmp_path / "catalog-tmp"
    temp_dir.mkdir()
    binary = _fake_codex(tmp_path)
    capture = tmp_path / "capture.json"
    started = tmp_path / "started"
    adapter = CodexAdapter(
        bin_path=str(binary),
        catalog_workaround=LUNA_V2_CATALOG_WORKAROUND,
    )
    command = adapter.build_command(
        HarnessInvocation(prompt="do the thing", worktree=tmp_path, model="gpt-5.6-luna")
    )
    environment = {
        **os.environ,
        "FAKE_CAPTURE": str(capture),
        "FAKE_STARTED": str(started),
        "FAKE_SLEEP": "1",
        "TMPDIR": str(temp_dir),
    }
    child = subprocess.Popen(
        command,
        cwd=tmp_path,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdin is not None
    child.stdin.write("do the thing")
    child.stdin.close()
    deadline = time.monotonic() + 5
    while not started.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert started.exists()

    child.send_signal(signal.SIGTERM)
    child.wait(timeout=5)

    assert child.returncode != 0
    assert list(temp_dir.iterdir()) == []
