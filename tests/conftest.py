import getpass
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MIGRATION_ROLE = "actionq_migration"
RUNTIME_ROLE = "actionq_runtime"
_POSTGRES_STATE = None


def _needs_postgres(config) -> bool:
    args = [str(argument) for argument in config.args]
    if not args:
        return True
    return any(
        argument == "tests"
        or "integration" in argument
        or "claim_authority" in argument
        for argument in args
    )


def _start_postgres() -> dict:
    import psycopg
    from psycopg import sql

    binaries = {command: shutil.which(command) for command in ("initdb", "pg_ctl")}
    missing = sorted(command for command, path in binaries.items() if path is None)
    if missing:
        raise pytest.UsageError(f"PostgreSQL server binaries are required: {missing}")

    root = Path(tempfile.mkdtemp(prefix="actionq-pg-"))
    data = root / "data"
    socket_dir = root / "socket"
    socket_dir.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    initdb = Path(binaries["initdb"] or "initdb").resolve()
    pg_ctl = Path(binaries["pg_ctl"] or "pg_ctl").resolve()
    initdb_args = [
        str(initdb),
        "--no-locale",
        "--encoding=UTF8",
        "--auth=trust",
        "-D",
        str(data),
    ]
    adjacent_share = initdb.parents[1] / "share" / "postgresql"
    if (adjacent_share / "postgres.bki").exists():
        initdb_args.extend(["-L", str(adjacent_share)])
    try:
        subprocess.run(initdb_args, check=True, capture_output=True, text=True)
        subprocess.run(
            [
                str(pg_ctl),
                "-D",
                str(data),
                "-l",
                str(root / "postgres.log"),
                "-o",
                f"-F -h '' -k {socket_dir} -p {port}",
                "-w",
                "start",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise

    base = f"dbname=postgres host={socket_dir} port={port}"
    urls = {
        "admin": f"{base} user={getpass.getuser()}",
        "migration": f"{base} user={MIGRATION_ROLE}",
        "runtime": f"{base} user={RUNTIME_ROLE}",
    }
    try:
        with psycopg.connect(urls["admin"], autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(MIGRATION_ROLE))
            )
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(RUNTIME_ROLE))
            )
            conn.execute(
                sql.SQL("GRANT CREATE ON DATABASE postgres TO {}").format(
                    sql.Identifier(MIGRATION_ROLE)
                )
            )
    except Exception:
        subprocess.run(
            [str(pg_ctl), "-D", str(data), "-m", "fast", "-w", "stop"],
            check=False,
            capture_output=True,
            text=True,
        )
        shutil.rmtree(root, ignore_errors=True)
        raise
    return {
        "root": root,
        "data": data,
        "pg_ctl": pg_ctl,
        "urls": urls,
        "previous_env": {
            name: os.environ.get(name)
            for name in (
                "ACTIONQ_TEST_URL",
                "ACTIONQ_TEST_MIGRATION_URL",
                "ACTIONQ_TEST_RUNTIME_URL",
                "ACTIONQ_RUNTIME_ROLE",
            )
        },
    }


def pytest_configure(config) -> None:
    global _POSTGRES_STATE
    if not _needs_postgres(config):
        return
    _POSTGRES_STATE = _start_postgres()
    urls = _POSTGRES_STATE["urls"]
    os.environ["ACTIONQ_TEST_URL"] = urls["admin"]
    os.environ["ACTIONQ_TEST_MIGRATION_URL"] = urls["migration"]
    os.environ["ACTIONQ_TEST_RUNTIME_URL"] = urls["runtime"]
    os.environ["ACTIONQ_RUNTIME_ROLE"] = RUNTIME_ROLE


def pytest_unconfigure(config) -> None:
    global _POSTGRES_STATE
    if _POSTGRES_STATE is None:
        return
    for name, value in _POSTGRES_STATE["previous_env"].items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    subprocess.run(
        [
            str(_POSTGRES_STATE["pg_ctl"]),
            "-D",
            str(_POSTGRES_STATE["data"]),
            "-m",
            "fast",
            "-w",
            "stop",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    shutil.rmtree(_POSTGRES_STATE["root"], ignore_errors=True)
    _POSTGRES_STATE = None


@pytest.fixture(scope="session")
def postgres_urls():
    if _POSTGRES_STATE is None:
        pytest.fail("the hermetic PostgreSQL harness was not started")
    return _POSTGRES_STATE["urls"]


class RoleAwareCliRunner(CliRunner):
    def invoke(self, cli, args=None, **kwargs):
        command_args = list(args or ())
        if "migrate" in command_args:
            os.environ["ACTIONQ_URL"] = os.environ["ACTIONQ_TEST_MIGRATION_URL"]
        else:
            os.environ["ACTIONQ_URL"] = os.environ["ACTIONQ_TEST_RUNTIME_URL"]
        try:
            return super().invoke(cli, args, **kwargs)
        finally:
            os.environ["ACTIONQ_URL"] = os.environ["ACTIONQ_TEST_RUNTIME_URL"]


@pytest.fixture
def actionq_cli_runner(monkeypatch):
    monkeypatch.setenv("ACTIONQ_URL", os.environ["ACTIONQ_TEST_RUNTIME_URL"])
    return RoleAwareCliRunner()
