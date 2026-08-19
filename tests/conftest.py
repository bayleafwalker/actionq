import getpass
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import json
import base64
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "packages/actionq-runner"))

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from actionq_runner.identity import sign_runner_request


MIGRATION_ROLE = "actionq_migration"
RUNTIME_ROLE = "actionq_runtime"
COMPLETION_INGEST_ROLE = "actionq_completion_ingest"
COMPLETION_READ_ROLE = "actionq_completion_read"
_POSTGRES_STATE = None


@pytest.fixture(scope="session", autouse=True)
def runner_identity(tmp_path_factory):
    root = tmp_path_factory.mktemp("runner-identity")
    private_path = root / "runner.pem"
    registry_path = root / "registry.json"
    key = Ed25519PrivateKey.generate()
    private_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    private_path.chmod(0o600)
    public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    registry_path.write_text(json.dumps({"worker:test": base64.b64encode(public).decode(),
                                         "worker:one": base64.b64encode(public).decode(),
                                         "actionq-daemon:test": base64.b64encode(public).decode(),
                                         "runner:devbox": base64.b64encode(public).decode()}))
    previous = os.environ.get("ACTIONQ_RUNNER_IDENTITY_REGISTRY")
    previous_private = os.environ.get("ACTIONQ_RUNNER_PRIVATE_KEY")
    os.environ["ACTIONQ_RUNNER_IDENTITY_REGISTRY"] = str(registry_path)
    os.environ["ACTIONQ_RUNNER_PRIVATE_KEY"] = str(private_path)
    yield {"private_key": private_path, "registry": registry_path}
    if previous is None:
        os.environ.pop("ACTIONQ_RUNNER_IDENTITY_REGISTRY", None)
    else:
        os.environ["ACTIONQ_RUNNER_IDENTITY_REGISTRY"] = previous
    if previous_private is None:
        os.environ.pop("ACTIONQ_RUNNER_PRIVATE_KEY", None)
    else:
        os.environ["ACTIONQ_RUNNER_PRIVATE_KEY"] = previous_private


@pytest.fixture
def signed_runner_proof(runner_identity):
    def sign(runner_id: str, operation: str, resource: str):
        return sign_runner_request(
            runner_identity["private_key"], runner_id=runner_id, operation=operation,
            resource=resource, request_id=str(__import__("uuid").uuid4()),
        )
    return sign


@pytest.fixture(autouse=True)
def daemon_test_result_cas(tmp_path: Path, request, monkeypatch):
    """Give legacy daemon unit doubles an explicitly test-only CAS.

    Production ``Daemon._cas`` still requires ``DaemonConfig.artifact_root``
    and uses the strict FilesystemCAS checks.  Most older coordinator tests
    predate result artifacts and intentionally focus on lifecycle wiring, so
    this fixture supplies an unsafe temporary CAS only to those unit doubles.
    Tests for the fail-closed root boundary opt out with the marker below.
    """
    if request.node.get_closest_marker("real_daemon_result_cas"):
        yield
        return
    from actionq.daemon import Daemon
    from actionq_runner import FilesystemCAS

    root = Path("/dev/shm") / f"actionq-daemon-test-cas-{uuid.uuid4().hex}"
    root.mkdir(mode=0o700)

    def test_cas(self):
        if self.config.artifact_root is None:
            # Real ActionctlClient doubles settle through a subprocess. Point
            # that subprocess at the same test-only CAS that the daemon used
            # to write the verified result packet; otherwise the full-suite
            # harness supplies bytes to one root and verifies another.
            if hasattr(self.client, "artifact_root"):
                self.client.artifact_root = root
            return FilesystemCAS(root, allow_unsafe_test_root=True)
        return FilesystemCAS(self.config.artifact_root, allow_unsafe_test_root=True)

    monkeypatch.setattr(Daemon, "_cas", test_cas)
    yield
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def server_cas_root():
    """Provision a real owner-controlled CAS outside test-runner staging."""
    root = Path("/dev/shm") / f"actionq-test-cas-{uuid.uuid4().hex}"
    root.mkdir(mode=0o700)
    yield root
    shutil.rmtree(root, ignore_errors=True)


def _needs_postgres(config) -> bool:
    args = [str(argument) for argument in config.args]
    if not args:
        return True
    return any(
        Path(argument).name == "tests"
        or "integration" in argument
        or "claim_authority" in argument
        or "runner_auth" in argument
        or "cross_authority" in argument
        or "dispatch_settlement" in argument
        for argument in args
    )


def _start_postgres() -> dict:
    import psycopg
    from psycopg import sql

    external_url = os.environ.get("ACTIONQ_TEST_URL")
    if external_url:
        runtime_url = external_url.replace("//actionq:", "//actionq_runtime:")
        migration_url = external_url.replace("//actionq:", "//actionq_migration:")
        return {"external": True, "pg_ctl": None,
                "urls": {"admin": external_url, "migration": migration_url, "runtime": runtime_url,
                        "completion_ingest": runtime_url, "completion_read": runtime_url},
                "previous_env": {name: os.environ.get(name) for name in ("ACTIONQ_TEST_URL", "ACTIONQ_TEST_MIGRATION_URL", "ACTIONQ_TEST_RUNTIME_URL", "ACTIONQ_RUNTIME_ROLE", "ACTIONQ_COMPLETION_INGEST_ROLE", "ACTIONQ_COMPLETION_READ_ROLE")}}

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
        "completion_ingest": f"{base} user={COMPLETION_INGEST_ROLE}",
        "completion_read": f"{base} user={COMPLETION_READ_ROLE}",
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
                sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(COMPLETION_INGEST_ROLE))
            )
            conn.execute(
                sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(COMPLETION_READ_ROLE))
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
                "ACTIONQ_COMPLETION_INGEST_ROLE",
                "ACTIONQ_COMPLETION_READ_ROLE",
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
    if not _POSTGRES_STATE.get("external"):
        os.environ["ACTIONQ_COMPLETION_INGEST_ROLE"] = COMPLETION_INGEST_ROLE
        os.environ["ACTIONQ_COMPLETION_READ_ROLE"] = COMPLETION_READ_ROLE


def pytest_unconfigure(config) -> None:
    global _POSTGRES_STATE
    if _POSTGRES_STATE is None:
        return
    for name, value in _POSTGRES_STATE["previous_env"].items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    if not _POSTGRES_STATE.get("external"):
        subprocess.run(
            [str(_POSTGRES_STATE["pg_ctl"]), "-D", str(_POSTGRES_STATE["data"]), "-m", "fast", "-w", "stop"],
            check=False, capture_output=True, text=True,
        )
    if not _POSTGRES_STATE.get("external"):
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


def agentops_root() -> Path:
    """Where the agentops contracts repo is checked out.

    Same resolution as tests/test_managed_dispatch.py: the env var wins, and the
    sibling checkout is the default, so a normal /projects/dev layout needs no
    setup.
    """
    override = os.environ.get("ACTIONQ_AGENTOPS_ROOT")
    if override:
        return Path(override)
    return Path(__file__).parents[2] / "agentops"


@pytest.fixture
def session_artifact_validator() -> Path:
    """Path to agentops' session-artifact validator.

    Previously read straight from AGENTOPS_SESSION_ARTIFACT_VALIDATOR, which CI
    sets and a developer shell does not -- so these tests passed in CI and failed
    locally, and the local failure got carried from handoff to handoff as
    "pre-existing". Deriving it from the agentops root removes that divergence;
    the env var still wins where it is set.

    Skips, rather than fails, when the checkout is absent: a missing sibling repo
    is a setup fact, not a defect in the code under test.
    """
    override = os.environ.get("AGENTOPS_SESSION_ARTIFACT_VALIDATOR")
    validator = (
        Path(override)
        if override
        else agentops_root()
        / "templates/dispatch/scripts/validate_session_mechanization_artifacts.py"
    )
    if not validator.is_file():
        pytest.skip(
            f"agentops session-artifact validator not found at {validator}; "
            "set ACTIONQ_AGENTOPS_ROOT or AGENTOPS_SESSION_ARTIFACT_VALIDATOR"
        )
    return validator
