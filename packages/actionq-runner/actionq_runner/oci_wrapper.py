"""Immutable PID-1 wrapper for the rootless OCI runner."""

from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path


_SAFE_GIT = [
    "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
    "-c", "filter.lfs.smudge=", "-c", "filter.lfs.required=false",
    "-c", "protocol.file.allow=always", "-c", "protocol.ext.allow=never",
]


def _git(repo: Path | None, *args: str) -> str:
    command = ["git", *_SAFE_GIT]
    if repo is not None:
        command += ["-C", os.fspath(repo)]
    completed = subprocess.run(command + list(args), text=True, capture_output=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


def _worker(args: argparse.Namespace, command: list[str]) -> int:
    """All Git touching worker-controlled state runs at the worker identity."""
    workspace = Path("/workspace")
    repo = workspace / "repo"
    candidate = Path(args.candidate)
    _git(None, "clone", "--quiet", args.source, os.fspath(repo))
    if _git(repo, "rev-parse", "HEAD") != args.source_commit:
        raise RuntimeError("source bundle did not select the exact commit")
    _git(repo, "remote", "remove", "origin")
    child = subprocess.Popen(command, cwd=repo, start_new_session=True)
    command_exit = child.wait()
    # Kill the command session, but do not trust this as the containment proof;
    # PID 1 also reaps and checks the complete container PID namespace below.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if command_exit:
        return command_exit
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise RuntimeError("registered command left uncommitted files")
    head = _git(repo, "rev-parse", "HEAD")
    if subprocess.run(["git", *_SAFE_GIT, "-C", os.fspath(repo), "merge-base",
                       "--is-ancestor", args.source_commit, head], check=False).returncode:
        raise RuntimeError("candidate does not descend from source")
    if _git(repo, "remote"):
        raise RuntimeError("registered command added a remote")
    temporary = candidate.with_suffix(".tmp")
    _git(repo, "bundle", "create", os.fspath(temporary), "HEAD")
    os.chmod(temporary, 0o600)
    os.replace(temporary, candidate)
    return 0


def _reap_all() -> None:
    """PID 1 must prove that no worker descendant survives sealing."""
    try:
        os.kill(-1, signal.SIGKILL)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            time.sleep(0.01)
    raise RuntimeError("worker descendant survived container-wide kill")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--worker-uid", required=True, type=int)
    parser.add_argument("--worker-internal", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a registered command is required")
    if args.worker_internal:
        return _worker(args, command)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    workspace = Path("/workspace")
    control = Path("/control")
    if (workspace / "repo").exists() or Path(args.candidate).exists():
        raise RuntimeError("workspace was not empty")
    filesystem = os.statvfs(workspace)
    worker_source = workspace / "source.bundle"
    shutil.copyfile(args.source, worker_source, follow_symlinks=False)
    os.chmod(worker_source, 0o400)
    os.chown(worker_source, args.worker_uid, args.worker_uid)
    os.chown(workspace, args.worker_uid, args.worker_uid)
    args.source = os.fspath(worker_source)

    def drop_identity() -> None:
        os.setgroups([])
        os.setgid(args.worker_uid)
        os.setuid(args.worker_uid)

    worker_argv = [sys.executable, os.fspath(Path(__file__)), *sys.argv[1:]]
    worker_argv.insert(2, "--worker-internal")
    worker_argv[worker_argv.index("--source") + 1] = os.fspath(worker_source)
    worker = subprocess.Popen(worker_argv, preexec_fn=drop_identity)
    command_exit = worker.wait()
    _reap_all()
    if any(control.iterdir()):
        raise RuntimeError("worker-controlled execution reached the trusted control directory")
    metadata = control / "runner-metadata.json"
    metadata.write_text(json.dumps({
        "capacity_bytes": filesystem.f_frsize * filesystem.f_blocks,
        "workspace_mode": oct(workspace.stat().st_mode & 0o777),
        "command_exit": command_exit,
        "descendants_reaped": True,
    }, sort_keys=True))
    os.chmod(metadata, 0o600)
    (control / "sealed").write_text("sealed\n")
    os.chmod(control / "sealed", 0o600)
    while True:
        signal.pause()


if __name__ == "__main__":
    sys.exit(main())
