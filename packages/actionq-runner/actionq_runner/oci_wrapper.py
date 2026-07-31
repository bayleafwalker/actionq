"""Immutable in-container wrapper for the rootless OCI runner.

This module is copied into the pinned image.  It deliberately has no ActionQ
imports: the untrusted command sees only a detached clone of the exact bundle.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from pathlib import Path


def _git(repo: Path | None, *args: str) -> str:
    command = ["git"]
    if repo is not None:
        command += ["-c", f"safe.directory={repo}", "-C", os.fspath(repo)]
    completed = subprocess.run(command + list(args), text=True, capture_output=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or "git command failed")
    return completed.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--worker-uid", required=True, type=int)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a registered command is required")

    workspace = Path("/workspace")
    repo = workspace / "repo"
    candidate = Path(args.candidate)
    if repo.exists() or candidate.exists():
        raise RuntimeError("workspace was not empty")
    filesystem = os.statvfs(workspace)
    control = Path("/control")
    metadata = control / "runner-metadata.json"
    metadata.write_text(json.dumps({
        "capacity_bytes": filesystem.f_frsize * filesystem.f_blocks,
        "workspace_mode": oct(workspace.stat().st_mode & 0o777),
    }, sort_keys=True))
    os.chmod(metadata, 0o600)
    _git(None, "clone", "--quiet", args.source, os.fspath(repo))
    if _git(repo, "rev-parse", "HEAD") != args.source_commit:
        raise RuntimeError("source bundle did not select the exact commit")
    _git(repo, "remote", "remove", "origin")
    if _git(repo, "remote"):
        raise RuntimeError("clone retained a remote")
    for root, directories, files in os.walk(repo):
        os.chown(root, args.worker_uid, args.worker_uid)
        for name in directories:
            os.chown(Path(root) / name, args.worker_uid, args.worker_uid)
        for name in files:
            os.chown(Path(root) / name, args.worker_uid, args.worker_uid)
    os.chown(workspace, args.worker_uid, args.worker_uid)
    def drop_identity() -> None:
        os.setgroups([])
        os.setgid(args.worker_uid)
        os.setuid(args.worker_uid)
    child = subprocess.Popen(command, cwd=repo, preexec_fn=drop_identity, start_new_session=True)
    command_exit = child.wait()
    # A registered command owns exactly one process group.  Do not let a
    # daemonized descendant race trusted collection after its leader exits.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if command_exit == 0:
        if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
            raise RuntimeError("registered command left uncommitted files")
        head = _git(repo, "rev-parse", "HEAD")
        if subprocess.run(["git", "-c", f"safe.directory={repo}", "-C", os.fspath(repo),
                           "merge-base", "--is-ancestor",
                           args.source_commit, head], check=False).returncode:
            raise RuntimeError("candidate does not descend from source")
        if _git(repo, "remote"):
            raise RuntimeError("registered command added a remote")
        temporary = candidate.with_suffix(".tmp")
        _git(repo, "bundle", "create", os.fspath(temporary), "HEAD")
        os.chmod(temporary, 0o600)
        os.replace(temporary, candidate)
    metadata.write_text(json.dumps({
        "capacity_bytes": filesystem.f_frsize * filesystem.f_blocks,
        "workspace_mode": oct(workspace.stat().st_mode & 0o777),
        "command_exit": command_exit,
    }, sort_keys=True))
    os.chmod(metadata, 0o600)
    (control / "sealed").write_text("sealed\n")
    os.chmod(control / "sealed", 0o600)

    signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
    while True:
        signal.pause()


if __name__ == "__main__":
    sys.exit(main())
