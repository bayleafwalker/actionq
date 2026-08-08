#!/usr/bin/env python3
"""Fail closed unless the checkout is the declared ActionQ release tag."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import subprocess
import sys
import tomllib


RELEASE_TAG = re.compile(r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()

    if not RELEASE_TAG.fullmatch(args.tag):
        parser.error("--tag must be a release tag in vX.Y.Z form")

    tag_target = git("rev-parse", "--verify", f"refs/tags/{args.tag}^{{commit}}")
    checkout = git("rev-parse", "HEAD")
    if tag_target != checkout:
        parser.error(f"tag {args.tag} resolves to {tag_target}, not checked-out HEAD {checkout}")

    project = tomllib.loads(Path("pyproject.toml").read_text())
    expected_tag = f"v{project['project']['version']}"
    if args.tag != expected_tag:
        parser.error(f"tag {args.tag} does not match pyproject version {expected_tag}")

    output = f"tag={args.tag}\nsha={checkout}\n"
    if args.github_output:
        with args.github_output.open("a") as stream:
            stream.write(output)
    else:
        sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
