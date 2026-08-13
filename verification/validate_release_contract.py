"""Validate the exact ActionQ wheel selected for a GitHub release."""

from __future__ import annotations

import argparse
from email import policy
from email.parser import BytesParser
import re
import sys
import tomllib
import zipfile
from pathlib import Path
from urllib.parse import urldefrag, urlparse

ROOT = Path(__file__).resolve().parents[1]
ADAPTER_NAME = "vuoro-adapter-kit"
ADAPTER_DIGEST_RE = re.compile(r"^sha256=(?P<digest>[0-9a-f]{64})$")
ADAPTER_PATH_RE = re.compile(
    r"^/bayleafwalker/vuoro/releases/download/"
    r"vuoro-adapter-kit-v(?P<release_version>[^/]+)/"
    r"vuoro_adapter_kit-(?P<wheel_version>[^-]+)-py3-none-any\.whl$"
)
ACTIONQ_WHEEL_RE = re.compile(r"^actionq-(?P<version>[^-]+)-.+\.whl$")


def _project_metadata() -> dict:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        return tomllib.load(stream)["project"]


def _adapter_requirement() -> str:
    requirements = [
        requirement
        for requirement in _project_metadata()["dependencies"]
        if requirement.startswith(f"{ADAPTER_NAME} @ ")
    ]
    if len(requirements) != 1:
        raise AssertionError("pyproject must declare exactly one adapter-kit URL")
    return requirements[0].split(" @ ", 1)[1]


def _locked_adapter_requirement() -> tuple[str, str]:
    with (ROOT / "uv.lock").open("rb") as stream:
        lock = tomllib.load(stream)
    packages = [package for package in lock["package"] if package["name"] == ADAPTER_NAME]
    if len(packages) != 1:
        raise AssertionError("uv.lock must contain exactly one adapter-kit package")
    package = packages[0]
    wheels = package.get("wheels", [])
    if len(wheels) != 1:
        raise AssertionError("uv.lock must contain exactly one adapter-kit wheel")
    digest = wheels[0]["hash"]
    if not digest.startswith("sha256:"):
        raise AssertionError("adapter-kit lock hash must be sha256")
    return package["source"]["url"], digest.removeprefix("sha256:")


def _validate_adapter_pin(url: str) -> str:
    plain_url, fragment = urldefrag(url)
    parsed = urlparse(plain_url)
    if parsed.scheme != "https" or parsed.netloc != "github.com":
        raise AssertionError("adapter-kit dependency must use an HTTPS GitHub URL")
    match = ADAPTER_PATH_RE.fullmatch(parsed.path)
    if match is None or match.group("release_version") != match.group("wheel_version"):
        raise AssertionError("adapter-kit URL must identify one versioned GitHub wheel")
    digest_match = ADAPTER_DIGEST_RE.fullmatch(fragment)
    if digest_match is None:
        raise AssertionError("adapter-kit dependency must include a sha256 URL fragment")
    return digest_match.group("digest")


def validate_wheel(wheel_path: Path, tag: str | None = None) -> None:
    if not wheel_path.is_file() or wheel_path.suffix != ".whl":
        raise AssertionError(f"wheel does not exist: {wheel_path}")
    wheel_match = ACTIONQ_WHEEL_RE.fullmatch(wheel_path.name)
    if wheel_match is None:
        raise AssertionError(f"wheel is not an ActionQ wheel: {wheel_path.name}")

    project = _project_metadata()
    if project["version"] != "0.1.20":
        raise AssertionError("the release contract is frozen to ActionQ 0.1.20")
    if "actionq-contracts==0.1.1" not in project["dependencies"]:
        raise AssertionError("actionq-contracts must remain pinned at 0.1.1")

    pyproject_url = _adapter_requirement()
    pyproject_digest = _validate_adapter_pin(pyproject_url)
    lock_url, lock_digest = _locked_adapter_requirement()
    if lock_url != urldefrag(pyproject_url)[0]:
        raise AssertionError("uv.lock adapter URL does not match pyproject.toml")
    if lock_digest != pyproject_digest:
        raise AssertionError("uv.lock adapter digest does not match pyproject.toml")

    with zipfile.ZipFile(wheel_path) as wheel:
        metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise AssertionError("wheel must contain exactly one dist-info/METADATA file")
        metadata = BytesParser(policy=policy.default).parsebytes(wheel.read(metadata_names[0]))

    if metadata["Name"] != "actionq":
        raise AssertionError(f"wheel metadata name is not actionq: {metadata['Name']!r}")
    version = metadata["Version"]
    if version != wheel_match.group("version") or version != project["version"]:
        raise AssertionError("wheel filename, metadata, and source versions differ")
    expected_tag = f"v{version}"
    if tag is not None and tag != expected_tag:
        raise AssertionError(f"release tag {tag!r} does not match {expected_tag!r}")
    requirements = metadata.get_all("Requires-Dist", [])
    if "actionq-contracts==0.1.1" not in requirements:
        raise AssertionError("wheel metadata must retain actionq-contracts 0.1.1")
    if not any(requirement.endswith(pyproject_url) for requirement in requirements):
        raise AssertionError("wheel metadata does not preserve the pinned adapter URL and digest")
    print(f"validated {wheel_path.name}: tag={expected_tag}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--tag")
    args = parser.parse_args(argv)
    try:
        validate_wheel(args.wheel, args.tag)
    except (AssertionError, KeyError, OSError, zipfile.BadZipFile) as exc:
        print(f"release contract failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
