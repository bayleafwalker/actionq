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
SCHEMA_RUNTIME_NAME = "vuoro-schema-runtime"
EXPECTED_SHARED_DIGESTS = {
    ADAPTER_NAME: "0037898a4c9f01720a42302365b0172ecd203732070326ea2abdf549a44bf0c2",
    SCHEMA_RUNTIME_NAME: "b66c9357c99aa9e1a7353991ce54105a8621958ecfac47f8c121d80b90b77912",
}
DEPENDENCY_DIGEST_RE = re.compile(r"^sha256=(?P<digest>[0-9a-f]{64})$")
DEPENDENCY_PATH_RE = re.compile(
    r"^/bayleafwalker/vuoro/releases/download/"
    r"(?P<tag_name>vuoro-(?:adapter-kit|schema-runtime))-v(?P<release_version>[^/]+)/"
    r"(?P<wheel_name>vuoro_(?:adapter_kit|schema_runtime))-(?P<wheel_version>[^-]+)-py3-none-any\.whl$"
)
ACTIONQ_WHEEL_RE = re.compile(r"^actionq-(?P<version>[^-]+)-.+\.whl$")


def _project_metadata() -> dict:
    with (ROOT / "pyproject.toml").open("rb") as stream:
        return tomllib.load(stream)["project"]


def _dependency_requirement(name: str) -> str:
    requirements = [
        requirement
        for requirement in _project_metadata()["dependencies"]
        if requirement.startswith(f"{name} @ ")
    ]
    if len(requirements) != 1:
        raise AssertionError(f"pyproject must declare exactly one {name} URL")
    return requirements[0].split(" @ ", 1)[1]


def _adapter_requirement() -> str:
    return _dependency_requirement(ADAPTER_NAME)


def _schema_runtime_requirement() -> str:
    return _dependency_requirement(SCHEMA_RUNTIME_NAME)


def _locked_dependency_requirement(name: str) -> tuple[str, str]:
    with (ROOT / "uv.lock").open("rb") as stream:
        lock = tomllib.load(stream)
    packages = [package for package in lock["package"] if package["name"] == name]
    if len(packages) != 1:
        raise AssertionError(f"uv.lock must contain exactly one {name} package")
    package = packages[0]
    wheels = package.get("wheels", [])
    if len(wheels) != 1:
        raise AssertionError(f"uv.lock must contain exactly one {name} wheel")
    digest = wheels[0]["hash"]
    if not digest.startswith("sha256:"):
        raise AssertionError(f"{name} lock hash must be sha256")
    return package["source"]["url"], digest.removeprefix("sha256:")


def _locked_adapter_requirement() -> tuple[str, str]:
    return _locked_dependency_requirement(ADAPTER_NAME)


def _locked_schema_runtime_requirement() -> tuple[str, str]:
    return _locked_dependency_requirement(SCHEMA_RUNTIME_NAME)


def _validate_dependency_pin(url: str, *, name: str) -> str:
    plain_url, fragment = urldefrag(url)
    parsed = urlparse(plain_url)
    if parsed.scheme != "https" or parsed.netloc != "github.com":
        raise AssertionError(f"{name} dependency must use an HTTPS GitHub URL")
    match = DEPENDENCY_PATH_RE.fullmatch(parsed.path)
    if (
        match is None
        or match.group("tag_name") != name
        or match.group("wheel_name") != name.replace("-", "_")
        or match.group("release_version") != match.group("wheel_version")
    ):
        raise AssertionError(f"{name} URL must identify one versioned GitHub wheel")
    digest_match = DEPENDENCY_DIGEST_RE.fullmatch(fragment)
    if digest_match is None:
        raise AssertionError(f"{name} dependency must include a sha256 URL fragment")
    digest = digest_match.group("digest")
    if digest != EXPECTED_SHARED_DIGESTS[name]:
        raise AssertionError(f"{name} dependency must use its accepted release digest")
    return digest


def _validate_adapter_pin(url: str) -> str:
    return _validate_dependency_pin(url, name=ADAPTER_NAME)


def _validate_schema_runtime_pin(url: str) -> str:
    return _validate_dependency_pin(url, name=SCHEMA_RUNTIME_NAME)


def validate_wheel(wheel_path: Path, tag: str | None = None) -> None:
    if not wheel_path.is_file() or wheel_path.suffix != ".whl":
        raise AssertionError(f"wheel does not exist: {wheel_path}")
    wheel_match = ACTIONQ_WHEEL_RE.fullmatch(wheel_path.name)
    if wheel_match is None:
        raise AssertionError(f"wheel is not an ActionQ wheel: {wheel_path.name}")

    project = _project_metadata()
    # Frozen to the version the Vuoro composition pins, and moved only by the
    # tranche that publishes a new one. W4 is that tranche: it adds the
    # federation serving surface, which cannot reach a deployment without a
    # wheel, and a released wheel cannot be amended afterwards.
    if project["version"] != "0.1.27":
        raise AssertionError("the release contract is frozen to ActionQ 0.1.27")
    if "actionq-contracts==0.1.1" not in project["dependencies"]:
        raise AssertionError("actionq-contracts must remain pinned at 0.1.1")

    dependency_urls: list[str] = []
    for name, requirement, locked, validator in (
        (ADAPTER_NAME, _adapter_requirement, _locked_adapter_requirement, _validate_adapter_pin),
        (
            SCHEMA_RUNTIME_NAME,
            _schema_runtime_requirement,
            _locked_schema_runtime_requirement,
            _validate_schema_runtime_pin,
        ),
    ):
        pyproject_url = requirement()
        pyproject_digest = validator(pyproject_url)
        lock_url, lock_digest = locked()
        if lock_url != urldefrag(pyproject_url)[0]:
            raise AssertionError(f"uv.lock {name} URL does not match pyproject.toml")
        if lock_digest != pyproject_digest:
            raise AssertionError(f"uv.lock {name} digest does not match pyproject.toml")
        dependency_urls.append(pyproject_url)

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
    for dependency_url in dependency_urls:
        if not any(requirement.endswith(dependency_url) for requirement in requirements):
            raise AssertionError(
                "wheel metadata does not preserve every pinned shared dependency URL and digest"
            )
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
