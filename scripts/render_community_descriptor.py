#!/usr/bin/env python3
"""Render a DuckDB community-extensions descriptor for `flight`."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = "community/description.template.yml"


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def first_match(pattern: str, text: str, label: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    if match is None:
        raise ValueError(f"could not find {label}")
    return match.group(1)


def project_version() -> str:
    cargo = first_match(
        r'^version\s*=\s*"([^"]+)"',
        read("Cargo.toml"),
        "Cargo.toml package version",
    )
    makefile = first_match(
        r"^EXTENSION_VERSION=([0-9]+\.[0-9]+\.[0-9]+)$",
        read("Makefile"),
        "Makefile EXTENSION_VERSION",
    )
    template = first_match(
        r"^\s*version:\s*([0-9]+\.[0-9]+\.[0-9]+)\s*$",
        read(TEMPLATE),
        "community descriptor version",
    )
    versions = {"Cargo.toml": cargo, "Makefile": makefile, TEMPLATE: template}
    if len(set(versions.values())) != 1:
        details = ", ".join(f"{path}={version}" for path, version in versions.items())
        raise ValueError(f"project versions differ: {details}")
    return cargo


def validate_ref(ref: str, version: str) -> None:
    if re.fullmatch(r"[0-9a-f]{40}", ref):
        return
    if ref == f"v{version}":
        return
    raise ValueError(f"release ref must be a 40-character commit hash or v{version}; got {ref!r}")


def render(ref: str, github: str, maintainer: str, version: str) -> str:
    text = read(TEMPLATE)
    text = re.sub(
        r"^(\s*version:\s*)[^\s]+(\s*)$",
        rf"\g<1>{version}\2",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^(\s*github:\s*)[^\s#]+.*$",
        rf"\g<1>{github}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^(\s*ref:\s*)[^\s#]+.*$",
        rf"\g<1>{ref}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^(\s*-\s*)TODO_GITHUB_USERNAME\s*$",
        rf"\g<1>{maintainer}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    return text


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref", required=True, help="release tag or 40-character commit hash")
    parser.add_argument("--github", required=True, help="GitHub repository as OWNER/REPO")
    parser.add_argument("--maintainer", required=True, help="GitHub username for descriptor maintainers")
    parser.add_argument(
        "--out",
        default="build/community-extensions/extensions/flight/description.yml",
        help="output descriptor path, relative to the repository root",
    )
    args = parser.parse_args()

    version = project_version()
    validate_ref(args.ref, version)

    if not re.fullmatch(r"[^/\s]+/[^/\s]+", args.github):
        raise SystemExit(f"--github must look like OWNER/REPO; got {args.github!r}")
    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", args.maintainer):
        raise SystemExit(f"--maintainer must be a GitHub username; got {args.maintainer!r}")

    out = ROOT / args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(args.ref, args.github, args.maintainer, version), encoding="utf-8")
    try:
        display_path = out.relative_to(ROOT)
    except ValueError:
        display_path = out
    print(f"wrote {display_path} for flight {version} at {args.ref}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
