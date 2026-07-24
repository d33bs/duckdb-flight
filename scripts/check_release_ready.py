#!/usr/bin/env python3
"""Validate release-critical DuckDB community extension metadata."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXTENSION_NAME = "flight"


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def first_match(pattern: str, text: str, label: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    if match is None:
        raise ValueError(f"could not find {label}")
    return match.group(1)


def duckdb_crate_version(duckdb_version: str) -> str:
    match = re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)", duckdb_version)
    if match is None:
        raise ValueError(f"invalid DuckDB version {duckdb_version!r}")
    _major, minor, patch = (int(part) for part in match.groups())
    return f"1.1{minor:02d}{patch:02d}.0"


def has_todo_value(text: str) -> bool:
    return bool(re.search(r"\bTODO[_A-Z0-9]*\b", text))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--description-path",
        default="community/description.template.yml",
        help="descriptor to validate, relative to the repository root",
    )
    parser.add_argument(
        "--strict-community-ref",
        action="store_true",
        help="fail unless repo metadata is final and repo.ref is immutable",
    )
    args = parser.parse_args()

    failures: list[str] = []
    warnings: list[str] = []

    makefile = read("Makefile")
    workflow = read(".github/workflows/MainDistributionPipeline.yml")
    cargo = read("Cargo.toml")
    description = read(args.description_path)

    try:
        cargo_name = first_match(
            r'^name\s*=\s*"([^"]+)"',
            cargo,
            "Cargo.toml package name",
        )
        cargo_version = first_match(
            r'^version\s*=\s*"([^"]+)"',
            cargo,
            "Cargo.toml package version",
        )
        make_name = first_match(
            r"^EXTENSION_NAME=([A-Za-z0-9_]+)$",
            makefile,
            "Makefile EXTENSION_NAME",
        )
        make_extension_version = first_match(
            r"^EXTENSION_VERSION=([0-9]+\.[0-9]+\.[0-9]+)$",
            makefile,
            "Makefile EXTENSION_VERSION",
        )
        make_duckdb = first_match(
            r"^TARGET_DUCKDB_VERSION=(v\d+\.\d+\.\d+)$",
            makefile,
            "Makefile TARGET_DUCKDB_VERSION",
        )
        make_test_duckdb = first_match(
            r"^DUCKDB_TEST_VERSION=(\d+\.\d+\.\d+)$",
            makefile,
            "Makefile DUCKDB_TEST_VERSION",
        )
        workflow_name = first_match(
            r"^\s*extension_name:\s*([A-Za-z0-9_]+)\s*$",
            workflow,
            "workflow extension_name",
        )
        workflow_duckdb = first_match(
            r"^\s*duckdb_version:\s*(v\d+\.\d+\.\d+)$",
            workflow,
            "workflow duckdb_version",
        )
        cargo_duckdb = first_match(
            r'duckdb\s*=\s*\{\s*version\s*=\s*"=([0-9]+\.[0-9]+\.[0-9]+)"',
            cargo,
            "Cargo.toml exact duckdb crate pin",
        )
        descriptor_name = first_match(
            r"^\s*name:\s*([A-Za-z0-9_]+)\s*$",
            description,
            "description extension.name",
        )
        descriptor_version = first_match(
            r"^\s*version:\s*([0-9]+\.[0-9]+\.[0-9]+)\s*$",
            description,
            "description extension.version",
        )
        descriptor_ref = first_match(
            r"^\s*ref:\s*([^\s#]+)",
            description,
            "description repo.ref",
        )
    except ValueError as exc:
        failures.append(str(exc))
    else:
        names = {
            "Cargo.toml": cargo_name,
            "Makefile": make_name,
            "workflow": workflow_name,
            args.description_path: descriptor_name,
        }
        for source, name in names.items():
            if name != EXTENSION_NAME:
                failures.append(f"{source} names extension {name!r}, expected {EXTENSION_NAME!r}")

        versions = {
            "Cargo.toml": cargo_version,
            "Makefile": make_extension_version,
            args.description_path: descriptor_version,
        }
        if len(set(versions.values())) != 1:
            detail = ", ".join(f"{source}={version}" for source, version in versions.items())
            failures.append(f"extension version drift: {detail}")

        if make_duckdb != workflow_duckdb:
            failures.append(
                f"DuckDB version drift: Makefile has {make_duckdb}, workflow has {workflow_duckdb}"
            )

        if make_test_duckdb != make_duckdb.removeprefix("v"):
            failures.append(
                "DuckDB test version drift: "
                f"DUCKDB_TEST_VERSION={make_test_duckdb}, TARGET_DUCKDB_VERSION={make_duckdb}"
            )

        expected_crate = duckdb_crate_version(make_duckdb)
        if cargo_duckdb != expected_crate:
            failures.append(
                "DuckDB crate pin drift: "
                f"Cargo.toml has {cargo_duckdb}, expected {expected_crate} for {make_duckdb}"
            )

        immutable_ref = bool(
            re.fullmatch(r"[0-9a-f]{40}", descriptor_ref)
            or re.fullmatch(r"v\d+\.\d+\.\d+", descriptor_ref)
        )
        if not immutable_ref:
            message = (
                "description repo.ref should be an immutable release tag or 40-character "
                f"commit hash before community submission; current value is {descriptor_ref!r}"
            )
            if args.strict_community_ref:
                failures.append(message)
            else:
                warnings.append(message)

    descriptor_checks = {
        "extension.language": r"^\s*language:\s*Rust\s*$",
        "extension.build": r"^\s*build:\s*cargo\s*$",
        "extension.license": r"^\s*license:\s*MIT\s*$",
        "extension.requires_toolchains": r'^\s*requires_toolchains:\s*["\']rust;python3["\']\s*$',
    }
    for label, pattern in descriptor_checks.items():
        if re.search(pattern, description, re.MULTILINE) is None:
            failures.append(f"{args.description_path} missing or incorrect {label}")

    required_exclusions = ["wasm_mvp", "wasm_eh", "wasm_threads", "linux_amd64_musl"]
    excluded_platforms = first_match(
        r'^\s*excluded_platforms:\s*["\']([^"\']+)["\']\s*$',
        description,
        "description excluded_platforms",
    )
    for platform in required_exclusions:
        if platform not in excluded_platforms.split(";"):
            failures.append(f"{args.description_path} missing excluded platform {platform}")

    if has_todo_value(description):
        message = f"{args.description_path} still contains TODO release metadata"
        if args.strict_community_ref:
            failures.append(message)
        else:
            warnings.append(message)

    if failures:
        print("release readiness check failed:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    for warning in warnings:
        print(f"warning: {warning}")
    print("release readiness check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
