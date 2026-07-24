.PHONY: clean clean_all fmt fmt-check clippy lint release-check test_rust

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=flight

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.4
DUCKDB_TEST_VERSION=1.5.4
EXTENSION_VERSION=0.1.0

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_rust:
	cargo test
test_debug: test_extension_debug
test_release: test_extension_release

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

lint: fmt-check clippy

release-check:
	python3 scripts/check_release_ready.py

clean: clean_build clean_rust
clean_all: clean_configure clean
