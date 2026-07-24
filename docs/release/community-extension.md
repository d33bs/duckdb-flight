# Community Extension Release Notes

This repository is named and built as the DuckDB extension `flight`.

## Current Answer

If this repository were published today, the extension binary and SQL load name
are prepared for:

```sql
INSTALL flight FROM community;
LOAD flight;
```

The remaining release blockers are external to the local build:

- Publish the repository publicly on GitHub.
- Replace `TODO_OWNER/TODO_REPO`, `TODO_RELEASE_COMMIT_SHA`, and maintainers in
  `community/description.template.yml`.
- Open a PR to `duckdb/community-extensions` adding that descriptor as
  `extensions/flight/description.yml`.
- Confirm the community CI accepts the Rust `cargo` build for the pinned DuckDB
  version and excluded platforms.

## Local Release Checks

Run these before cutting a community-extension PR:

```sh
python3 scripts/check_release_ready.py
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
make clean_configure
make configure debug
make test_debug
make release
make test_release
```

## Descriptor Source

Use `community/description.template.yml` as the starting point for the Community
Extensions Repository descriptor. The format follows the current Rust community
extension example, including `build: cargo` and `requires_toolchains:
"rust;python3"`.

Render a submission-ready descriptor for a release tag or commit hash:

```sh
python3 scripts/render_community_descriptor.py \
  --ref v0.1.0 \
  --github OWNER/REPO \
  --maintainer GITHUB_USERNAME

python3 scripts/check_release_ready.py \
  --description-path build/community-extensions/extensions/flight/description.yml \
  --strict-community-ref
```

The generated file belongs in the Community Extensions Repository at
`extensions/flight/description.yml`.
