# Community Extension Release

This repository is named and built as the DuckDB extension `flight`.

## Install Name

The extension installs and loads as:

```sql
INSTALL flight FROM community;
LOAD flight;
```

## Community Descriptor

The descriptor template lives at `community/description.template.yml`. Render a
submission descriptor with the release ref, GitHub repository, and maintainer:

```sh
python3 scripts/render_community_descriptor.py \
  --ref v0.1.0 \
  --github OWNER/REPO \
  --maintainer GITHUB_USERNAME
```

The generated descriptor path is:

```text
build/community-extensions/extensions/flight/description.yml
```

Submit that file to the DuckDB Community Extensions Repository as:

```text
extensions/flight/description.yml
```

## Local Release Checks

Run:

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

Validate the rendered descriptor:

```sh
python3 scripts/check_release_ready.py \
  --description-path build/community-extensions/extensions/flight/description.yml \
  --strict-community-ref
```
