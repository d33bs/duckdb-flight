# Contributing

## Development Workflow

```sh
make
make test_rust
make lint
make test_debug
```

Install local hooks if you want pre-commit feedback before pushing:

```sh
prek install
prek run --all-files
```

If `prek` is not installed, `pre-commit run --all-files` works with the same
`.pre-commit-config.yaml`.

## Code Style

- Run `make fmt` before submitting Rust changes.
- Run `make lint` before opening a pull request.
- Add SQLLogic or Rust tests for behavior changes.
- Keep the public SQL surface documented in `SPEC.md`.
