# Contributing Guide

Contributions are highly welcome! When authoring a pull request, please

1. Run the pre-commit checks locally (`uv run python x.py precommit`)
2. Add tests for any new code
3. Update the changelog ([`Changes.md`](Changes.md))

## Development Setup

Install Rust and [`uv`](https://docs.astral.sh/uv/). The Python environment is
managed by `uv` from [`pyproject.toml`](pyproject.toml), including the packages
used by formatting, integration tests, examples, and benchmark summaries.

All common tasks are bundled in the `x.py` script:

```bash
# format the code and run tests
uv run python x.py precommit
```

Run `uv run python x.py --help` for details.

Useful commands:

- `uv run python x.py format`: format Rust sources and `x.py`
- `uv run python x.py check`: run linters and package checks
- `uv run python x.py test`: run default unit and integration tests
- `uv run python x.py test-unit --full`: run tests for all supported Arrow
  feature combinations
- `uv run python x.py test-unit {TEST_NAME}`: run tests matching a filter
- `uv run python x.py test-integration`: run the PyArrow integration tests
- `uv run python x.py doc --open`: build and open local documentation

Add `--backtrace` to `precommit`, `test`, `test-unit`, or `test-integration` to
set `RUST_BACKTRACE=1`.

The GitHub workflow files are generated from templates in `x.py`. If you change
workflow generation logic or supported Arrow versions, run:

```bash
uv run python x.py update-workflows
```

## `marrow`

This repository also contains [`marrow`](marrow/Readme.md), the minimal Arrow
interop crate used by `serde_arrow`. See [`marrow/Changes.md`](marrow/Changes.md)
for its changelog. The crates are versioned and released independently with
crate-scoped tags such as `marrow/v1.0.0` and `serde_arrow/v1.0.0`.


## Creating a release

1. Create a new branch with name `release/{VERSION}`
2. Update the crate version:
   - `serde_arrow`: [`serde_arrow/Cargo.toml`](serde_arrow/Cargo.toml)
   - `marrow`: [`marrow/Cargo.toml`](marrow/Cargo.toml)
3. If releasing both crates together, update `serde_arrow`'s `marrow`
   dependency to the new `marrow` version
4. Update the relevant changelog:
   - `serde_arrow`: [`Changes.md`](Changes.md)
   - `marrow`: [`marrow/Changes.md`](marrow/Changes.md)
5. Run `uv run python x.py precommit`
6. Create a pull request
7. Merge the branch into main (requires maintainer access)
8. Create a new release via the GH UI tagged with `serde_arrow/v{VERSION}` to
   trigger the release workflow (requires maintainer access). Use
   `marrow/v{VERSION}` to release a new `marrow` version.

Before publishing, the release workflows run package-specific checks and
`cargo package`. You can run those checks locally with:

```bash
cargo package -p serde_arrow --allow-dirty
cargo package -p marrow --allow-dirty
```

For release candidates, use normal Cargo prerelease versions such as
`0.15.0-rc.1`.

This repository uses the following tags:

- `serde_arrow/v*`: `serde_arrow` releases
- `marrow/v*`: `marrow` releases
- `v*`: `serde_arrow` releases `<0.15.0`

## Running the benchmarks

1. `uv run python x.py serde-arrow-bench`
2. (optional) `uv run python x.py summarize-bench --update` to update the
   Readme and `timings.png`

Use `uv run python x.py serde-arrow-bench --quick` for a shorter local run.

On GitHub, execute `uv run python x.py bench-remote`, or run:

```bash
gh workflow run Bench --ref {BRANCH}
```

The examples in the `benches` packages can be used to generate flamegraphs:

```bash
cargo flamegraph --example serialize_nested_struct --package serde_arrow_bench --profile bench
```

## Adding a new arrow version

1. Start with a clean working tree. The command edits every file that contains
   `arrow-version` markers and refuses to run with unstaged changes.
2. `uv run python x.py add-arrow-version {VERSION}`
3. `uv run python x.py precommit`

## Error format

Style:

- Use lowercase letters to start error messages
- Do not include trailing punctuation (e.g., "not supported", not "not
  supported.")

Common annotations:

- `field`: the path of the field affected by the error
- `data_type`: the Arrow data type of the field affected by the error
