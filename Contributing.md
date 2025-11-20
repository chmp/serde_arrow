# Contributing Guide

Contributions are highly welcome! When authoring a pull request, please

1. Run the pre-commit checks locally (`uv run python x.py precommit`)
2. Add tests for any new code
3. Update the changelog ([`Changes.md`](Changes.md))

## Development Setup

All common tasks are bundled in the `x.py` script:

```bash
# format the code and run tests
uv run x.py precommit
```

Run `uv run x.py --help` for details. The script only uses standard Python
modules can can be run without installing further packages.

## Creating a release

1. Create a new branch with name `release/{VERSION}`
2. Update the `version` field in
   [`serde_arrow/Cargo.toml`](serde_arrow/Cargo.toml)
3. Updatt the changelog ([`Changes.md`](Changes.md))
4. Create a pull request
5. Merge the branch into main (requires maintainer access)
6. Create a new release via the GH UI tagged with `v{VERSION}` to trigger the
   release workflow (requires maintainer access)

## Running the benchmarks

1. `uv run x.py bench`
2. (optional)  `uv run x.py summarize-bench --update` to update the readme

On GitHub, execute `gh workflow run Bench --ref {BRANCH}`

## Adding a new arrow version

1. `uv run x.py add-arrow-version {VERSION}`
2. `uv run x.py precommit`

## Error format

Style:

- Use uppercase letters to start the error message
- Do not include trailing punctuation (e.g., "Not supported", not "Not supported.")

Common annotations:

- `field`: the path of the field affected by the error
- `data_type`: the Arrow data type of the field affected by the error
