# Development process

All common tasks are bundled in the `x.py` script:

```bash
# format the code and run tests
uv run python x.py precommit
```

Run `python x.py --help` for details. The script only uses standard Python
modules can can be run without installing further packages.

## Creating a release

1. Create a new branch with name `release/{VERSION}`
2. Update the `version` field in
   [`serde_arrow/Cargo.toml`](serde_arrow/Cargo.toml)
3. Merge the branch into main
4. Create a new release via the GH UI tagged with `v{VERSION}` to trigger the
   release workflow

## Running the benchmarks

1. `uv run python x.py bench`
2. (optional)  `uv run python x.py summarize-bench --update` to update the readme

## Adding a new arrow version

1. `uv run python x.py add-arrow-version {VERSION}`
2. `uv run python x.py precommit`

## Error format

Style:

- Use uppercase letters to start the error message
- Do not include trailing punctuation (e.g., "Not supported", not "Not supported.")

Common annotations:

- `field`: the path of the field affected by the error
- `data_type`: the Arrow data type of the field affected by the error
