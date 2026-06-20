# serde_arrow Monorepo

This repository contains two Rust crates for working with Apache Arrow data:

- [`serde_arrow`](serde_arrow/Readme.md): convert sequences of Rust objects to Arrow arrays and back again.
- [`marrow`](marrow/Readme.md): minimal Arrow-compatible data structures and interop helpers.

## Crates

| Crate | Documentation | Changelog |
| --- | --- | --- |
| `serde_arrow` | <https://docs.rs/serde_arrow> | [`serde_arrow/Changes.md`](serde_arrow/Changes.md) |
| `marrow` | <https://docs.rs/marrow> | [`marrow/Changes.md`](marrow/Changes.md) |

The crates are versioned and released independently. Release tags are scoped by crate, for example `marrow/v1.0.0` and `serde_arrow/v1.0.0`.

## License

The code in this repository is licensed under the MIT license. See [`License.md`](License.md).
