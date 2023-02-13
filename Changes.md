# Change log

## 0.6.0

### Remove support arrow in favor of arrow2

Drop support [arrow][] in favor of [arrow2][]. Arrow2 is a smaller, faster to
build implementation of the Arrow format that follow semver. It is also used by
[polars][]. That said most of the implementation is pretty generic and [arrow][]
support could be added.

### More flexible support for Rust / Arrow features

`serde_arrow` now supports many more Rust and Arrow features.

- Rust: Struct, Lists, Maps, Enums, Tuples
- Arrow: Struct, List, Maps, Unions, ...

### Removal of custom API

`serde_arrow` no longer relies on its own schema object. Now all schema
information is retrieved from arrow fields with additional metadata.

### More flexible APIs

In addition to the previous API that worked on a sequence of records,
`serde_arrow` now also supports to operate on a sequence of individual items
(`serialize_into_array`, `deserialize_form_array`) and to operate on single
items (`ArraysBuilder`).

## 0.5.0

- Bump arrow to version 16.0.0

[arrow]: https://github.com/apache/arrow-rs
[arrow2]: https://github.com/jorgecarleitao/arrow2
[polars]: https://github.com/pola-rs/polars