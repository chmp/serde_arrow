# Change log

## 0.8.0

Make bytecode based serialization  and deserialization the default

- Remove state machine serialization, and use bytecode serialization as the
  default. This change results in a 2.6x speed up for the default configuration
- Implement deserialization via bytecode (remove state machine implementation)
- Add deserialization support for arrow

Update arrow version support

- Add `arrow=40`, `arrow=41`, `arrow=42`, `arrow=43`,`arrow=44`, `arrow=45`,
  `arrow=46` support
- Remove for `arrow=35`, `arrow=36` support

Improve type support

- Implement bytecode serialization / deserialization of f16
- Add support for coercing different numeric types (use
  `TracingOptions::default().coerce_numbers(true)`)
- Add support for `Timestamp(Milliseconds, None)` and
  `Timestamp(Milliseconds, Some("UTC"))`.

Quality of life features

- Ignore unknown fields in serialization (Rust -> Arrow)
- Raise an error if resulting arrays are of unequal length (#78)
- Add an experimental schema struct under `serde_arrow::experimental::Schema`
  that can be easily serialized and deserialized.

No longer export the `base` module: the implementation details as-is where not
really useful. Remove for now and think about a better design.

Bug fixes:

- Fix bug in bytecode serialization for missing fields (#79)
- Fix bytecode serialization for nested options, .e.g, `Option<Option<T>>`.
- Fix bytecode serialization of structs with missing fields, e.g., missing keys
  with maps serialized as structs
- Fix nullable top-level fields in bytecode serialization
- Fix bug in bytecode serialization for out of order fields (#80)

## 0.7.1

- Fix a bug for unions with unknown variants reported [here][issue-57]. Now
  `serde_arrow` correctly handles unions during serialization, for which not all
  variants were encountered during tracing. Serializing unknown variants will
  result in an error. All variants that are seen during tracing are save to use.

[issue-57]: https://github.com/chmp/serde_arrow/issues/57

## 0.7

- **Breaking change**: add new `Item` event emitted before list items, tuple
  items, or map entries
- Add support for `arrow=38` and `arrow=39` with the  `arrow-38` and `arrow-39`
  features
- Add support for an experimental bytecode serializer that shows speeds of up to
  4x. Enable it with

    ```rust
    serde_arrow::experimental::configure(|config| {
        config.serialize_with_bytecode = true;
    });
    ```

  This setting is global and used for all calls to `serialize_to_array` and
  `serialize_to_arrays`. At the moment the following features are not supported
  by the bytecode serializer:

  - nested options (`Option<Option<T>>`)
  - creating `float16` arrays

## 0.6.1

- Add support for `arrow=37` with the `arrow-37` feature

## 0.6.0

### Add support for arrow2

Now both [arrow][] and [arrow2][] are supported. Use the features to select the
relevant version of either crate. E.g., to use `serde_arrow` with `arrow=0.36`:

```
serde_arrow = { version = "0.6", features = ["arrow-36"] }
```

### Deserialization support (arrow2 only)

`serde_arrow` now supports to deserialize Rust objects from arrays. At the
moment this operation is only support for `arrow2`. Adding support `arrow` is
[planned](https://github.com/chmp/serde_arrow/issues/38).

### More flexible support for Rust / Arrow features

`serde_arrow` now supports many more Rust and Arrow features.

- Rust: Struct, Lists, Maps, Enums, Tuples
- Arrow: Struct, List, Maps, Unions, ...

### Removal of custom schema APIs

`serde_arrow` no longer relies on its own schema object. Now all schema
information is retrieved from arrow fields with additional metadata.

### More flexible APIs

In addition to the previous API that worked on a sequence of records,
`serde_arrow` now also supports to operate on a sequence of individual items
(`serialize_into_array`, `deserialize_form_array`) and to operate on single
items (`ArraysBuilder`).

## Support for dictionary encoded strings (categories)

`serde_arrow` supports dictionary encoding for string arrays. This way string
arrays are encoded via a lookup table to avoid including repeated string values.

## 0.5.0

- Bump arrow to version 16.0.0

[arrow]: https://github.com/apache/arrow-rs
[arrow2]: https://github.com/jorgecarleitao/arrow2
[polars]: https://github.com/pola-rs/polars
[arrow2-to-arrow]: ./arrow2-to-arrow
