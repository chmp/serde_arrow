# Change log

## 0.13.6

- [@jpopesculian](https://github.com/jpopesculian) added the option `bytes_as_large_binary`
  to `TracingOptions` ([#281](https://github.com/chmp/serde_arrow/pull/282))
- [@jpopesculian](https://github.com/jpopesculian) added support to serialize strings as
  bytes ([#281](https://github.com/chmp/serde_arrow/pull/281))

## 0.13.5

- Add `arrow=56` support

## 0.13.4

- Allow dictionaries with nullable values to be deserialized, if all values are valid

### Thanks

The following people contributed to this release:

- [@ryzhyk](https://github.com/ryzhyk) improved the null checks in dictionary deserialization
  ([#277](https://github.com/chmp/serde_arrow/pull/277))

## 0.13.3

- Fix: Use the correct lower bound for marrow to enable `arrow=55`

## 0.13.2

- Add `arrow=55` support
- Allow primitive to string conversion ([#272](https://github.com/chmp/serde_arrow/pull/272))
  - Adds the option `allow_to_string` to `TracerOptions` to allow numbers, booleans and chars
    to coerce to strings (`false` by default)
  - Allows for numbers, booleans and chars to be serialized to strings

### Thanks

The following people contributed to this release:

- [@jpopesculian](https://github.com/jpopesculian) added support for serializing primitives to
  string ([#272](https://github.com/chmp/serde_arrow/pull/272))

## 0.13.1

- Allow to use enums / unions in nullable structs. The arrow format requires that each child field
  of a null struct value contains a dummy value. For union fields such a dummy value was not
  supported before. With this release the following dummy values for unions are used:
  - For unions, the first variant is used as a dummy
  - For dictionary encoded strings, the first non-null value is used as a dummy value or an empty
    string if no values are encountered

### Thanks

The following people contributed to this release:

- [@bartek358](https://github.com/bartek358) discovered that unions in nullable structs are not
  supported and fixed the implementation in ([#265](https://github.com/chmp/serde_arrow/pull/265))

## 0.13.0

- Migrate internal array abstraction to  [`marrow`][https://github.com/chmp/marrow]
  - Breaking change: Dictionary data types no longer support sorting
  - Breaking change: `serde_arrow::Error` no longer implements `From<arrow::error::ArrowError>`
  - Breaking change: `serde_arrow::Error` no longer implements `From<arrow2::error::Error>`
- Add support for view types `Utf8View`, `BytesView`
- Add APIs to interact with `marorw` arrays directly. Allows to use `serde_arrow` with different
  arrow versions at the same time.
- Fix `Date64` semantics: use `Date64` exclusively for dates, and `Timestamp` for date times
  - Trace date time strings as `Timestamp(Millisecond, tz)` with `tz` either being `None` or
    `Some("UTC")`
  - Remove the `UtcAsDate64Str` and `NaiveAsDate64Str` strategies
- Fix bug in deserialization of sub seconds for `Time32` and `Time64`
- Fix bug that prevented to deserialize `String` from `Decimal` arrays
- Improve performance when deserializing from sliced arrays
- Allow to treat deserializers as sequence of deserializers by iterating over them or accessing
  individual items

### Thanks

The following people contributed to this release:

- [@ryzhyk](https://github.com/ryzhyk) discovered and fixed a bug introduced during refactoring
  ([#261](https://github.com/chmp/serde_arrow/pull/261))


## 0.12.3

- Add `arrow=54` support

## 0.12.2

Bug fixes:

- Fixed deserialization from sliced arrays ([#248](https://github.com/chmp/serde_arrow/issues/248)).
  Note that the current solution requires up front work when constructing the array deserializers,
  as described in the issue. The removal of the performance penalty is tracked in
  ([#250](https://github.com/chmp/serde_arrow/issues/250))

### Thanks

- [@jkylling](https://github.com/jkylling) for reporting
  ([#248](https://github.com/chmp/serde_arrow/issues/248)) and for discussing potential solutions

## 0.12.1

New features

- Add support for various `jiff` types (`jiff::Date`, `jiff::Time`, `jiff::DateTime`,
  `jiff::Timestamp`, `jiff::Span`, `jiff::SignedDuration`)
- Add support for tracing lists as `List` instead of `LargeList` by setting `sequence_as_large_list`
  to `false` in `TracingOptions`
- Add support for tracing strings and strings in dictionaries as `Utf8` instead of `LargeUtf8` by
  setting `strings_as_large_utf8` to `false` in `TracingOptions`
- Add support to auto-detect dates (`2024-09-30`, mapped to `Date32`) and times (`12:00:00`, mapped
  to `Time64(Nanosecond))`) in `from_samples`
- Improved error messages for non self describing types (`chrono::*`, `uuid::Uuid`,
  `std::net::IpAddr`)

### Thanks

The following people contributed to this release:

- [@jkylling](https://github.com/jkylling) added support for tracing lists as `List` and strings as
  `Utf8`

## 0.12.0

Refactor the underlying implementation to prepare for further development

New features

- Add `Binary`, `LargeBinary`, `FixedSizeBinary(n)`, `FixedSizeList(n)` support for `arrow2`
- Add support to serialize / deserialize `bool` from integer arrays
- Add a helper to construct `Bool8` arrays
- Include the path of the field that caused an error in the error message
- Include backtrace information only for the debug representations of errors

API changes

- Use `impl serde::Serialize` instead of `&(impl serde::Serialize + ?Sized)`
- Use `&[FieldRef]` instead of `&[Field]` in arrow APIs

Removed deprecated API

- Remove `serde_arrow::schema::Schema`
- Remove `serde_arrow::ArrowBuilder` and `serde_arrow::Arrow2Builder`
- Remove `from_arrow_fields` / `to_arrow_fields` for `SerdeArrowSchema`, use the
  `TryFrom` conversions to convert between fields and `SerdeArrowSchema`
- Remove `SerdeArrowSchema::new()`, `Overwrites::new()`

## 0.11.8

- Add `arrow=53` support

### Thanks

The following people contributed to this release:

- [shehabgamin](https://github.com/shehabgamin) prepared this release
  ([pr](https://github.com/chmp/serde_arrow/pull/235))

## 0.11.7

- Fix tracing of JSON mixing nulls with non-null data

## 0.11.6

- Add `arrow=52` support
- Add support for `Binary`, `LargeBinary` (only `arrow`)
- Add support for `FixedSizeBinary(n)` (only `arrow>=47`)
- Add support for `FixedSizeList(n)` (only `arrow`)
- Add support to overwrite field definitions with `TracingOptions::overwrite`
- Add support to serialize enums without data (e.g., `enum E { A, B, C}`) as
  strings by setting the corresponding field to a string value (`Utf`,
  `LargeUtf`, `Dictionary(_, Utf8)`, `Dictionary(_, LargeUtf8`)
- Allow to trace enums without data as dictionary encoded strings by setting
  `enums_without_data_as_strings` to `true` in `TracingOptions`

## 0.11.5

- Add `serde_arrow::Serializer`
- Add support for new type wrappers, tuples and tuple structs to
  `serde_arrow::Deserializer`
- Add a generic `serde_arrow::ArrayBuilder` with support for both `arrow` and
  `arrow2`
- Implement `TryFrom<&[Field]>` (`arrow` and `arrow2`) and
  `TryFrom<&[FieldRef]>` (`arrow` only) for `SerdeArrowSchema`
- Implement `TryFrom<&SerdeArrowSchema>` for `Vec<Field>` and `Vec<FieldRef>`
  for `arrow`

## 0.11.4

- Add `serde_arrow::Deserializer`

## 0.11.3

- Support for serializing/deserializing timestamps with second, microsecond, and
  nanosecond encoding.
- Fixed (de)serialization of fractional seconds.

### Thanks

The following people contributed to this release:

- [@ryzhyk](https://github.com/ryzhyk) added string support for timestamps with
  non-millisecond units, fixed the handling of fractional seconds
  ([PR](https://github.com/chmp/serde_arrow/pull/168))

## 0.11.2

- Support `Duration(unit)`
- Rewrite data type parsing with stricter parsing

## 0.11.1

- Support `Timestamp(Second, tz)`, `Timestamp(Millisecond, tz)`,
  `Timestamp(Nanosecond, tz)`. At the moment only (de)serialization from / to
  integers is supported for non-microsecond units
- Support `Time32(unit)`

## 0.11.0

`0.11.0` does not contain any known breaking changes. However it's a major
refactoring and untested behavior may change.

The biggest feature is the removal of the bytecode deserializer and use of the
Serde API directly. With this change, the code is easier to understand and
extend. Further `Deserialization` implementations can request specific types and
`serde_arrow` is able to supply them. As a consequence deserialization of
`chrono::DateTime<Utc>` is supported by `serde_arrow` without an explicit
strategy.

Further changes:

- Add `arrow=51` support
- Add `Date32` and `Time64` support
- Add `to_record_batch`, `from_record_batch` to offer more streamlined APIs for
  working with record batches
- Allow to perform zero-copy deserialization from arrow arrays
- Allow to use `arrow` schemas in `SchemaLike::from_value()`, e.g., `let fields
  = Vec::<Field>::from_value(&batch.schema())`.
- Implement `SchemaLike` for `arrow::datatypes::FieldRef`s
- Fix bug in `SchemaLike::from_type()` for nested unions

### Thanks

The following people contributed to this release:

- [@gz](https://github.com/gz) added `Date32` and `Time64` support
  ([PR](https://github.com/chmp/serde_arrow/pull/147))
- [@progval](https://github.com/progval) added additional error messages
  ([PR](https://github.com/chmp/serde_arrow/pull/142))
- [@gstvg](https://github.com/gstvg) contributed zero-copy deserialization
  ([PR](https://github.com/chmp/serde_arrow/pull/151))

## 0.10.0

- Remove deprecated APIs
- Use the serde serialization APIs directly, instead of using the bytecode
  serializer. Serialization will be about `2x` faster
- Fix bug in `SchemaLike::from_value` with incorrect strategy deserialization

### Thanks

The following people contributed to this release:

- [@Ten0](https://github.com/Ten0) motivated the rewrite to use the serde API
  directly and contributed additional benchmarks for JSON transcoding
  ([PR](https://github.com/chmp/serde_arrow/pull/130))
- [@alamb](https://github.com/alamb) added improved documentation on how to use
  `serde_arrow` with the `arrow` crate
  ([PR](https://github.com/chmp/serde_arrow/pull/131))

## 0.9.1

- `Decimal128` support: serialize / deserialize
  [`rust_decimal`](https://crates.io/crates/rust_decimal) and
  [`bigdecimal`](https://crates.io/crates/bigdecimal) objects
- Add `arrow=50` support
- Improved error messages when deserializing `SchemaLike`
- Relax `Sized` requirement for `SchemaLike::from_samples(..)`,
  `SchemaLike::from_type(..)`, `SchemaLike::from_value(..)`
- Derive `Debug`, `PartialEq` for `Item` and `Items`

## 0.9.0

Breaking changes:

- Make tracing options non-exhaustive
- Remove the `try_parse_dates` field in favor of the `guess_dates` field in
  `TracingOptions` (the setter name is not affected)
- Remove the experimental configuration api

Improvements:

- Simpler and streamlined API (`to_arrow` / `from_arrow` and `to_arrow2` /
  `from_arrow2`)
- Add `SchemaLike` trait to support direct construction of arrow / arrow2 fields
- Add type based tracing to allow schema tracing without samples
  (`SchemaLike::form_type()`)
- Allow to build schema objects from serializable objects, e.g.,
  `serde_json::Value` (`SchemaLike::from_value()`)
- Add support for `arrow=47`, `arrow=48`, `arrow=49`
- Improve error messages in schema tracing
- Fix bug in `arrow2=0.16` support
- Fix unused warnings without selected arrow versions

Deprecations (see the documentation of deprecated items for how to migrate):

- Rename `serde_arrow::schema::Schema` to
  `serde_arrow::schema::SerdeArrowSchema` to prevent name clashes with the
  schema types of `arrow` and `arrow2`.
- Deprecate `serialize_into_arrays`, `deserialize_from_arrays` methods in favor of
  `to_arrow` / `to_arrow2` and `from_arrow` / `from_arrow2`
- Deprecate `serialize_into_fields` methods in favor of
  `SchemaLike::from_samples`
- Deprecated single item methods in favor of using the `Items` and `Item`
  wrappers

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

### Thanks

The following people contributed to this release:

- [@elbaro](https://github.com/elbaro) updated the readme example
  ([PR](https://github.com/chmp/serde_arrow/pull/33))

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
