# Status

Supported arrow data types:

- [x] `Null`
- [x] `Boolean`
- [x] `Int8`, `Int16`, `Int32`, `Int64`
- [x] `UInt8`, `UInt16`, `UInt32`, `UInt64`
- [x] `Float16`:  can be serialized / deserialized from Rust `f32`
- [x] `Float32`, `Float64`
- [ ] `Timestamp`
- [ ] `Date32`
- [x] `Date64`: either as formatted dates (UTC + Naive) (`Event::Str`) or as
  timestamps (`Event::I64`). Both cases require additional configuration
- [ ] `Time32`
- [ ] `Time64`
- [ ] `Duration`
- [ ] `Interval`
- [ ] `Binary`
- [ ] `FixedSizeBinary`
- [ ] `LargeBinary`
- [x] `Utf8`
- [x] `LargeUtf8`
- [x] `List`
- [ ] `FixedSizeList`
- [x] `LargeList`
- [x] `Struct`
- [x] `Union`: at the moment only dense unions are supported
- [x] `Map`: at the moment only unsorted maps are supported
- [x] `Dictionary`: at the moment only Utf8 and LargeUtf8 as values are
  supported
- [ ] `Decimal`
- [ ] `Decimal256`
- [ ] `Extension`

Supported Serde / Rust types:

- [x] `bool`
- [x] `i8`, `i16`, `i32`, `i64`
- [x] `u8`, `u16`, `u32`, `u64`
- [x] `f32`, `f64`
- [x] `char`: serialized as u32
- [x] `Option<T>`: if `T` is supported
- [x] `()`: serialized as a missing value, `Option<()>` is always deserialized
  as `None`
- [x] `struct S{ .. }`: if the fields are supported
- [x] `Vec<T>`: if T is supported. Any type that serializes into a Serde
  sequence is supported
- [x] `HashMap<K, V>, BTreeMap<K, V>` and similar map types are supported if `K`
  and `V` are supported
- [x] tuples: tuples or tuple structs are not yet supported. It is planned to
  map them to struct arrays with numeric field names
- [x] `enum ... { }`: enums are mapped to union arrays. At the moment options of
  unions are not supported. Also unions with more than 127 variants are not
  supported. All types of union variants (unit, newtype, tuple, struct) are
  supported
- [x] `struct S(T)`: newtype structs are supported, if `T` is supported
- [x] `chrono::DateTime<Utc>`: depends on the configured strategy:
  - mapped to UTF8 arrays without configuration
  - mapped to `Date64` with `Strategy::UtcStrAsDate64` and field data type `Date64`
  - mapped to `Date64` with field data type `Date64` and chrono configured to
    serialize to timestamps using
    [`chrono::serde::ts_microseconds`][chrono-ts-microseconds]
- [x] `chrono::NaiveDateTime`: depends on the configured strategy:
  - mapped to UTF8 arrays without configuration
  - mapped to `Date64` with `Strategy::NaiveStrAsDate64` and field data type `Date64`
  - mapped to `Date64` with field data type `Date64` and chrono configured to
    serialize to timestamps using
    [`chrono::serde::ts_microseconds`][chrono-ts-microseconds]

[crate::base::Event]: https://docs.rs/serde_arrow/latest/serde_arrow/event/enum.Event.html
[crate::to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[crate::trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
[serde::Serialize]: https://docs.serde.rs/serde/trait.Serialize.html
[serde::Deserialize]: https://docs.serde.rs/serde/trait.Deserialize.html
[crate::Schema::from_records]: https://docs.rs/serde_arrow/latest/serde_arrow/struct.Schema.html#method.from_records
[chrono]: https://docs.rs/chrono/latest/chrono/

[crate::base::EventSource]: https://docs.rs/serde_arrow
[crate::base::EventSink]: https://docs.rs/serde_arrow
[chrono-ts-microseconds]: https://docs.rs/chrono/latest/chrono/serde/ts_microseconds/
