# Implementation notes

`serde_arrow` allows to convert between Rust objects that implement
[Serialize][serde::Serialize] or [Deserialize][serde::Deserialize] and arrow
arrays. `serde_arrow` introduces an intermediate JSON-like representation in the
form of a stream of [Event][crate::base::Event] objects. Consider the following
Rust vector

```rust
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Record {
    a: i32,
    b: u32,
}

let items = vec![
    Record { a: 1, b: 2},
    Record { a: 3, b: 4},
    // ...
];
```

The items vector can be converted into a stream of events as in:

```rust
let mut events: Vec<Event<'static>> = Vec::new();
serialize_into_sink(&mut events, &items)?;

assert_eq!(
    events,
    vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Key("a"),
        Event::I32(1),
        Event::Key("b"),
        Event::U32(2),
        Event::EndStruct,
        Event::StartStruct,
        Event::Key("a"),
        Event::I32(3),
        Event::Key("b"),
        Event::U32(4),
        Event::EndStruct,
        Event::EndSequence
    ],
);
```

`serde_arrow` can also deserialize from events back to Rust objects

```rust
let items_from_events: Vec<Record> = deserialize_from_source(&events)?;
assert_eq!(items_from_events, items);
```

`serde_arrow` includes functionality that builds arrow arrays from Rust objects
or vice versa by interpreting the stream of events. In both cases, `serde_arrow`
requires additional information over the array types in in the form of arrow
fields

```rust
let fields = vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::UInt32, false),
];
```

With the fields the records can be converted into arrays by calling
`serialize_into_arrays`

```rust
let arrays = serialize_into_arrays(&fields, &items)?;
```

The records can be re-created from the arrays by calling
`deserialize_from_arrays`

```rust
let items_from_arrays: Vec<Record> = deserialize_from_arrays(&fields, &arrays)?;
assert_eq!(items_from_arrays, items);
```

To simplify creating the fields, `serde_arrow` allows to determine the schema
from the records themselves

```rust
let fields_from_items = serialize_into_fields(&items)?;
assert_eq!(fields_from_items, fields);
```

## Type conversions

Due to the two conversions in play (Rust <-> Intermediate Format <-> Arrow)
there are different options to convert Rust types to Arrow. For examples, dates
can be stored as string, integer or date columns depending on configuration.

First, there is the conversion from Rust to the intermediate format. Per default
[chrono](https://docs.rs/chrono/latest/chrono/) serializes date time objects to
strings, but by using its serde module it can be configured to serialize date
times to integers.

For example:

```rust
#[derive(Serialize)]
struct RecordAsString {
    date: NaiveDateTime,
}

#[derive(Serialize)]
struct RecordAsInteger {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    date: NaiveDateTime,
}
```

Serializing the first record type will generate a sequence of events similar to

- `Event::StartSequence`
- `Event::StartStruct`
- `Event::Str("date")`
- `Event::Str(...)`
- `Event::EndStruct`
- ...
- `Event::EndSequence`

Whereas the serializing the second type will generate an event sequence similar to

- `Event::StartSequence`
- `Event::StartStruct`
- `Event::Str("date")`
- `Event::I64(...)`
- `Event::EndStruct`
- ...
- `Event::EndSequence`

## Status

Supported arrow data types:

- [x] `Null`
- [x] `Boolean`
- [x] `Int8`, `Int16`, `Int32`, `Int64`
- [x] `UInt8`, `UInt16`, `UInt32`, `UInt64`
- [ ] `Float16`, 
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
- [ ] `Map`
- [ ] `Dictionary`
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
  - mapped to `Date64` with `Strategy::UtcDateTimeStr` and field data type `Date64`
  - mapped to `Date64` with field data type `Date64` and chrono configured to
    serialize to timestamps using
    [`chrono::serde::ts_microseconds`][chrono-ts-microseconds]
- [x] `chrono::NaiveDateTime`: depends on the configured strategy:
  - mapped to UTF8 arrays without configuration
  - mapped to `Date64` with `Strategy::NaiveDateTimeStr` and field data type `Date64`
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
