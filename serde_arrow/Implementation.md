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
        Event::StartMap,
        Event::OwnedKey(String::from("a")),
        Event::I32(1),
        Event::OwnedKey(String::from("b")),
        Event::U32(2),
        Event::EndMap,
        Event::StartMap,
        Event::OwnedKey(String::from("a")),
        Event::I32(3),
        Event::OwnedKey(String::from("b")),
        Event::U32(4),
        Event::EndMap,
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
- `Event::StartMap`
- `Event::Key("date")`
- `Event::String(...)`
- `Event::EndMap`
- ...
- `Event::EndSequence`

Whereas the serializing the second type will generate an event sequence similar to

- `Event::StartSequence`
- `Event::StartMap`
- `Event::Key("date")`
- `Event::I64(...)`
- `Event::EndMap`
- ...
- `Event::EndSequence`

Without extra configurations, serializing these records will generate record
batches with a string column or 64 bit integer column respectively. Both can be
converted into a `Date64` column by setting the data type to `NaiveDateTimeStr`
or `DateTimeMilliseconds` respectively:

```rust
// for RecordAsString
let schema = Schema::from_records(&items)?
    .with_field("date", Some(DataType::NaiveDateTimeStr), None);
let items = serde_arrow::arrow::from_record_batch(&items, &schema)?;

// for RecordAsInteger
let schema = Schema::from_records(&items)?
    .with_field("date", Some(DataType::DateTimeMilliseconds), None);
let items = serde_arrow::arrow::from_record_batch(&items, &schema)?;
```

## Abstractions

- `trace_schema`: `value` -> `SchemaSink`
-

## Status

| Rust             | Schema    | Arrow      | Comment |
|------------------|-----------|------------|---------|
| `bool`           | `Bool`    | `Boolean`  | |
| `i8`             | `I8`      | `Int8`     | |
| `i16`            | `I16`     | `Int16`    | |
| `i32`            | `I32`     | `Int32`    | |
| `i64`            | `I64`     | `Int64`    | |
| `u8`             | `U8`      | `UInt8`    | |
| `u16`            | `U16`     | `UInt16`   | |
| `u32`            | `U32`     | `UInt32`   | |
| `u64`            | `U64`     | `UInt64`   | |
| `f32`            | `F32`     | `Float32`  | |
| `f64`            | `F64`     | `Float64`  | |
| `char`           | `U32`     | `UInt32`   | |
| `&str`, `String` | `Str`     | `Utf8`     | **default** |
| `&str`, `String` | `Arrow(LargeUtf8)` | `LargeUtf8` | |
| `chrono::NaiveDateTime` | `Str` | `Utf8` | **default** |
| `chrono::NaiveDateTime` | `NaiveDateTimeStr` | `Date64` | |
| `chrono::NaiveDateTime` + `chrono::serde::ts_milliseconds` | `U64` | `UInt64` | **default** |
| `chrono::NaiveDateTime` + `chrono::serde::ts_milliseconds` | `DateTimeMilliseconds` | `Date64` | |
| `chrono::DateTime<Utc>` | `Str` | `Utf8` | **default** |
| `chrono::DateTime<Utc>` | `DateTimeStr` | `Date64` | |
| `chrono::DateTime<Utc>` + `chrono::serde::ts_milliseconds` | `U64` | `UInt64` | **default** |
| `chrono::DateTime<Utc>` + `chrono::serde::ts_milliseconds` | `DateTimeMilliseconds` | `Date64` | |

**default** is the configuration that is auto detected, when the schema is
traced.

**Warning:** the RFC 3339 format used, when serializing date times as strings,
will strip the milliseconds.

Missing:

- [ ] storing dates as `Date32`
- [ ] binary data (serde: `Seq[u8]`, arrow: `Binary`, `FixedSizeBinary`,
  `LargeBinary`)
- [ ] remaining arrow time data types (`Timestamp`, `Time32`, `Time64`,
  `Duration`, `Interval`)
- [ ] nested structs (arrow: `Struct`)
- [ ] nested sequences (serde: `Seq<T>`, arrow: `List<T>`, `FixedSizeList<T>`,
  `LargeList<T>`)
- [ ] nested maps (serde: `Map<K, V>`, arrow: `Dictionary<K, V>`)
- [ ] decimals (arrow: `Decimal`)
- [ ] unions (arrow: `Union`)

Comments:

- Structures with flattened children are supported. For example
    ```rust
    #[derive(Serialize)]
    struct FlattenExample {
        a: i32,
        #[serde(flatten)]
        child: OtherStructure,
    }
    ```
- For maps, all fields need to be added to the schema and need to be found in
  each record. The latter restriction will be lifted

[crate::base::Event]: https://docs.rs/serde_arrow/latest/serde_arrow/event/enum.Event.html
[crate::to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[crate::trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
[serde::Serialize]: https://docs.serde.rs/serde/trait.Serialize.html
[serde::Deserialize]: https://docs.serde.rs/serde/trait.Deserialize.html
[crate::Schema::from_records]: https://docs.rs/serde_arrow/latest/serde_arrow/struct.Schema.html#method.from_records
[chrono]: https://docs.rs/chrono/latest/chrono/

[crate::base::EventSource]: https://docs.rs/serde_arrow
[crate::base::EventSink]: https://docs.rs/serde_arrow