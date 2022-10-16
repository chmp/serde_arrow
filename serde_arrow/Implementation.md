# Implementation notes

The fundamental data model is a sequence of records, think a list of maps in
JSON. In Rust this maps to slices of structs or HashMaps. The only requirement
on the record types is that they implement [Serialize][serde::Serialize] or
[Deserialize][serde::Deserialize] depending on the desired operation. For
example:

```rust
#[derive(Serialize)]
struct Record {
    a: i32,
    b: u32,
}

let items = vec![
    Record { a: 1, b: 2},
    // ...
];
```

To interface with Serde `serde_arrow` interprets the sequence of records as a
JSON-like internal representation in the form of a stream of
[Event][crate::event::Event] objects. For example, the `items` object above
would result in the following stream of events:

- `Event::StartSequence`
- `Event::StartMap`
- `Event::Key("a")`
- `Event::I32(1)`
- `Event::Key("b")`
- `Event::U32(2)`
- `Event::EndMap`
- `Event::EndSequence`

Next `serde_arrow` converts this stream of events into a record batch. In sum,
process looks like:

- Use Serde to convert a sequence of Rust records into an Event stream
- Convert the Event stream into an Arrow RecordBatch

As the [Serde data model](https://serde.rs/data-model.html) and the Arrow data
model do not match directly, `serde_arrow` requires an additional schema to map
of fields to their types. Given a schema a list of records can be converted into
a record batch using [`serde_arrow::to_record_batch`][crate::to_record_batch]:

```rust
let batch = serde_arrow::arrow::to_record_batch(&items, &schema)?;
```

To support in creation of schema definitions `serde_arrow` offers the function
[`Schema::from_records`][crate::Schema::from_records], which tries to auto-detect
the schema. For example:

```rust
let schema = Schema::from_records(&items)?;
// update detected data types here
```

Note however, this detection is not always reliable. For example `Option`s with
only `None` values cannot be detected. Also chrono's date types map to different
serialization formats (strings, ints, ..) depending on configuration.

The reverse process, from a record batch to a sequence of Rust records, is
implemented similarly:

- Convert an Arrow RecordBatch into an Event stream
- Use Serde to convert the Event stream into a sequence of Rust objects

For example:

```rust
let items = serde_arrow::arrow::from_record_batch(&batch, &schema)?;
```

Again a schema is required to match the different data models. The schema can
either be constructed manually or auto-detected from the record batch:

```rust
let schema = Schema::from_record_batch(&batch)?;
// update detected data types here
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

[crate::event::Event]: https://docs.rs/serde_arrow/latest/serde_arrow/event/enum.Event.html
[crate::to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[crate::trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
[serde::Serialize]: https://docs.serde.rs/serde/trait.Serialize.html
[serde::Deserialize]: https://docs.serde.rs/serde/trait.Deserialize.html
[crate::Schema::from_records]: https://docs.rs/serde_arrow/latest/serde_arrow/struct.Schema.html#method.from_records
[chrono]: https://docs.rs/chrono/latest/chrono/