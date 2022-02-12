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
let batch = serde_arrow::to_record_batch(&items, &schema)?;
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

Again a schema is required to match the different data models. For example:


```rust
let items = serde_arrow::from_record_batch(&batch, &schema)?;
```

The schema can either be constructed manually or auto-detected from the record
batch: 

```rust
let schema = Schema::from_record_batch(&batch)?;
// update detected data types here
```

[crate::event::Event]: https://docs.rs/serde_arrow/latest/serde_arrow/event/enum.Event.html
[crate::to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[crate::trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
[serde::Serialize]: https://docs.serde.rs/serde/trait.Serialize.html
[serde::Deserialize]: https://docs.serde.rs/serde/trait.Deserialize.html
[crate::Schema::from_records]: https://docs.rs/serde_arrow/latest/serde_arrow/struct.Schema.html#method.from_records
