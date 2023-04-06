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
        Event::Str("a"),
        Event::I32(1),
        Event::Str("b"),
        Event::U32(2),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("a"),
        Event::I32(3),
        Event::Str("b"),
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

Second, the events can be differently interpreted when creating the arrays. In
the first case, a `UTF8` array would be created in the second case a `Int64`
array. To create `Date64` set the type of the field to `Date64` and add
additional metadata, in the case of strings:

```rust
let mut fields = serialize_into_fields(records).unwrap();

let val_field = fields.iter_mut().find(|field| field.name == "date").unwrap();
val_field.data_type = DataType::Date64;

// only required if the datetime objects are serialized as strings
val_field.metadata = Strategy::NaiveStrAsDate64.into();
```

Currently only datetime objects are supported.

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
