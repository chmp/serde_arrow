# Implementation notes

The fundamental data model is a sequence of records that is transformed into a
record batch. The conical example is a Vec of records. Each record itself is a
struct that implements Serialize, but i can also be a map (e.g., HashMap).

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

To convert a sequence of records into a record batch `serde_arrow` requires a
valid schema as input, i.e., a mapping of fields to their type. The schema is
used to map the [Serde data model](https://serde.rs/data-model.html) to Arrow
types. Given a schema a list of records can be converted into a record batch
using [`serde_arrow::to_record_batch`][crate::to_record_batch]:

```rust
let batch = serde_arrow::to_record_batch(&items, &schema)?;
```

To support in creation of schema definitions `serde_arrow` offers the function
[`serde_arrow::trace_schema`][crate::trace_schema], which tries to auto-detect
the schema. However, this detection is not always reliable. For example
`Option`s with only `None` values cannot be detected. Also chrono's date types
map to different serialization formats (strings, ints, ..) depending on
configuration. For example:

```rust
let schema = serde_arrow::trace_schema(&items)?;
// update detected data types here
```

[crate::to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[crate::trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
