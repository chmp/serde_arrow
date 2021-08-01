# `serde_arrow` - generate parquet / csv with serde

**Warning:** this package is in an experiment at the moment.

## Example

```rust
#[derive(Serialize)]
struct Example {
    a: f32,
    b: i32,
}

let examples = vec![
    Example { a: 1.0, b: 1 },
    Example { a: 2.0, b: 2 },
];

let mut schema = serde_arrow::trace_schema(&examples)?;
let schema = arrow::datatypes::Schema::try_from(schema)?;

let batch = serde_arrow::to_record_batch(&examples, schema)?;
arrow::csv::Writer::new(std::io::stdout()).write(&batch)?;
```

## The data model

The fundamental data model is a sequence of records that is transformed into a
record batch. The conical example is a Vec or slice of records:

```rust
let items = vec![
    Record { a: 1, b: 2},
    // ...
];
```

Each record can either be a struct that implements Serialize or map (e.g.,
HashMap). Structures with flattened children are also supported. For example

```rust
#[derive(Serialize)]
struct FlattenExample {
    a: i32,
    #[serde(flatten)]
    child: OtherStructure,
}
```



For maps, all fields need to be added to the schema.

Datetimes are supported, but their data type cannot be auto detected. Without
additional configuration they are stored as string columns. This can be changed
by overwriting the data type after tracing:

```rust
let mut schema = serde_arrow::trace_schema(&examples)?;
schema.set_data_type("date", DataType::Date64);
```

## Status

The following field types are supported. In general that the mapping of Rust
types to [Serde's data model][serde-data-model] is not one-to-one. For example
Chrono's date time types can be serialized as a Serde integer or a Serde string
depending on configuration.

- [x] booleans (serde: `bool`, arrow: `Boolean`)
- [x] signed integers (serde: `i8`, `i16`, `i32`, `i64`, arrow: `Int8`, `Int16`,
  `Int32`, `Int64`)
- [x] unsigned integers (serde: `u8`, `u16`, `u32`, `u64`, arrow: `UInt8`,
  `UInt16`, `UInt32`, `UInt64`)
- [x] floating point numbers (serde: `f32`, `f64`, arrow: `Float32`, `Float64`)
- [x] strings (serde: `&str`, arrow: `Utf8`, `LargeUtf8`)
- [x] datetimes expressed as a RFC 3339 string (the default serialization of,
  for example, `chrono::NaiveDateTime`) (serde: `&str`, arrow: `Date64`)
- [ ] dates (serde: `&str`, arrow: `Date32`)
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

[serde-data-model]: https://serde.rs/data-model.html