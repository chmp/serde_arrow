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
record batch. Each record can either be a struct or map (other types of records,
e.g., tuples are planned).

Structures with flatten'ed children are supported. For example

```rust
#[derive(Serialize)]
struct FlattenExample {
    a: i32,
    #[serde(flatten)]
    child: OtherStructure,
}
```
