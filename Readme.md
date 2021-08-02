# `serde_arrow` - convert sequences of structs / maps to arrow tables

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Example](#example)
| [How does it work?](#how-does-it-work)
| [Status](#status)
| [License](#license)

**Warning:** this package is in an experiment at the moment.

This package is focused on serialization for the moment, as this is the author's
use case.

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

// Detect the schema from the supplied data
let schema = serde_arrow::trace_schema(&examples)?;
let schema = arrow::datatypes::Schema::try_from(schema)?;

// Write the records into an IPC file
let out  = File::create("examples.ipc")?;
serde_arrow::to_ipc_writer(out, &examples, schema)?;

// NOTE: the records can also be converted into a RecordBatch and then for
// example written to a parquet file:
//
// let batch = serde_arrow::to_record_batch(&examples, schema)?;
```

The written file can now be read in, for example, Python via

```python
import pandas as pd
pd.read_feather("examples.ipc")
```

## How does it work?

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
used to map the [Serde data model][serde-data-model] to Arrow types. Given a
schema a list of records can be converted into a record batch using
[`serde_arrow::to_record_batch`][docs:to_record_batch]:

```rust
let batch = serde_arrow::to_record_batch(&items, schema)?;
```

To support in creation of schema definitions `serde_arrow` offers the function
[`serde_arrow::trace_schema][docs:trace_schema], which tries to auto-detect the
schema. However, this detection is not always reliable. For example `Option`s
with only `None` values cannot be detected. Also chrono's date types map to
different serialization formats (strings, ints, ..) depending on configuration.
Therefore, the traced schema can be further customized before converting it into
an arrow schema:

```rust
let schema = serde_arrow::trace_schema(&items)?;
// update detected data types here
let schema = arrow::datatypes::Schema::try_from(schema)?;
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
- [x] datetimes expressed as a RFC 3339 string or as i64 milliseconds (serde:
  `&str`, `i64`, arrow: `Date64`). This convention maps to chrono types as
  `NaiveDateTime` as string or `DateTime<Utc>` as integer via
  `chrono::serde::ts_milliseconds`. **Warning:** the RFC 3339 format will strip
  the milliseconds
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
- Datetimes are supported, but their data type cannot be auto detected. Their
  data type has to be overwriting after tracing:
    ```rust
    let mut schema = serde_arrow::trace_schema(&examples)?;
    schema.set_data_type("date", DataType::Date64);
    ```

# License

```text
Copyright (c) 2021 Christopher Prohm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

[serde-data-model]: https://serde.rs/data-model.html
[docs:to_record_batch]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.to_record_batch.html
[docs:trace_schema]: https://docs.rs/serde_arrow/latest/serde_arrow/fn.trace_schema.html
