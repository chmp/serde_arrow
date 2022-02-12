# `serde_arrow` - convert sequences of structs / maps to and from arrow tables

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Example](#example)
| [How does it work?](Implementation.md)
| [Status](#status)
| [License](#license)

**Warning:** this package is in an experiment at the moment.

[Arrow][arrow)] is a powerful library to work with data frame like structures.
The surrounding ecosystem includes a rich set of libraries, ranging from data
frames via [Polars][polar] to query engines via [DataFusion][datafusion].
However, it's API due to the statically typed nature of Rust can be at times
cumbersome to use directly. This package, `serde_arrow`, tries to bridge this
gap by offering a simple way to convert Rust objects into Arrow objects and vice
versa.  This package is optimized for ease of use, not performance.

`serde_arrow` relies on the [Serde](https://serde.rs) package to interpret Rust
objects. Therefore, adding support for `serde_arrow` to custom types is as easy
as using Serde's derive macros. 

See the [implementation notes](Implementation.md) for details on how it is
implemented.

[arrow]: https://docs.rs/arrow/latest/arrow/
[polars]: https://github.com/pola-rs/polars
[datafusion]: https://github.com/apache/arrow-datafusion/

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

// Write the records into an IPC file
let out  = File::create("examples.arrow")?;
serde_arrow::to_ipc_writer(out, &examples, &schema)?;

// NOTE: the records can also be converted into a RecordBatch. The RecordBatch
// can then be used to convert it into a polars DataFrame or written to parquet.
//
// let batch = serde_arrow::to_record_batch(&examples, schema)?;
```

The written file can now be read in Python via

```python
import pandas as pd
pd.read_feather("examples.arrow")
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
- [x] `chars` (serde: `char`, arrow: `UInt32`)
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
  data type has to be overwriting after tracing. For example, the chrono date
  time types are supported via:
    ```rust
    let mut schema = serde_arrow::trace_schema(&examples)?;
    schema.add_field("date", Some(DataType::DateTimeStr), None);
    ```

# License

```text
Copyright (c) 2021 - 2022 Christopher Prohm

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
