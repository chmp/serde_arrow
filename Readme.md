# `serde_arrow` - convert sequences of Rust objects to Arrow arrays and back again

[Crate info](https://crates.io/crates/serde_arrow)
| [API docs](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Example](#example)
| [Related packages & performance](#related-packages--performance)
| [Status](serde_arrow/Status.md)
| [License](#license)
| [Changes](Changes.md)
| [Contributing](Contributing.md)

The Arrow in-memory format is a powerful way to work with data-frame-like
structures. The surrounding ecosystem includes a rich set of libraries, ranging
from data frames such as [Polars][polars] to query engines such as
[DataFusion][datafusion]. However, the API of the underlying Rust crates can be
at times cumbersome to use due to the statically typed nature of Rust.

`serde_arrow` offers a simple way to convert Rust objects into Arrow arrays and
back. `serde_arrow` relies on the [Serde](https://serde.rs) package to
interpret Rust objects. Therefore, adding support for `serde_arrow` to custom
types is as easy as using Serde's derive macros.

`serde_arrow` supports [`arrow`][arrow] for schema tracing, serialization from
Rust structs to arrays, and deserialization from arrays to Rust structs.

[arrow]: https://docs.rs/arrow/latest/arrow/
[polars]: https://github.com/pola-rs/polars
[datafusion]: https://github.com/apache/arrow-datafusion/

## Example

The following examples assume that `serde_arrow` is added to the `Cargo.toml`
file and its features are configured. `serde_arrow` supports different `arrow`
versions. The relevant one can be selected by specifying the correct feature
(e.g., `arrow-53` to support `arrow=53`). See
[here][feature-docs] for more details.

[feature-docs]: https://docs.rs/serde_arrow/latest/serde_arrow/#features

The following examples use this Rust structure and example records:

```rust
#[derive(Serialize, Deserialize)]
struct Record {
    a: f32,
    b: i32,
}

let records = vec![
    Record { a: 1.0, b: 1 },
    Record { a: 2.0, b: 2 },
    Record { a: 3.0, b: 3 },
];
```

### Serialize to `arrow` `RecordBatch`

```rust
use arrow::datatypes::FieldRef;
use serde_arrow::schema::{SchemaLike, TracingOptions};

// Determine Arrow schema
let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;

// Build a record batch
let batch = serde_arrow::to_record_batch(&fields, &records)?;
```

This `RecordBatch` can now be written to disk using [ArrowWriter] from the
[parquet] crate.

[ArrowWriter]: https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html
[parquet]: https://docs.rs/parquet/latest/parquet/


```rust
use parquet::arrow::ArrowWriter;

let file = File::create("example.pq")?;
let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
writer.write(&batch)?;
writer.close()?;
```

### Usage from Python

The written files can be read in Python via

```python
# using polars
>>> import polars as pl
>>> pl.read_parquet("example.pq")
shape: (3, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ f32 ┆ i32 │
╞═════╪═════╡
│ 1.0 ┆ 1   │
│ 2.0 ┆ 2   │
│ 3.0 ┆ 3   │
└─────┴─────┘

# using pandas
>>> import pandas as pd
>>> pd.read_parquet("example.pq")
     a  b
0  1.0  1
1  2.0  2
2  3.0  3
```

## Related packages & Performance

- [`arrow`][arrow]: the JSON component of the official Arrow package supports
  serializing objects via the [Decoder][serde-decoder]. It supports primitive
  types, structs, and lists
- [`arrow-convert`][arrow-convert]: a derive-based converter for `arrow-rs`
- [`typed-arrow`][typed-arrow]: derive-based converter of Rust structs to Arrow

[serde-decoder]: https://docs.rs/arrow-json/latest/arrow_json/reader/struct.Decoder.html
[arrow-convert]: https://github.com/Swoorup/arrow-convert
[typed-arrow]: https://github.com/tonbo-io/typed-arrow

The different implementations have the following performance differences compared to direct
marrow-to-Arrow array construction:

![Time ](timings.png)

The detailed runtimes of the [benchmarks](./bench/benches/groups/) are listed below.

<!-- start:benchmarks -->
### `complex_1000`

| label                     | time [ms] | marrow to arrow | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|---------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| marrow to arrow           |      0.31 |            1.00 |            0.19 |            0.19 |            0.14 |
| serde_arrow::to_marrow    |      1.62 |            5.24 |            1.00 |            1.00 |            0.75 |
| serde_arrow::to_arrow     |      1.62 |            5.24 |            1.00 |            1.00 |            0.75 |
| arrow_json::ReaderBuilder |      2.15 |            6.94 |            1.33 |            1.32 |            1.00 |

### `primitives_1000`

| label                     | time [ms] | marrow to arrow | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|---------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| marrow to arrow           |      0.23 |            1.00 |            0.46 |            0.44 |            0.26 |
| serde_arrow::to_marrow    |      0.49 |            2.17 |            1.00 |            0.95 |            0.56 |
| serde_arrow::to_arrow     |      0.52 |            2.28 |            1.05 |            1.00 |            0.59 |
| arrow_json::ReaderBuilder |      0.88 |            3.88 |            1.79 |            1.71 |            1.00 |

<!-- end:benchmarks -->

## License

```text
Copyright (c) 2021 - 2024 Christopher Prohm and contributors

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
