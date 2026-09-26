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

The charts compare serialization with direct Arrow builder construction and deserialization with
manual construction of the Rust records, averaged over the primitive and complex workloads. These
benchmark results are workload-specific and only indicative.

![Serialization and deserialization runtimes relative to their baselines](timings.png)

The tables below give runtimes and pairwise ratios for the
[benchmark workloads](serde_arrow_bench/benches/groups/).
Deserialization benchmarks decode 1,000 records from arrays prepared before timing. The
`serde_arrow` calls include deserializer setup and creation of owned Rust records. The manual
baseline reads the same Arrow arrays and builds the same records directly; `Deserializer::iter`
measures the record-by-record API.

<!-- start:benchmarks -->
### Serialization

#### `complex_1000`

| label                     | time [ms] | arrow builder | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|---------------------------|-----------|---------------|-----------------|-----------------|-----------------|
| arrow builder             |      0.13 |          1.00 |            0.38 |            0.33 |            0.19 |
| serde_arrow::to_marrow    |      0.35 |          2.66 |            1.00 |            0.89 |            0.50 |
| serde_arrow::to_arrow     |      0.39 |          3.00 |            1.13 |            1.00 |            0.56 |
| arrow_json::ReaderBuilder |      0.71 |          5.37 |            2.02 |            1.79 |            1.00 |

#### `primitives_1000`

| label                     | time [ms] | arrow builder | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|---------------------------|-----------|---------------|-----------------|-----------------|-----------------|
| arrow builder             |      0.01 |          1.00 |            0.20 |            0.13 |            0.05 |
| serde_arrow::to_marrow    |      0.07 |          4.90 |            1.00 |            0.62 |            0.23 |
| serde_arrow::to_arrow     |      0.11 |          7.83 |            1.60 |            1.00 |            0.37 |
| arrow_json::ReaderBuilder |      0.29 |         20.99 |            4.29 |            2.68 |            1.00 |

### Deserialization

#### `complex_1000`

| label                    | time [ms] | manual | serde_arrow::fr | serde_arrow::fr | Deserializer::i |
|--------------------------|-----------|--------|-----------------|-----------------|-----------------|
| manual                   |      0.07 |   1.00 |            0.14 |            0.14 |            0.14 |
| serde_arrow::from_marrow |      0.52 |   7.00 |            1.00 |            0.98 |            0.97 |
| serde_arrow::from_arrow  |      0.53 |   7.14 |            1.02 |            1.00 |            0.99 |
| Deserializer::iter       |      0.53 |   7.19 |            1.03 |            1.01 |            1.00 |

#### `primitives_1000`

| label                    | time [ms] | manual | serde_arrow::fr | Deserializer::i | serde_arrow::fr |
|--------------------------|-----------|--------|-----------------|-----------------|-----------------|
| manual                   |      0.03 |   1.00 |            0.24 |            0.24 |            0.23 |
| serde_arrow::from_marrow |      0.13 |   4.14 |            1.00 |            0.99 |            0.95 |
| Deserializer::iter       |      0.13 |   4.20 |            1.01 |            1.00 |            0.97 |
| serde_arrow::from_arrow  |      0.14 |   4.35 |            1.05 |            1.04 |            1.00 |
<!-- end:benchmarks -->

## License

```text
Copyright (c) 2021 - 2026 Christopher Prohm and contributors

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
