# `serde_arrow` - convert sequences of Rust objects to Arrow arrays and back again

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Changes](Changes.md)
| [Example](#example)
| [Related packages & performance](#related-packages--performance)
| [[How does it work?]](https://docs.rs/serde_arrow/latest/serde_arrow/_impl/docs/implementation/index.html)
| [Status](serde_arrow/Status.md)
| [Development](#development)
| [License](#license)

The arrow in-memory format is a powerful way to work with data frame like
structures. The surrounding ecosystem includes a rich set of libraries, ranging
from data frames via [Polars][polars] to query engines via
[DataFusion][datafusion]. However, the API of the underlying Rust crates can be
at times cumbersome to use due to the statically typed nature of Rust.

`serde_arrow`, offers a simple way to convert Rust objects into Arrow arrays and
back.  `serde_arrow` relies on the [Serde](https://serde.rs) package to
interpret Rust objects. Therefore, adding support for `serde_arrow` to custom
types is as easy as using Serde's derive macros.

In the Rust ecosystem there are two competing implementations of the arrow
in-memory format. `serde_arrow` supports both [`arrow`][arrow] and
[`arrow2`][arrow2] for schema tracing, serialization from Rust structs to
arrays, and deserialization from arrays to Rust structs.

[arrow]: https://docs.rs/arrow/latest/arrow/
[arrow2]: https://docs.rs/arrow2/latest/arrow2/
[polars]: https://github.com/pola-rs/polars
[datafusion]: https://github.com/apache/arrow-datafusion/

## Example

```rust
use serde_arrow::schema::{TracingOptions, SerdeArrowSchema};

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

let fields =
    SerdeArrowSchema::from_type::<Record>(TracingOptions::default())?
    .to_arrow2_fields()?;

let arrays = serde_arrow::to_arrow2(&fields, &records)?;
```

These arrays can now be written to disk using the helper method defined in the
[arrow2 guide][arrow2-guide]. For parquet:

```rust,ignore
use arrow2::{chunk::Chunk, datatypes::Schema};

// see https://jorgecarleitao.github.io/arrow2/io/parquet_write.html
write_chunk(
    "example.pq",
    Schema::from(fields),
    Chunk::new(arrays),
)?;
```

The written file can now be read in Python via

```python
# using polars
import polars as pl
pl.read_parquet("example.pq")

# using pandas
import pandas as pd
pd.read_parquet("example.pq")
```

[arrow2-guide]: https://jorgecarleitao.github.io/arrow2

## Related packages & Performance

- [`arrow`][arrow]: the JSON component of the official Arrow package supports
   serializing objects that support serialize via the [RawDecoder][raw-decoder]
   object. It supports primitives types, structs and lists
- [`arrow2-convert`][arrow2-convert]: adds derive macros to convert objects from
  and to arrow2 arrays. It supports primitive types, structs, lists, and
  chrono's date time types. Enum support is experimental according to the
  Readme. If performance is the main objective, `arrow2-convert` is a good
  choice as it has no or minimal overhead over building the arrays manually.

[raw-decoder]: https://docs.rs/arrow-json/37.0.0/arrow_json/struct.RawDecoder.html#method.serialize
[arrow2-convert]: https://github.com/DataEngineeringLabs/arrow2-convert

The different implementation have the following performance differences, when
compared to arrow2-convert:

![Time ](timings.png)

The detailed runtimes of the [benchmarks](./serde_arrow/benches/groups/) are listed below.

<!-- start:benchmarks -->
### complex_common_serialize(100000)

| label                        | time [ms] | arrow2_convert: | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|------------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| arrow2_convert::TryIntoArrow |     50.41 |            1.00 |            0.33 |            0.31 |            0.13 |
| serde_arrow::to_arrow2       |    150.91 |            2.99 |            1.00 |            0.94 |            0.40 |
| serde_arrow::to_arrow        |    161.25 |            3.20 |            1.07 |            1.00 |            0.43 |
| arrow_json::ReaderBuilder    |    375.00 |            7.44 |            2.48 |            2.33 |            1.00 |

### complex_common_serialize(1000000)

| label                        | time [ms] | arrow2_convert: | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|------------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| arrow2_convert::TryIntoArrow |    470.21 |            1.00 |            0.32 |            0.30 |            0.13 |
| serde_arrow::to_arrow2       |   1448.66 |            3.08 |            1.00 |            0.92 |            0.40 |
| serde_arrow::to_arrow        |   1574.82 |            3.35 |            1.09 |            1.00 |            0.43 |
| arrow_json::ReaderBuilder    |   3628.26 |            7.72 |            2.50 |            2.30 |            1.00 |

### primitives_serialize(100000)

| label                        | time [ms] | arrow2_convert: | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|------------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| arrow2_convert::TryIntoArrow |     14.11 |            1.00 |            0.46 |            0.32 |            0.11 |
| serde_arrow::to_arrow2       |     30.85 |            2.19 |            1.00 |            0.70 |            0.25 |
| serde_arrow::to_arrow        |     44.02 |            3.12 |            1.43 |            1.00 |            0.35 |
| arrow_json::ReaderBuilder    |    125.16 |            8.87 |            4.06 |            2.84 |            1.00 |

### primitives_serialize(1000000)

| label                        | time [ms] | arrow2_convert: | serde_arrow::to | serde_arrow::to | arrow_json::Rea |
|------------------------------|-----------|-----------------|-----------------|-----------------|-----------------|
| arrow2_convert::TryIntoArrow |    144.14 |            1.00 |            0.43 |            0.33 |            0.11 |
| serde_arrow::to_arrow2       |    333.09 |            2.31 |            1.00 |            0.75 |            0.24 |
| serde_arrow::to_arrow        |    441.65 |            3.06 |            1.33 |            1.00 |            0.32 |
| arrow_json::ReaderBuilder    |   1362.27 |            9.45 |            4.09 |            3.08 |            1.00 |

<!-- end:benchmarks -->

## Development

All common tasks are bundled in the `x.py` script:

```bash
# format the code and run tests
python x.py precommit
```

Run `python x.py --help` for details. The script only uses standard Python
modules can can be run without installing further packages.

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
