# `serde_arrow` - convert sequences of structs / maps to and from arrow arrays

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Changes](Changes.md)
| [Example](#example)
| [Related packages](#related-packages)
| [Performance](#performance)
| [How does it work?](serde_arrow/Implementation.md)
| [Status](serde_arrow/Status.md)
| [Development](#development)
| [License](#license)

**Warning:** this package is in an experiment at the moment.

The arrow in-memory format is a powerful way to work with data frame like
structures. The surrounding ecosystem includes a rich set of libraries, ranging
from data frames via [Polars][polars] to query engines via
[DataFusion][datafusion]. However, the API of the underlying Rust crates can be
at times cumbersome to use due to the statically typed nature of Rust.

`serde_arrow`, offers a simple way to convert Rust objects into Arrow arrays and
back.  `serde_arrow` relies on the [Serde](https://serde.rs) package to
interpret Rust objects. Therefore, adding support for `serde_arrow` to custom
types is as easy as using Serde's derive macros.

In the Rust ecosystem there are two competing implemenetations of the arrow
in-memory format: [`arrow`][arrow] and [`arrow2`][arrow2]. `serde_arrow`
supports both for schema tracing and serialization from Rust structs to arrays.
Deserialization from arrays to Rust structs is currently only implemented for
`arrow2`.

[arrow]: https://docs.rs/arrow/latest/arrow/
[arrow2]: https://docs.rs/arrow2/latest/arrow2/
[polars]: https://github.com/pola-rs/polars
[datafusion]: https://github.com/apache/arrow-datafusion/

## Example

```rust
#[derive(Serialize)]
struct Item {
    a: f32,
    b: i32,
    point: Point,
}

#[derive(Serialize)]
struct Point(f32, f32);

let items = vec![
    Item { a: 1.0, b: 1, point: Point(0.0, 1.0) },
    Item { a: 2.0, b: 2, point: Point(2.0, 3.0) },
    // ...
];

// detect the field types and convert the items to arrays
use serde_arrow::arrow2::{serialize_into_fields, serialize_into_arrays};

let fields = serialize_into_fields(&items, TracingOptions::default())?;
let arrays = serialize_into_arrays(&fields, &items)?;
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

## Performance

See the [implementation notes](serde_arrow/Implementation.md) for details on how
it is implemented and [status summary](serde_arrow/Status.md) for a list of
supported Rust and Arrow constructs.

This package is optimized for ease of use, not performance. Depending on the
complexity of the types, a performance penality of 4x - 7x compared to manually
building the arrays can be expected. More complex types incur a smaller
performance penalty. See the [benches](serde_arrow/benches/arrow2.rs) for
details.

<!-- start:benchmarks -->
###  deserialize_arrow2_primitives

| label       | time [ms] | manual | serde_arrow |
|-------------|-----------|--------|-------------|
| manual      |      3.86 |   1.00 |        0.03 |
| serde_arrow |    121.02 |  31.36 |        1.00 |

###  serialize_arrow2_complex

| label                | time [ms] | manual | serde_arrow | serde_arrow_byt |
|----------------------|-----------|--------|-------------|-----------------|
| manual               |     50.92 |   1.00 |        0.40 |            0.09 |
| serde_arrow_bytecode |    127.93 |   2.51 |        1.00 |            0.23 |
| serde_arrow          |    556.96 |  10.94 |        4.35 |            1.00 |

###  serialize_arrow2_primitives

| label                | time [ms] | manual | serde_arrow | serde_arrow_byt |
|----------------------|-----------|--------|-------------|-----------------|
| manual               |      7.44 |   1.00 |        0.46 |            0.18 |
| serde_arrow_bytecode |     16.33 |   2.19 |        1.00 |            0.39 |
| serde_arrow          |     41.91 |   5.63 |        2.57 |            1.00 |

###  serialize_arrow_complex

| label                | time [ms] | arrow | serde_arrow | serde_arrow_byt |
|----------------------|-----------|-------|-------------|-----------------|
| serde_arrow_bytecode |    134.86 |  1.00 |        0.23 |            0.17 |
| serde_arrow          |    585.77 |  4.34 |        1.00 |            0.72 |
| arrow                |    808.92 |  6.00 |        1.38 |            1.00 |


<!-- end:benchmarks -->

## Related packages

- [`arrow`][arrow]: the JSON component of the official Arrow package supports
   serializing objects that support serialize via the [RawDecoder][raw-decoder]
   object. It supports primitives types, structs and lists
- [`arrow2-convert`][arrow2-convert]: adds derive macros to convert objects from
  and to arrow2 arrays. It supports primitive types, structs, lists, and
  chrono's date time types. Enum support is experimental according to the
  Readme

[raw-decoder]: https://docs.rs/arrow-json/37.0.0/arrow_json/struct.RawDecoder.html#method.serialize
[arrow2-convert]: https://github.com/DataEngineeringLabs/arrow2-convert

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
Copyright (c) 2021 - 2023 Christopher Prohm

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
