# `serde_arrow` - convert sequences of structs / maps to and from arrow tables

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Changes](Changes.md)
| [Example](#example)
| [How does it work?](serde_arrow/Implementation.md)
| [Status](serde_arrow/Implementation.md#status)
| [License](#license)

**Warning:** this package is in an experiment at the moment.

[Arrow2][arrow2] is a powerful library to work with data frame like structures.
The surrounding ecosystem includes a rich set of libraries, ranging from data
frames via [Polars][polars] to query engines via [DataFusion][datafusion].
However, it's API due to the statically typed nature of Rust can be at times
cumbersome to use directly. This package, `serde_arrow`, tries to bridge this
gap by offering a simple way to convert Rust objects into Arrow objects and vice
versa.  `serde_arrow` relies on the [Serde](https://serde.rs) package to
interpret Rust objects. Therefore, adding support for `serde_arrow` to custom
types is as easy as using Serde's derive macros. 

See the [implementation notes](serde_arrow/Implementation.md) for details on how
it is implemented. This package is optimized for ease of use, not performance.

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

let fields = serialize_into_fields(&items)?;
let arrays = serialize_into_arrays(&fields, &items)?;

// using the helper method defined in the arrow2 guide at
// https://jorgecarleitao.github.io/arrow2/io/parquet_write.html
use  arrow2::{chunk::Chunk, datatypes::Schema};

write_chunk(
    "example.pq",
    Schema::from(fields),
    Chunk::new(items),
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
