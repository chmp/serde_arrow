# `serde_arrow` - convert sequences of structs / maps to and from arrow tables

[[Crate info]](https://crates.io/crates/serde_arrow)
| [[API docs]](https://docs.rs/serde_arrow/latest/serde_arrow/)
| [Example](#example)
| [How does it work?](serde_arrow/Implementation.md)
| [Status](serde_arrow/Implementation.md#status)
| [License](#license)

**Warning:** this package is in an experiment at the moment.

[Arrow][arrow] is a powerful library to work with data frame like structures.
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
use serde_arrow::Schema;
let schema = Schema::from_records(&examples)?;

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
