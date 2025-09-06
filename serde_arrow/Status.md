# Status

The page documents the supported types both from an Arrow and a Rust perspective.

- [Arrow data types](#arrow-data-types)
- [Rust types](#rust-types)
  - [Native / standard types](#native--standard-types)
  - [`chrono` types](#chrono-types)
  - [`jiff` types](#jiff-types)
  - [`rust_decimal::Decimal`](#rust_decimaldecimal)
  - [`bigdecimal::BigDecimal`](#bigdecimalbigdecimal)

## Arrow data types

- [x] [`Null`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Null)
- [x] [`Boolean`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Boolean)
- [x] [`Int8`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Int8),
  [`Int16`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Int16),
  [`Int32`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Int32),
  [`Int64`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Int64)
- [x] [`UInt8`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.UInt8),
  [`UInt16`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.UInt16),
  [`UInt32`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.UInt32),
  [`UInt64`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.UInt64)
- [x] [`Float16`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Float16):
  can be serialized / deserialized from Rust `f32`
- [x] [`Float32`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Float32),
  [`Float64`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Float64)
- [x] [`Timestamp`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Timestamp)
- [x] [`Date32`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Date32)
- [x] [`Date64`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Date64)
- [x] [`Time32`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Time32)
- [x] [`Time64`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Time64)
- [x] [`Duration`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Duration)
- [ ] [`Interval`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Interval)
- [x] [`Timestamp(Second | Millisecond | Microsecond | Nanosecond, None | Some("UTC"))`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Timestamp):
  at the moment only timestamps without timezone or UTC timezone are supported
- [x] [`Binary`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Binary)
- [x] [`FixedSizeBinary`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.FixedSizedBinary)
- [x] [`LargeBinary`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.LargeBinary)
- [x] [`Utf8`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Utf8)
- [x] [`LargeUtf8`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.LargeUtf8)
- [x] [`List`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.List)
- [x] [`LargeList`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.LargeList)
- [x] [`FixedSizeList`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.FixedSizeList)
- [x] [`Struct`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Struct)
- [x] [`Union`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Union):
  at the moment only dense unions are supported
- [x] [`Map`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Map):
  at the moment only unsorted maps are supported
- [x] [`Dictionary`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Dictionary):
  at the moment only Utf8 and LargeUtf8 as values are supported
- [x] [`Decimal128(precision, scale)`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Decimal128):
  decimals that are serialized to string or float are supported. `Decimal128`
  arrays are always deserialized as string. Values are truncated to the given
  `(precision, scale)` range. Values too large for this range will result in a
  serialization error.
- [ ] [`Decimal256(precision, scale)`](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html#variant.Decimal256)

## Rust types

### Native / standard types

- [x] `bool`
- [x] `i8`, `i16`, `i32`, `i64`
- [x] `u8`, `u16`, `u32`, `u64`
- [x] `f32`, `f64`
- [x] `char`: serialized as u32
- [x] `Option<T>`: if `T` is supported
- [x] `()`: serialized as a missing value, `Option<()>` is always deserialized
  as `None`
- [x] `struct S{ .. }`: if the fields are supported
- [x] `Vec<T>`: if T is supported. Any type that serializes into a Serde
  sequence is supported
- [x] `HashMap<K, V>, BTreeMap<K, V>` and similar map types are supported if `K`
  and `V` are supported
- [x] tuples: tuples or tuple structs are not yet supported. It is planned to
  map them to struct arrays with numeric field names
- [x] `enum ... { }`: enums are mapped to union arrays. At the moment options of
  unions are not supported. Also unions with more than 127 variants are not
  supported. All types of union variants (unit, newtype, tuple, struct) are
  supported
- [x] `struct S(T)`: newtype structs are supported, if `T` is supported

### `chrono` types

#### `chrono::DateTime<Utc>`

- is serialized / deserialized as strings
- can be mapped to `Utf8`, `LargeUtf8`, `Timestamp(.., Some("UTC"))`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Timestamp(Millisecond, Some("UTC"))` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

With [`chrono::serde::ts_microseconds`][chrono-ts-microseconds]:

- is serialized / deserialized  as `i64`
- can be mapped to `Utf8`, `LargeUtf8`, `Timestamp(.., Some("UTC"))`
- `from_samples` and `from_type` detect `Int64`

#### `chrono::NaiveDateTime`

- is serialized / deserialized as strings
- can be mapped to `Utf8`, `LargeUtf8`, `Timestamp(.., None)`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Timestamp(Millisecond, None)` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `chrono::NaiveTime`

- serialized / deserialized as strings
- can be mapped to `Utf8`, `LargeUtf8`, `Time32(..)` and `Time64` arrays
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Time64(Nanosecond)` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `chrono::NaiveDate`

- is serialized as Serde strings
- can be mapped to `Utf8`, `LargeUtf8`, `Date32` arrays
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Date32` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

`chrono::Duration` does not support Serde and is therefore not supported

###  `jiff` types

#### `jiff::Date`

- is serialized as Serde strings
- can me mapped to `Utf8`, `LargeUtf8`, `Date32`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Date32` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `jiff::Time`

- is serialized as Serde strings
- can me mapped to `Utf8`, `LargeUtf8`, `Time32(..)`, `Time64(..)`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Time64(Nanosecond)` when setitng `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `jiff::DateTime`

- is serialized as Serde strings
- can me mapped to `Utf8`, `LargeUtf8`, `Timestmap(.., None)`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Timestamp(Millisecond, None)` when setting `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `jiff::Timestamp`

- is serialized as Serde strings
- can me mapped to `Utf8`, `LargeUtf8`, `Timestamp(.., Some("UTC"))`
- `from_samples` detects
  - `LargeUtf8` without configuration
  - `Timestamp(Millisecond, Some("UTC"))` when setting  `guess_dates = true`
- `from_type` is not supported, as the type is not self-describing

#### `jiff::Span`

- is serialized as Serde strings
- can me mapped to `Utf8`, `LargeUtf8`, `Duration(..)`
- `from_samples` detects `LargeUtf8`
- `from_type` is not supported, as the type is not self-describing

#### `jiff::SignedDuration`

Same as `jiff::Span`

#### `jiff::Zoned`

is not supported as there is no clear way of implementation

### [`rust_decimal::Decimal`][rust_decimal::Decimal]

- for the `float` and `str` (de)serialization options when using the `Decimal128(..)` data type

### [`bigdecimal::BigDecimal`][bigdecimal::BigDecimal]

- when using the `Decimal128(..)` data type

[chrono-ts-microseconds]: https://docs.rs/chrono/latest/chrono/serde/ts_microseconds/
[rust_decimal::Decimal]: https://docs.rs/rust_decimal/latest/rust_decimal/struct.Decimal.html
[bigdecimal::BigDecimal]: https://docs.rs/bigdecimal/0.4.2/bigdecimal/struct.BigDecimal.html
