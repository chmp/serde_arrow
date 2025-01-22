// NOTE: This file is a Rust file to doctest the code examples
//! # Quickstart guide
//!
//! **Contents**
//!
//! 1. [Modifying data types](#modifying-data-types)
//! 2. [Decimals](#decimals)
//! 3. [Dictionary encoding for strings](#dictionary-encoding-for-strings)
//! 4. [Working with enums](#working-with-enums)
//! 5. [Convert from arrow2 to arrow arrays](#convert-from-arrow2-to-arrow-arrays)
//!
//! The examples assume the following items to be in scope:
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow as arrow;
//! use arrow::datatypes::{DataType, FieldRef};
//! use serde_arrow::{
//!     schema::{SchemaLike, Strategy, TracingOptions},
//!     utils::{Item, Items},
//! };
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! ## Modifying data types
//!
//! When using `chrono`'s types, such as `NaiveDate`, `NaiveTime`, `DateTime<Utc>`, or
//! `NaiveDateTime`, the values are per default encoded as strings. To store them compactly as
//! integer columns, the data type has to be modified.
//!
//! For example, consider a list of [`NaiveDateTime`][chrono::NaiveDateTime] objects. The traced
//! field `val` will be of type `Utf8`.
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::arrow::datatypes::{DataType, FieldRef};
//! # use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Item};
//! use chrono::NaiveDateTime;
//!
//! let items: &[Item<NaiveDateTime>] = &[
//!     Item(NaiveDateTime::from_timestamp_opt(12 * 60 * 60 * 24, 0).unwrap()),
//!     // ...
//! ];
//!
//! let fields = Vec::<FieldRef>::from_samples(items, TracingOptions::default())?;
//! assert_eq!(fields[0].data_type(), &DataType::LargeUtf8);
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! To store it as a `Timestamp` field, modify the data type as in
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow::datatypes::{DataType, TimeUnit, Field};
//! # use serde_arrow::schema::Strategy;
//! # let mut fields = vec![Field::new("dummy", DataType::Null, true)];
//! fields[0] = Field::new("item", DataType::Timestamp(TimeUnit::Millisecond, None), false);
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! Integer fields containing timestamps in milliseconds since the epoch or `DateTime<Utc>` objects
//! can be directly stored as `Timestamp(..)` without any configuration:
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use std::sync::Arc;
//! # use serde_arrow::_impl::arrow::datatypes::{DataType, TimeUnit, Field};
//! # use serde_arrow::utils::Item;
//! let records: &[Item<i64>] = &[
//!     Item(12 * 60 * 60 * 24 * 1000),
//!     Item(9 * 60 * 60 * 24 * 1000),
//! ];
//!
//! let fields = vec![
//!     Arc::new(Field::new("item", DataType::Timestamp(TimeUnit::Millisecond, None), false)),
//! ];
//! let arrays = serde_arrow::to_arrow(&fields, records)?;
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! ## Decimals
//!
//! To serialize decimals, use the `Decimal128(precision, scale)` data type:
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::arrow::datatypes::FieldRef;
//! # use serde_arrow::{schema::SchemaLike, utils::Item};
//! use std::str::FromStr;
//!
//! use bigdecimal::BigDecimal;
//! use serde_json::json;
//!
//! let items = &[
//!     Item(BigDecimal::from_str("1.23").unwrap()),
//!     Item(BigDecimal::from_str("4.56").unwrap()),
//! ];
//!
//! let fields = Vec::<FieldRef>::from_value(&json!([
//!     {"name": "item", "data_type": "Decimal128(5, 2)"},
//! ]))?;
//!
//! let arrays = serde_arrow::to_arrow(&fields, items)?;
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! ## Dictionary encoding for strings
//!
//! Strings with repeated values can be encoded as dictionaries. The data type of the corresponding
//! field must be changed to `Dictionary`.
//!
//! For an existing field this can be done via:
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() {
//! # use serde_arrow::_impl::arrow::datatypes::{Field, DataType};
//! let data_type = DataType::Dictionary(
//!     // the integer type used for the keys
//!     Box::new(DataType::UInt32),
//!     // the data type of the values
//!     Box::new(DataType::Utf8),
//! );
//! let field = Field::new("item", data_type, false);
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! To dictionary encode all string fields, set the `string_dictionary_encoding` of
//! `TracingOptions`, when tracing the fields:
//!
//! ```rust
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::_impl::PanicOnError<()> {
//! # use serde_arrow::_impl::arrow::datatypes::FieldRef;
//! # use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Item};
//! let items = &[Item("foo"), Item("bar")];
//! let fields = Vec::<FieldRef>::from_samples(
//!     items,
//!     TracingOptions::default().string_dictionary_encoding(true),
//! )?;
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! ## Working with enums
//!
//! Rust enums correspond to arrow's union types and are supported by `serde_arrow`. Both enums with
//! and without fields are supported. Variants without fields are mapped to null arrays. Only
//! variants that are included in schema can be serialized or deserialized and the variants must
//! have the correct index. When using
//! [`SchemaLike::from_type`][crate::schema::SchemaLike::from_type] these requirements will
//! automatically be met.
//!
//! For example:
//!
//! ```rust
//! enum MyEnum {
//!     VariantWithoutData,
//!     Pair(u32, u32),
//!     NewType(Inner),
//! }
//!
//! struct Inner {
//!     a: f32,
//!     b: f32,
//! }
//! ```
//!
//! will be mapped to the following arrow union:
//!
//! - `type = 0`: `Null`
//! - `type = 1`: `Struct { 0: u32, 1: u32 }`
//! - `type = 2`: `Struct { a: f32, b: f32 }`
//!
//! Enums without data can also be serialized to and deserialized from strings, both dictionary
//! encoded or non-dictionary encoded. To select this encoding, either set the field data type
//! manually to a string data type or trace the field with `enums_without_data_as_strings(true)`.
//! E.g.,
//!
//! ```rust
//! # use serde::{Deserialize, Serialize};
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::Result<()> {
//! # use std::sync::Arc;
//! # use serde_arrow::_impl::arrow;
//! # use arrow::datatypes::{DataType, FieldRef};
//! # use serde_arrow::{schema::{SchemaLike, TracingOptions}, utils::Item};
//! #
//! ##[derive(Serialize, Deserialize)]
//! enum U {
//!     A,
//!     B,
//!     C,
//! }
//!
//! // Option 1: trace the type with enums_without_data_as_strings
//! let tracing_options = TracingOptions::default().enums_without_data_as_strings(true);
//! let fields_v1 = Vec::<FieldRef>::from_type::<Item<U>>(tracing_options)?;
//!
//! // Option 2: overwrite the field
//! let tracing_options = TracingOptions::default().allow_null_fields(true);
//! let mut fields_v2 =  Vec::<FieldRef>::from_type::<Item<U>>(tracing_options)?;
//! fields_v2[0] = fields_v2[0].as_ref()
//!     .clone()
//!     .with_data_type(DataType::Dictionary(
//!         Box::new(DataType::UInt32),
//!         Box::new(DataType::LargeUtf8),
//!     ))
//!     .into();
//!
//! assert_eq!(fields_v1, fields_v2);
//!
//! // Option 3: create the schema directly with the relevant type
//! use serde_json::json;
//!
//! let fields_v3 = Vec::<FieldRef>::from_value(&json!([
//!     {
//!         "name": "item",
//!         "data_type": "Dictionary",
//!         "children": [
//!             {"name": "key", "data_type": "U32"},
//!             {"name": "value", "data_type": "LargeUtf8"},
//!         ],
//!     },
//! ]))?;
//!
//! assert_eq!(fields_v1, fields_v3);
//! #
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))] fn main() { }
//! ```
//!
//! ## Convert from arrow2 to arrow arrays
//!
//! Both `arrow` and `arrow2` use the Arrow memory format. Hence, it is possible to convert arrays
//! between both packages with minimal work using their respective FFI interfaces:
//!
//! - [`arrow2::ffi::export_field_to_c`](https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_field_to_c.html)
//! - [`arrow2::ffi_export_array_to_c`](https://docs.rs/arrow2/latest/arrow2/ffi/fn.export_array_to_c.html)
//! - [`arrow::ffi::ArrowArray::new`](https://docs.rs/arrow/latest/arrow/ffi/struct.ArrowArray.html#method.new)
//!
//! The arrow2 crate includes [a helper
//! trait](https://docs.rs/arrow2/latest/arrow2/array/trait.Arrow2Arrow.html) to perform this
//! conversion when used with the `arrow` feature.
//!
//!