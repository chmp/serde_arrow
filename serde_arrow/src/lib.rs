//! # `serde_arrow` - convert sequences Rust objects to / from arrow arrays
//!
//! The arrow in-memory format is a powerful way to work with data frame like
//! structures. However, the API of the underlying Rust crates can be at times
//! cumbersome to use due to the statically typed nature of Rust. `serde_arrow`,
//! offers a simple way to convert Rust objects into Arrow arrays and back.
//! `serde_arrow` relies on [Serde](https://serde.rs) to interpret Rust objects.
//! Therefore, adding support for `serde_arrow` to custom types is as easy as
//! using Serde's derive macros.
//!
//! In the Rust ecosystem there are two competing implementations of the arrow
//! in-memory format, [`arrow`](https://github.com/apache/arrow-rs) and
//! [`arrow2`](https://github.com/jorgecarleitao/arrow2). `serde_arrow` supports
//! both. The supported arrow implementations can be selected via
//! [features](#features).
//!
//! `serde_arrow` relies on a schema to translate between Rust and Arrow as
//! their type systems do not directly match. The schema is expressed as a
//! collection of Arrow fields with additional metadata describing the arrays.
//! E.g., to convert Rust strings containing timestamps to Date64 arrays, the
//! schema should contain a  `Date64`. `serde_arrow` supports to derive the
//! schema from the data itself via schema tracing, but does not require it. It
//! is always possible to specify the schema manually. See the [`schema`
//! module][schema] and [`SchemaLike`][schema::SchemaLike] for further details.
//!
#![cfg_attr(
    all(has_arrow, has_arrow2),
    doc = r#"
## Overview

| Operation     |                                                                   |
|:--------------|:------------------------------------------------------------------|
| Rust to Arrow | [`to_record_batch`], [`to_arrow`]                                 |
| Arrow to Rust | [`from_record_batch`], [`from_arrow`]                             |
| Array Builder | [`ArrayBuilder::from_arrow`]                                      |
| Serializer    | [`ArrayBuilder::from_arrow`] + [`Serializer::new`]                |
| Deserializer  | [`Deserializer::from_record_batch`], [`Deserializer::from_arrow`] |
"#
)]
//!
//! See also:
//!
//! - the [quickstart guide][_impl::docs::quickstart] for more examples of how
//!   to use this package
//! - the [status summary][_impl::docs::status] for an overview over the
//!   supported Arrow and Rust constructs
//!
//! ## `arrow` Example
//!
//! ```rust
//! # use serde::{Deserialize, Serialize};
//! # #[cfg(has_arrow)]
//! # fn main() -> serde_arrow::Result<()> {
//! # use serde_arrow::_impl::arrow;
//! use arrow::datatypes::FieldRef;
//! use serde_arrow::schema::{SchemaLike, TracingOptions};
//!
//! ##[derive(Serialize, Deserialize)]
//! struct Record {
//!     a: f32,
//!     b: i32,
//! }
//!
//! let records = vec![
//!     Record { a: 1.0, b: 1 },
//!     Record { a: 2.0, b: 2 },
//!     Record { a: 3.0, b: 3 },
//! ];
//!
//! // Determine Arrow schema
//! let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
//!
//! // Build the record batch
//! let batch = serde_arrow::to_record_batch(&fields, &records)?;
//! # Ok(())
//! # }
//! # #[cfg(not(has_arrow))]
//! # fn main() { }
//! ```
//!
//! The `RecordBatch` can then be written to disk, e.g., as parquet using
//! the [`ArrowWriter`] from the [`parquet`] crate.
//!
//! [`ArrowWriter`]: https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html
//! [`parquet`]: https://docs.rs/parquet/latest/parquet/
//!
//! # Features:
//!
//! The version of `arrow` or `arrow2` used can be selected via features. Per
//! default no arrow implementation is used. In that case only the base features
//! of `serde_arrow` are available.
//!
//! The `arrow-*` and `arrow2-*` feature groups are compatible with each other.
//! I.e., it is possible to use `arrow` and `arrow2` together. Within each group
//! the highest version is selected, if multiple features are activated. E.g,
//! when selecting  `arrow2-0-16` and `arrow2-0-17`, `arrow2=0.17` will be used.
//!
//! Available features:
//!
//! | Arrow Feature | Arrow Version |
//! |---------------|---------------|
// arrow-version:insert: //! | `arrow-{version}`    | `arrow={version}`    |
//! | `arrow-52`    | `arrow=52`    |
//! | `arrow-51`    | `arrow=51`    |
//! | `arrow-50`    | `arrow=50`    |
//! | `arrow-49`    | `arrow=49`    |
//! | `arrow-48`    | `arrow=48`    |
//! | `arrow-47`    | `arrow=47`    |
//! | `arrow-46`    | `arrow=46`    |
//! | `arrow-45`    | `arrow=45`    |
//! | `arrow-44`    | `arrow=44`    |
//! | `arrow-43`    | `arrow=43`    |
//! | `arrow-42`    | `arrow=42`    |
//! | `arrow-41`    | `arrow=41`    |
//! | `arrow-40`    | `arrow=40`    |
//! | `arrow-39`    | `arrow=39`    |
//! | `arrow-38`    | `arrow=38`    |
//! | `arrow-37`    | `arrow=37`    |
//! | `arrow2-0-17` | `arrow2=0.17` |
//! | `arrow2-0-16` | `arrow2=0.16` |

// be more forgiving without any active implementation
#[cfg_attr(all(not(has_arrow), not(has_arrow2)), allow(unused))]
mod internal;

/// *Internal. Do not use*
///
/// This module is an internal implementation detail and not subject to any
/// compatibility promises. It re-exports the  arrow impls selected via features
/// to allow usage in doc tests or benchmarks.
///
#[rustfmt::skip]
pub mod _impl {
    #[allow(unused)]
    macro_rules! build_arrow_crate {
        ($arrow_array:ident, $arrow_buffer:ident, $arrow_data:ident, $arrow_schema:ident) => {
            /// A "fake" arrow crate re-exporting the relevant definitions of the
            /// used arrow-* subcrates
            #[doc(hidden)]
            pub mod arrow {
                /// The raw arrow packages
                pub mod _raw {
                    pub use $arrow_array as array;
                    pub use $arrow_buffer as buffer;
                    pub use $arrow_data as data;
                    pub use $arrow_schema as schema;
                }
                pub mod array {
                    pub use $arrow_array::RecordBatch;
                    pub use $arrow_array::array::{
                        Array,
                        ArrayRef,
                        ArrowPrimitiveType,
                        BooleanArray,
                        DictionaryArray,
                        FixedSizeBinaryArray,
                        FixedSizeListArray,
                        GenericListArray,
                        GenericBinaryArray,
                        GenericStringArray,
                        LargeStringArray,
                        make_array,
                        MapArray,
                        NullArray,
                        OffsetSizeTrait,
                        PrimitiveArray,
                        StringArray,
                        StructArray,
                        UnionArray,
                    };
                    pub use $arrow_data::ArrayData;
                }
                pub mod buffer {
                    pub use $arrow_buffer::buffer::{Buffer, ScalarBuffer};
                }
                pub mod datatypes {
                    pub use $arrow_array::types::{
                        ArrowDictionaryKeyType,
                        ArrowPrimitiveType,
                        Date32Type,
                        Date64Type,
                        Decimal128Type,
                        DurationMicrosecondType,
                        DurationMillisecondType,
                        DurationNanosecondType,
                        DurationSecondType,
                        Float16Type,
                        Float32Type,
                        Float64Type,
                        Int16Type,
                        Int32Type,
                        Int64Type,
                        Int8Type,
                        Time32MillisecondType,
                        Time32SecondType,
                        Time64MicrosecondType,
                        Time64NanosecondType,
                        TimestampMicrosecondType,
                        TimestampMillisecondType,
                        TimestampNanosecondType,
                        TimestampSecondType,
                        UInt16Type,
                        UInt32Type,
                        UInt64Type,
                        UInt8Type,
                    };
                    pub use $arrow_buffer::ArrowNativeType;
                    pub use $arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit, UnionMode};
                }
                pub mod error {
                    pub use $arrow_schema::ArrowError;
                }
            }
        };
    }

    // arrow-version:insert: #[cfg(has_arrow_{version})] build_arrow_crate!(arrow_array_{version}, arrow_buffer_{version}, arrow_data_{version}, arrow_schema_{version});
    #[cfg(has_arrow_52)] build_arrow_crate!(arrow_array_52, arrow_buffer_52, arrow_data_52, arrow_schema_52);
    #[cfg(has_arrow_51)] build_arrow_crate!(arrow_array_51, arrow_buffer_51, arrow_data_51, arrow_schema_51);
    #[cfg(has_arrow_50)] build_arrow_crate!(arrow_array_50, arrow_buffer_50, arrow_data_50, arrow_schema_50);
    #[cfg(has_arrow_49)] build_arrow_crate!(arrow_array_49, arrow_buffer_49, arrow_data_49, arrow_schema_49);
    #[cfg(has_arrow_48)] build_arrow_crate!(arrow_array_48, arrow_buffer_48, arrow_data_48, arrow_schema_48);
    #[cfg(has_arrow_47)] build_arrow_crate!(arrow_array_47, arrow_buffer_47, arrow_data_47, arrow_schema_47);
    #[cfg(has_arrow_46)] build_arrow_crate!(arrow_array_46, arrow_buffer_46, arrow_data_46, arrow_schema_46);
    #[cfg(has_arrow_45)] build_arrow_crate!(arrow_array_45, arrow_buffer_45, arrow_data_45, arrow_schema_45);
    #[cfg(has_arrow_44)] build_arrow_crate!(arrow_array_44, arrow_buffer_44, arrow_data_44, arrow_schema_44);
    #[cfg(has_arrow_43)] build_arrow_crate!(arrow_array_43, arrow_buffer_43, arrow_data_43, arrow_schema_43);
    #[cfg(has_arrow_42)] build_arrow_crate!(arrow_array_42, arrow_buffer_42, arrow_data_42, arrow_schema_42);
    #[cfg(has_arrow_41)] build_arrow_crate!(arrow_array_41, arrow_buffer_41, arrow_data_41, arrow_schema_41);
    #[cfg(has_arrow_40)] build_arrow_crate!(arrow_array_40, arrow_buffer_40, arrow_data_40, arrow_schema_40);
    #[cfg(has_arrow_39)] build_arrow_crate!(arrow_array_39, arrow_buffer_39, arrow_data_39, arrow_schema_39);
    #[cfg(has_arrow_38)] build_arrow_crate!(arrow_array_38, arrow_buffer_38, arrow_data_38, arrow_schema_38);
    #[cfg(has_arrow_37)] build_arrow_crate!(arrow_array_37, arrow_buffer_37, arrow_data_37, arrow_schema_37);
    #[cfg(has_arrow_36)] build_arrow_crate!(arrow_array_36, arrow_buffer_36, arrow_data_36, arrow_schema_36);

    /// Documentation
    pub mod docs {
        #[doc(hidden)]
        pub mod defs;

        pub mod quickstart;

        #[doc = include_str!("../Status.md")]
        #[cfg(not(doctest))]
        pub mod status {}
    }

    // Reexport for tests
    #[doc(hidden)]
    pub use crate::internal::{
        error::{PanicOnError, PanicOnErrorError},
        serialization::array_builder::ArrayBuilder,
    };
}

#[cfg(all(test, has_arrow))]
mod test_with_arrow;

#[cfg(test)]
mod test;

pub use crate::internal::error::{Error, Result};

pub use crate::internal::deserializer::Deserializer;
pub use crate::internal::serializer::Serializer;

pub use crate::internal::array_builder::ArrayBuilder;

#[cfg(has_arrow)]
mod arrow_impl;

#[cfg(has_arrow)]
pub use arrow_impl::api::{from_arrow, from_record_batch, to_arrow, to_record_batch};

#[deny(missing_docs)]
/// Helpers that may be useful when using `serde_arrow`
pub mod utils {
    pub use crate::internal::utils::{Item, Items};
}

/// The mapping between Rust and Arrow types
///
/// To convert between Rust objects and Arrow types, `serde_arrows` requires
/// schema information as a list of Arrow fields with additional metadata. See
/// [`SchemaLike`][crate::schema::SchemaLike] for details on how to specify the
/// schema.
///
/// The default mapping of Rust types to [Arrow types][arrow-types] is as
/// follows:
///
/// [arrow-types]:
///     https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html
///
/// - Unit (`()`): `Null`
/// - Booleans (`bool`): `Boolean`
/// - Integers (`u8`, .., `u64`, `i8`, .., `i64`): `UInt8`, .., `Uint64`,
///   `Int8`, .. `UInt64`
/// - Floats (`f32`, `f64`): `Float32`, `Float64`
/// - Strings (`str`, `String`, ..): `LargeUtf8` with i64 offsets
/// - Sequences: `LargeList` with i64 offsets
/// - Structs / Map / Tuples: `Struct` type
/// - Enums: dense Unions. Each variant is mapped to a separate field. Its type
///   depends on the union type: Field-less variants are mapped to `NULL`. New
///   type variants are mapped according to their inner type. Other variant
///   types are mapped to struct types.
///
/// All customization of the types happens by including a suitable
/// [`Strategy`][crate::schema::Strategy] in the metadata of the fields. For
/// example, to let `serde_arrow` handle date time objects that are serialized
/// to strings (chrono's default), use
///
/// ```rust
/// # #[cfg(feature="has_arrow2")]
/// # fn main() {
/// # use arrow2::datatypes::{DataType, Field};
/// # use serde_arrow::schema::{STRATEGY_KEY, Strategy};
/// # let mut field = Field::new("my_field", DataType::Null, false);
/// field.data_type = DataType::Date64;
/// field.metadata = Strategy::UtcStrAsDate64.into();
/// # }
/// # #[cfg(not(feature="has_arrow2"))]
/// # fn main() {}
/// ```

#[deny(missing_docs)]
pub mod schema {
    pub use crate::internal::schema::{
        Overwrites, SchemaLike, SerdeArrowSchema, Strategy, TracingOptions, STRATEGY_KEY,
    };

    /// Support for [canonical extension types][ext-docs]. This module is experimental without semver guarantees.
    ///
    /// [ext-docs]: https://arrow.apache.org/docs/format/CanonicalExtensions.html
    pub mod ext {
        pub use crate::internal::schema::extensions::{
            FixedShapeTensorField, VariableShapeTensorField,
        };
    }
}
