//! # `serde_arrow` - convert sequences Rust objects to / from arrow arrays
//!
//! The arrow in-memory format is a powerful way to work with data frame like structures. However,
//! the API of the underlying Rust crates can be at times cumbersome to use due to the statically
//! typed nature of Rust. `serde_arrow`, offers a simple way to convert Rust objects into Arrow
//! arrays and back. `serde_arrow` relies on [Serde](https://serde.rs) to interpret Rust objects.
//! Therefore, adding support for `serde_arrow` to custom types is as easy as using Serde's derive
//! macros.
//!
//! `serde_arrow` mainly targets the [`arrow`](https://github.com/apache/arrow-rs) crate, but also
//! supports the deprecated [`arrow2`](https://github.com/jorgecarleitao/arrow2) crate. The arrow
//! implementations can be selected via [features](#features).
//!
//! `serde_arrow` relies on a schema to translate between Rust and Arrow as their type systems do
//! not directly match. The schema is expressed as a collection of Arrow fields with additional
//! metadata describing the arrays. E.g., to convert a vector of Rust strings representing
//! timestamps to an arrow `Timestamp` array, the schema should contain a field with data type
//! `Timestamp`. `serde_arrow` supports to derive the schema from the data or the Rust types
//! themselves via schema tracing, but does not require it. It is always possible to specify the
//! schema manually. See the [`schema` module][schema] and [`SchemaLike`][schema::SchemaLike] for
//! further details.
//!
#![cfg_attr(
    all(has_arrow, has_arrow2),
    doc = r#"
## Overview

| Operation        | [`arrow-*`](#features)                                            | [`arrow2-*`](#features)                             | `marrow`                                            |
|:-----------------|:------------------------------------------------------------------|:----------------------------------------------------|:----------------------------------------------------|
| Rust to Arrow    | [`to_record_batch`], [`to_arrow`]                                 | [`to_arrow2`]                                       | [`to_marrow`]                                       |
| Arrow to Rust    | [`from_record_batch`], [`from_arrow`]                             | [`from_arrow2`]                                     | [`from_marrow`]                                     |
| [`ArrayBuilder`] | [`ArrayBuilder::from_arrow`]                                      | [`ArrayBuilder::from_arrow2`]                       | [`ArrayBuilder::from_marrow`]                       |
| [`Serializer`]   | [`ArrayBuilder::from_arrow`] + [`Serializer::new`]                | [`ArrayBuilder::from_arrow2`] + [`Serializer::new`] | [`ArrayBuilder::from_marrow`] + [`Serializer::new`] |
| [`Deserializer`] | [`Deserializer::from_record_batch`], [`Deserializer::from_arrow`] | [`Deserializer::from_arrow2`]                       | [`Deserializer::from_marrow`]                       |
"#
)]
//!
//! See also:
//!
//! - the [quickstart guide][_impl::docs::quickstart] for more examples of how to use this package
//! - the [status summary][_impl::docs::status] for an overview over the supported Arrow and Rust
//!   constructs
//!
//! ## Example
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
//! The `RecordBatch` can then be written to disk, e.g., as parquet using the [`ArrowWriter`] from
//! the [`parquet`] crate.
//!
//! [`ArrowWriter`]:
//!     https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html
//! [`parquet`]: https://docs.rs/parquet/latest/parquet/
//!
//! # Features:
//!
//! The version of `arrow` or `arrow2` used can be selected via features. Per default no arrow
//! implementation is used. In that case only the base features of `serde_arrow` are available.
//!
//! The `arrow-*` and `arrow2-*` feature groups are compatible with each other. I.e., it is possible
//! to use `arrow` and `arrow2` together. Within each group the highest version is selected, if
//! multiple features are activated. E.g, when selecting  `arrow2-0-16` and `arrow2-0-17`,
//! `arrow2=0.17` will be used.
//!
//! Note that because the highest version is selected, the features are not additive. In particular,
//! it is not possible to use `serde_arrow::to_arrow` for multiple different `arrow` versions at the
//! same time. See the next section for how to use `serde_arrow` in library code.
//!
//! Available features:
//!
//! | Arrow Feature | Arrow Version |
//! |---------------|---------------|
// arrow-version:insert: //! | `arrow-{version}`    | `arrow={version}`    |
//! | `arrow-56`    | `arrow=56`    |
//! | `arrow-55`    | `arrow=55`    |
//! | `arrow-54`    | `arrow=54`    |
//! | `arrow-53`    | `arrow=53`    |
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
//!
//! # Usage in  libraries
//!
//! In libraries, it is not recommended to use the `arrow` and `arrow2` functions directly. Rather
//! it is recommended to rely on the [`marrow`] based functionality, as the features of [`marrow`]
//! are designed to be strictly additive.
//!
//! For example to build a record batch, first build the corresponding marrow types and then use
//! them to build the record batch:
//!
//! ```rust
//! # use serde::{Deserialize, Serialize};
//! # fn main() -> serde_arrow::Result<()> {
//! # #[cfg(has_arrow)] {
//! # use serde_arrow::_impl::arrow;
//! # use std::sync::Arc;
//! # use serde_arrow::schema::{SchemaLike, TracingOptions};
//! #
//! # #[derive(Serialize, Deserialize)]
//! # struct Record {
//! #     a: f32,
//! #     b: i32,
//! # }
//! #
//! # let records = vec![
//! #     Record { a: 1.0, b: 1 },
//! #     Record { a: 2.0, b: 2 },
//! #     Record { a: 3.0, b: 3 },
//! # ];
//! #
//! // Determine Arrow schema
//! let fields = Vec::<marrow::datatypes::Field>::from_type::<Record>(TracingOptions::default())?;
//!
//! // Build the marrow arrays
//! let arrays = serde_arrow::to_marrow(&fields, &records)?;
//!
//! // Build the record batch
//! let arrow_fields = fields.iter()
//!     .map(arrow::datatypes::Field::try_from)
//!     .collect::<Result<Vec<_>, _>>()?;
//!
//! let arrow_arrays = arrays.into_iter()
//!     .map(arrow::array::ArrayRef::try_from)
//!     .collect::<Result<Vec<_>, _>>()?;
//!
//! let record_batch = arrow::array::RecordBatch::try_new(
//!     Arc::new(arrow::datatypes::Schema::new(arrow_fields)),
//!     arrow_arrays,
//! );
//! # }
//! # Ok(())
//! # }
//! ```

// be more forgiving without any active implementation
#[cfg_attr(not(any(has_arrow, has_arrow2)), allow(unused))]
mod internal;

/// *Internal. Do not use*
///
/// This module is an internal implementation detail and not subject to any
/// compatibility promises. It re-exports the  arrow impls selected via features
/// to allow usage in doc tests or benchmarks.
///
#[rustfmt::skip]
pub mod _impl {

    #[cfg(has_arrow2_0_17)]
    #[doc(hidden)]
    pub use arrow2_0_17 as arrow2;

    #[cfg(has_arrow2_0_16)]
    pub use arrow2_0_16 as arrow2;

    #[allow(unused)]
    macro_rules! build_arrow_crate {
        ($arrow_array:ident, $arrow_schema:ident) => {
            /// A "fake" arrow crate re-exporting the relevant definitions of the
            /// used arrow-* subcrates
            #[doc(hidden)]
            pub mod arrow {
                /// The raw arrow packages
                pub mod _raw {
                    pub use {$arrow_array as array, $arrow_schema as schema};
                }
                pub mod array {
                    pub use $arrow_array::{RecordBatch, array::{Array, ArrayRef}};
                }
                pub mod datatypes {
                    pub use $arrow_schema::{DataType, Field, FieldRef, Schema, TimeUnit};
                }
                pub mod error {
                    pub use $arrow_schema::ArrowError;
                }
            }
        };
    }

    // arrow-version:insert:     #[cfg(has_arrow_{version})] build_arrow_crate!(arrow_array_{version}, arrow_schema_{version});
    #[cfg(has_arrow_56)] build_arrow_crate!(arrow_array_56, arrow_schema_56);
    #[cfg(has_arrow_55)] build_arrow_crate!(arrow_array_55, arrow_schema_55);
    #[cfg(has_arrow_54)] build_arrow_crate!(arrow_array_54, arrow_schema_54);
    #[cfg(has_arrow_53)] build_arrow_crate!(arrow_array_53, arrow_schema_53);
    #[cfg(has_arrow_52)] build_arrow_crate!(arrow_array_52, arrow_schema_52);
    #[cfg(has_arrow_51)] build_arrow_crate!(arrow_array_51, arrow_schema_51);
    #[cfg(has_arrow_50)] build_arrow_crate!(arrow_array_50, arrow_schema_50);
    #[cfg(has_arrow_49)] build_arrow_crate!(arrow_array_49, arrow_schema_49);
    #[cfg(has_arrow_48)] build_arrow_crate!(arrow_array_48, arrow_schema_48);
    #[cfg(has_arrow_47)] build_arrow_crate!(arrow_array_47, arrow_schema_47);
    #[cfg(has_arrow_46)] build_arrow_crate!(arrow_array_46, arrow_schema_46);
    #[cfg(has_arrow_45)] build_arrow_crate!(arrow_array_45, arrow_schema_45);
    #[cfg(has_arrow_44)] build_arrow_crate!(arrow_array_44, arrow_schema_44);
    #[cfg(has_arrow_43)] build_arrow_crate!(arrow_array_43, arrow_schema_43);
    #[cfg(has_arrow_42)] build_arrow_crate!(arrow_array_42, arrow_schema_42);
    #[cfg(has_arrow_41)] build_arrow_crate!(arrow_array_41, arrow_schema_41);
    #[cfg(has_arrow_40)] build_arrow_crate!(arrow_array_40, arrow_schema_40);
    #[cfg(has_arrow_39)] build_arrow_crate!(arrow_array_39, arrow_schema_39);
    #[cfg(has_arrow_38)] build_arrow_crate!(arrow_array_38, arrow_schema_38);
    #[cfg(has_arrow_37)] build_arrow_crate!(arrow_array_37, arrow_schema_37);

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

#[cfg(all(test, has_arrow, has_arrow2))]
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
pub use arrow_impl::{from_arrow, from_record_batch, to_arrow, to_record_batch};

#[cfg(has_arrow2)]
mod arrow2_impl;

#[cfg(has_arrow2)]
pub use arrow2_impl::{from_arrow2, to_arrow2};

#[deny(missing_docs)]
mod marrow_impl;

pub use marrow_impl::{from_marrow, to_marrow};

#[deny(missing_docs)]
/// Helpers that may be useful when using `serde_arrow`
pub mod utils {
    pub use crate::internal::utils::{Item, Items};
}

#[deny(missing_docs)]
/// Deserialization of items
pub mod deserializer {
    pub use crate::internal::deserializer::{DeserializerItem, DeserializerIterator};
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
            Bool8Field, FixedShapeTensorField, VariableShapeTensorField,
        };
    }
}

/// Re-export of the used marrow version
pub use marrow;
