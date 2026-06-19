//! # `serde_arrow` - convert sequences of Rust objects to and from Arrow arrays
//!
//! The Arrow in-memory format is a powerful way to work with data-frame-like structures. However,
//! the API of the underlying Rust crates can be at times cumbersome to use due to the statically
//! typed nature of Rust. `serde_arrow` offers a simple way to convert Rust objects into Arrow
//! arrays and back. `serde_arrow` relies on [Serde](https://serde.rs) to interpret Rust objects.
//! Therefore, adding support for `serde_arrow` to custom types is as easy as using Serde's derive
//! macros.
//!
//! `serde_arrow` supports the [`arrow`](https://github.com/apache/arrow-rs) crate. The Arrow
//! version can be selected via [features](#features).
//!
//! `serde_arrow` relies on a schema to translate between Rust and Arrow as their type systems do
//! not directly match. The schema is expressed as a collection of Arrow fields with additional
//! metadata describing the arrays. For example, to convert a vector of Rust strings representing
//! timestamps to an Arrow `Timestamp` array, the schema should contain a field with data type
//! `Timestamp`. `serde_arrow` can derive the schema from the data or the Rust types
//! themselves via schema tracing, but does not require it. It is always possible to specify the
//! schema manually. See the [`schema` module][schema] and [`SchemaLike`][schema::SchemaLike] for
//! further details.
//!
#![cfg_attr(
    has_arrow,
    doc = r#"
## Overview

| Operation        | [`arrow-*`](#features)                                            | `marrow`                                            |
|:-----------------|:------------------------------------------------------------------|:----------------------------------------------------|
| Rust to Arrow    | [`to_record_batch`], [`to_arrow`]                                 | [`to_marrow`]                                       |
| Arrow to Rust    | [`from_record_batch`], [`from_arrow`]                             | [`from_marrow`]                                     |
| [`ArrayBuilder`] | [`ArrayBuilder::from_arrow`]                                      | [`ArrayBuilder::from_marrow`]                       |
| [`Serializer`]   | [`ArrayBuilder::from_arrow`] + [`Serializer::new`]                | [`ArrayBuilder::from_marrow`] + [`Serializer::new`] |
| [`Deserializer`] | [`Deserializer::from_record_batch`], [`Deserializer::from_arrow`] | [`Deserializer::from_marrow`]                       |
"#
)]
//!
//! See also:
//!
//! - the [quickstart guide][_impl::docs::quickstart] for more examples of how to use this package
//! - the [status summary][_impl::docs::status] for an overview of the supported Arrow and Rust
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
//! The version of `arrow` used can be selected via features. By default, no Arrow implementation
//! is used. In that case only the base features of `serde_arrow` are available.
//!
//! The highest selected `arrow-*` version is used if multiple features are activated.
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
//! | `arrow-59`    | `arrow=59`    |
//! | `arrow-58`    | `arrow=58`    |
//! | `arrow-57`    | `arrow=57`    |
//! | `arrow-56`    | `arrow=56`    |
//! | `arrow-55`    | `arrow=55`    |
//! | `arrow-54`    | `arrow=54`    |
//! | `arrow-53`    | `arrow=53`    |
//!
//! # Usage in libraries
//!
//! In libraries, it is not recommended to use the `arrow` functions directly. Rather it is
//! recommended to rely on the [`marrow`]-based functionality, as the features of [`marrow`]
//! are designed to be strictly additive.
//!
//! For example, to build a record batch, first build the corresponding marrow types and then use
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
#[cfg_attr(not(has_arrow), allow(unused))]
mod internal;

/// *Internal. Do not use*
///
/// This module is an internal implementation detail and not subject to any
/// compatibility promises. It re-exports the Arrow implementations selected via features
/// to allow usage in doc tests or benchmarks.
///
#[rustfmt::skip]
pub mod _impl {
    #[allow(unused, reason="there may be no arrow feature activated")]
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
    #[cfg(has_arrow_59)] build_arrow_crate!(arrow_array_59, arrow_schema_59);
    #[cfg(has_arrow_58)] build_arrow_crate!(arrow_array_58, arrow_schema_58);
    #[cfg(has_arrow_57)] build_arrow_crate!(arrow_array_57, arrow_schema_57);
    #[cfg(has_arrow_56)] build_arrow_crate!(arrow_array_56, arrow_schema_56);
    #[cfg(has_arrow_55)] build_arrow_crate!(arrow_array_55, arrow_schema_55);
    #[cfg(has_arrow_54)] build_arrow_crate!(arrow_array_54, arrow_schema_54);
    #[cfg(has_arrow_53)] build_arrow_crate!(arrow_array_53, arrow_schema_53);

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

pub use crate::internal::error::{Error, ErrorKind, Result};

pub use crate::internal::deserializer::Deserializer;
pub use crate::internal::serializer::Serializer;

pub use crate::internal::array_builder::ArrayBuilder;

#[cfg(has_arrow)]
mod arrow_impl;

#[cfg(has_arrow)]
pub use arrow_impl::{from_arrow, from_record_batch, to_arrow, to_record_batch};

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

/// Type mappings between Rust, Serde, and Arrow
///
/// `serde_arrow` bridges three distinct type systems: Rust types, the actual
/// types in your Rust code (`Vec<T>`, structs, enums, etc.);
/// [Serde data model][serde-model], the abstract representation Serde uses
/// during serialization; and [Arrow types][arrow-model], the columnar data
/// types defined by Apache Arrow. To convert between these type systems,
/// `serde_arrow` requires schema information as a list of Arrow fields with
/// additional metadata. See [`SchemaLike`][crate::schema::SchemaLike] for
/// details on how to specify the schema.
///
///
/// In most cases, `serde_arrow` expects data as a sequence of records:
///
/// ```rust
/// # struct Record { f0: i32, f1: i32 }
/// # let (v0, v1, v2, v3) = (0_i32, 1_i32, 2_i32, 3_i32);
/// vec![
///     Record { f0: v0, f1: v1 },
///     Record { f0: v2, f1: v3 },
///     // ..
/// ]
/// # ;
/// ```
///
/// The outer container must be one of these [Serde data types][serde-model]:
///
/// | Serde data type | Example Rust types | Comment |
/// |---|---|---|
/// |`seq` | [`Vec<T>`][std::vec::Vec], `&[T]` | variable-sized sequences |
/// | `tuple`, `tuple_struct`, `tuple_variant` |  `(T0, T1)`, `[T; N]`, `struct S(T0, T1)` | fixed-size sequences|
/// | `newtype_struct`, `newtype_variant` | `struct S(T)`, `enum E { V(T) }` | wrappers around the preceding types |
///
/// Each record must be one of these Serde data types:
///
/// | Serde data type | Example Rust types | Comment |
/// |---|---|---|
/// | `struct`, `struct_variant` | `struct S { f0: T0, f1: T1 }` | named fields |
/// | `map` | [`HashMap<K, V>`][std::collections::HashMap], [`BTreeMap<K, V>`][std::collections::BTreeMap] | key-value pairs |
/// | `seq`, `tuple`, `tuple_struct`, `tuple_variant` | `(T0, T1)`, `[T; N]` |  ordered fields |
/// | `newtype_struct`, `newtype_variant` | `struct S(T)` | wrappers around the preceding types |
///
/// Schema fields and struct fields do not have to be specified in the same
/// order, but matching order improves lookup performance. Missing schema
/// fields are serialized as null. Extra struct fields are ignored. Maps follow
/// the same semantics.
///
/// The following table shows how [Serde data types][serde-model], Rust types,
/// and [Arrow types][arrow-model] map to each other:
///
///
/// | Serde data type | Example Rust types | Default Arrow type |
/// |------------------|-------------------|------------|
/// | `unit` | `()` | `Null` |
/// | `bool` | `bool` | `Boolean` |
/// | `i8`, `i16`, `i32`, `i64` | `i8`, `i16`, `i32`, `i64` | `Int8`, `Int16`, `Int32`, `Int64` |
/// | `u8`, `u16`, `u32`, `u64` | `u8`, `u16`, `u32`, `u64` | `UInt8`, `UInt16`, `UInt32`, `UInt64` |
/// | `char` | `char` | `UInt32` |
/// | `bytes` | | `LargeBinary` |
/// | `f32`, `f64` | `f32`, `f64` | `Float32`, `Float64` |
/// | `str` | `str`, `String`, `&str` | `LargeUtf8` |
/// | `seq` | `Vec<T>`, `&[T]` | `LargeList` |
/// | `struct`, `tuple`, `tuple_struct` | `struct S { .. }`, `(T0, T1)` | `Struct` |
/// | `map` | [`HashMap<K, V>`][std::collections::HashMap], [`BTreeMap<K, V>`][std::collections::BTreeMap] | `Map` |
/// | `unit_variant`, `struct_variant`, `tuple_variant`, `newtype_variant` | `enum E { .. }` | Dense `Union` |
///
///
/// Enums are mapped to dense Arrow `Union` types, with each variant becoming a separate field:
///
/// - Unit variants (`V`) map to the `Null` Arrow type, but can also be serialized as arrow string types
/// - Newtype variants (`V(T)`) map to the inner type `T`
/// - Tuple variants or struct variants (`V(T0, T1)`, `V { f0: T0 }`) map to the Arrow `Struct` type
///
/// [serde-model]: https://serde.rs/data-model.html
/// [arrow-model]: https://arrow.apache.org/docs/format/Columnar.html
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
