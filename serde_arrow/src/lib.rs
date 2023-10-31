//! # `serde_arrow` - convert sequences Rust objects to / from arrow arrays
//!
//! The arrow in-memory format is a powerful way to work with data frame like
//! structures. However, the API of the underlying Rust crates can be at times
//! cumbersome to use due to the statically typed nature of Rust. `serde_arrow`,
//! offers a simple way to convert Rust objects into Arrow arrays and back.
//! `serde_arrow` relies on the [Serde](https://serde.rs) package to interpret
//! Rust objects. Therefore, adding support for `serde_arrow` to custom types is
//! as easy as using Serde's derive macros.
//!
//! In the Rust ecosystem there are two competing implementations of the arrow
//! in-memory format, [`arrow`][] and [`arrow2`][]. `serde_arrow` supports both.
//! The supported arrow implementations can be selected via
//! [features](#features).
//!
//! `serde_arrow` relies on a schema to translate between Rust and Arrow as
//! their type system are not directly translatable. The schema is expressed as
//! a collection of Arrow fields with additional metadata describing the arrays.
//! E.g., to convert Rust strings containing timestamps to Date64 arrays, the
//! schema should contain a  `Date64`. `serde_arrow` supports to derive the
//! schema from the data itself via schema tracing, but does not require it. It
//! is always possible to specify the schema manually. See the [`schema`
//! module][schema] and [SerdeArrowSchema][schema::SerdeArrowSchema]
//! for further details.
//!
//! ## Overview
//!
//! | Operation        | `arrow` |  `arrow2` |
//! |------------------|------------------|-------------------|
//! | Required features | [`arrow-*`](#features) | [`arrow2-*`](#features) |
//! | | | |
//! | Rust to Arrow    | [`to_arrow`]     | [`to_arrow2`]     |
//! | Arrow to Rust    | [`from_arrow`]   | [`from_arrow2`]   |
//! | Arrow Builder    | [`ArrowBuilder`] | [`Arrow2Builder`] |
//! | | | |
//! | Fields to Schema |  [`SerdeArrowSchema::from_arrow_fields`][schema::SerdeArrowSchema::from_arrow_fields] | [`SerdeArrowSchema::form_arrow2_fields`][schema::SerdeArrowSchema::from_arrow2_fields]  |
//! | Schema to fields | [`schema.to_arrow_fields()`][schema::SerdeArrowSchema::to_arrow_fields] | [`schema.to_arrow2_fields()`][schema::SerdeArrowSchema::to_arrow2_fields] |
//!
//! ## Example
//!
//! Requires one of `arrow2` feature (see below).
//!
//! ```rust
//! # use serde::{Deserialize, Serialize};
//! # #[cfg(feature = "has_arrow2")]
//! # fn main() -> serde_arrow::Result<()> {
//! use serde_arrow::schema::{TracingOptions, SerdeArrowSchema};
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
//! let fields =
//!     SerdeArrowSchema::from_type::<Record>(TracingOptions::default())?
//!     .to_arrow2_fields()?;
//!
//! let arrays = serde_arrow::to_arrow2(&fields, &records)?;
//! #
//! # drop(arrays);
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "has_arrow2"))]
//! # fn main() { }
//! ```
//!
//! The generated arrays can then be written to disk, e.g., as parquet:
//!
//! ```rust,ignore
//! use arrow2::{chunk::Chunk, datatypes::Schema};
//!
//! // see https://jorgecarleitao.github.io/arrow2/io/parquet_write.html
//! write_chunk(
//!     "example.pq",
//!     Schema::from(fields),
//!     Chunk::new(arrays),
//! )?;
//! ```
//!
//! See also:
//!
//! - the [quickstart guide][_impl::docs::quickstart] for more examples of how
//!   to use this package
//! - the [implementation notes][_impl::docs::implementation] for an explanation
//!   of how this package works and its underlying data model
//! - the [status summary][_impl::docs::status] for an overview over the
//!   supported Arrow and Rust constructs
//!
//! # Features:
//!
//! Which version of `arrow` or `arrow2` is used can be selected via features.
//! Per default no arrow implementation is used. In that case only the base
//! features of `serde_arrow` are available.
//!
//! The `arrow-*` and `arrow2-*` feature groups are compatible with each other.
//! I.e., it is possible to use `arrow` and `arrow2` together. Within each group
//! the highest version is selected, if multiple features are activated. E.g,
//! when selecting  `arrow2-0-16` and `arrow2-0-17`, `arrow2=0.17` will be used.
//!
//! Available features:
//!
//! | Feature       | Arrow Version |
//! |---------------|---------------|
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
    macro_rules! build_arrow2_crate {
        ($arrow2:ident) => {
            /// Re-export the used arrow2 crate
            pub use $arrow2 as arrow2;
        };
    }

    #[cfg(has_arrow2_0_17)] build_arrow2_crate!(arrow2_0_17);
    #[cfg(has_arrow2_0_16)] build_arrow2_crate!(arrow2_0_16);

    #[allow(unused)]
    macro_rules! build_arrow_crate {
        ($arrow_array:ident, $arrow_buffer:ident, $arrow_data:ident, $arrow_schema:ident) => {
            /// A "fake" arrow crate re-exporting the relevant definitions of the
            /// used arrow-* subcrates
            pub mod arrow {
                /// The raw arrow packages
                pub mod _raw {
                    pub use $arrow_array as array;
                    pub use $arrow_buffer as buffer;
                    pub use $arrow_data as data;
                    pub use $arrow_schema as schema;
                }
                pub mod array {
                    pub use $arrow_array::array::{
                        make_array, Array, ArrayRef, ArrowPrimitiveType, BooleanArray,
                        DictionaryArray, GenericListArray, LargeStringArray, MapArray, NullArray,
                        OffsetSizeTrait, PrimitiveArray, StringArray, StructArray, UnionArray,
                    };
                    pub use $arrow_data::ArrayData;
                }
                pub mod buffer {
                    pub use $arrow_buffer::buffer::{Buffer, ScalarBuffer};
                }
                pub mod datatypes {
                    pub use $arrow_array::types::{
                        ArrowPrimitiveType, Date64Type, Float16Type, Float32Type, Float64Type,
                        Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
                        UInt64Type, UInt8Type,
                    };
                    pub use $arrow_buffer::ArrowNativeType;
                    pub use $arrow_schema::{DataType, Field, TimeUnit, UnionMode};
                }
                pub mod error {
                    pub use $arrow_schema::ArrowError;
                }
            }
        };
    }

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
        #[doc = include_str!("../Implementation.md")]
        #[cfg(not(doctest))]
        pub mod implementation {}

        #[doc = include_str!("../Quickstart.md")]
        #[cfg(not(doctest))]
        pub mod quickstart {}

        #[doc = include_str!("../Status.md")]
        #[cfg(not(doctest))]
        pub mod status {}
    }

    // Reexport for tests
    pub use crate::internal::error::PanicOnError;
}

#[cfg(all(test, has_arrow, has_arrow2))]
mod test_impls;

#[cfg(all(test, has_arrow, has_arrow2))]
mod test_end_to_end;

#[cfg(test)]
mod test;

pub use crate::internal::error::{Error, Result};

#[cfg(has_arrow)]
mod arrow_impl;

#[cfg(has_arrow)]
pub use arrow_impl::api::{from_arrow, to_arrow, ArrowBuilder};

#[cfg(has_arrow)]
#[deprecated = "The items in serde_arrow::arrow are deprecated. See the individual items for suitable replacements"]
pub mod arrow {
    #[allow(deprecated)]
    pub use crate::arrow_impl::api::{
        deserialize_from_array, deserialize_from_arrays, serialize_into_array,
        serialize_into_arrays, serialize_into_field, serialize_into_fields, ArrayBuilder,
    };

    /// Renamed to [`serde_arrow::ArrowBuilder`][crate::ArrowBuilder]
    #[deprecated = "serde_arrow::arrow2::ArraysBuilder is deprecated. Use serde_arrow::Arrow2Builder instead"]
    pub type ArraysBuilder = crate::arrow_impl::api::ArrowBuilder;
}

#[cfg(has_arrow2)]
mod arrow2_impl;

#[cfg(has_arrow2)]
pub use arrow2_impl::api::{from_arrow2, to_arrow2, Arrow2Builder};

#[cfg(has_arrow2)]
#[deprecated = "The items in serde_arrow::arrow2 are deprecated. See the individual items for suitable replacements"]
pub mod arrow2 {
    #[allow(deprecated)]
    pub use crate::arrow2_impl::api::{
        deserialize_from_array, deserialize_from_arrays, serialize_into_array,
        serialize_into_arrays, serialize_into_field, serialize_into_fields, ArrayBuilder,
    };

    /// Renamed to [`serde_arrow::Arrow2Builder`][crate::Arrow2Builder]
    #[deprecated = "serde_arrow::arrow2::ArraysBuilder is deprecated. Use serde_arrow::Arrow2Builder instead"]
    pub type ArraysBuilder = crate::arrow2_impl::api::Arrow2Builder;
}

#[deny(missing_docs)]
pub mod schema;

#[deny(missing_docs)]
pub mod utils;
