//! # `serde_arrow` - convert sequences Rust objects to / from arrow arrays
//!
//! The arrow in-memory format is a powerful way to work with data frame like
//! structures. However, the API of the underlying Rust crates can be at times
//! cumbersome to use due to the statically typed nature of Rust.
//!
//! `serde_arrow`, offers a simple way to convert Rust objects into Arrow arrays
//! and back. `serde_arrow` relies on the [Serde](https://serde.rs) package to
//! interpret Rust objects. Therefore, adding support for `serde_arrow` to
//! custom types is as easy as using Serde's derive macros.
//!
//! [polars]: https://github.com/pola-rs/polars
//! [datafusion]: https://github.com/apache/arrow-datafusion/
//!
//! In the Rust ecosystem there are two competing implemenetations of the arrow
//! in-memory format. `serde_arrow` supports both [`arrow`][arrow] and
//! [`arrow2`][arrow2] for schema tracing and serialization from Rust structs to
//! arrays. Deserialization from arrays to Rust structs is currently only
//! implemented for `arrow2`.
//!
//! ## Overview
//!
//! The functions come in pairs: some work on single  arrays, i.e., the series
//! of a data frames, some work on multiples arrays, i.e., data frames
//! themselves.
//!
//! | implementation | operation | mutliple arrays           |  single array            |
//! |---|---|---|---|
//! | **arrow** | schema tracing | [arrow::serialize_into_fields] | [arrow::serialize_into_field] |
//! | | Rust to Arrow | [arrow::serialize_into_arrays] | [arrow::serialize_into_array] |
//! | | Arrow to Rust | not supported | not supported |
//! | | Builder | [arrow::ArraysBuilder] | [arrow::ArrayBuilder] |
//! | | | | |
//! | **arrow2** | schema tracing | [arrow2::serialize_into_fields] | [arrow2::serialize_into_field] |
//! | | Rust to Arrow | [arrow2::serialize_into_arrays] | [arrow2::serialize_into_array] |
//! | | Arrow to Rust | [arrow2::deserialize_from_arrays] | [arrow2::deserialize_from_array] |
//! | | Builder | [arrow2::ArraysBuilder] | [arrow2::ArrayBuilder] |
//!
//! Functions working on multiple arrays expect sequences of records in Rust,
//! e.g., a vector of structs. Functions working on single arrays expect vectors
//! of arrays elements.
//!
//!
//! ## Example
//!
//! Requires one of `arrow2` feature (see below).
//!
//! ```rust
//! # use serde::Serialize;
//! # #[cfg(feature = "arrow2-0-17")]
//! # fn main() -> serde_arrow::Result<()> {
//! use serde_arrow::{
//!     schema::TracingOptions,
//!     arrow2::{serialize_into_fields, serialize_into_arrays}
//! };
//!
//! ##[derive(Serialize)]
//! struct Example {
//!     a: f32,
//!     b: i32,
//! }
//!
//! let records = vec![
//!     Example { a: 1.0, b: 1 },
//!     Example { a: 2.0, b: 2 },
//!     Example { a: 3.0, b: 3 },
//! ];
//!
//! // Auto-detect the arrow types. Result may need to be overwritten and
//! // customized, see serde_arrow::schema::Strategy for details.
//! let fields = serialize_into_fields(&records, TracingOptions::default())?;
//! let arrays = serialize_into_arrays(&fields, &records)?;
//!
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "arrow2-0-17"))]
//! # fn main() { }
//! ```
//!
//! The generated arrays can then be written to disk, e.g., as parquet, and
//! loaded in another system.
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
//! features of `serde_arrow` are availble.
//!
//! The `arrow-*` and `arrow2-*` feature groupss are comptaible with each other.
//! I.e., it is possible to use `arrow` and `arrow2` together. Within each group
//! the highest version is selected, if multiple features are activated. E.g,
//! when selecting  `arrow2-0-16` and `arrow2-0-17`, `arrow2=0.17` will be used.
//!
//! Available features:
//!
//! | Feature       | Arrow Version |
//! |---------------|---------------|
//! | `arrow-39`    | `arrow=39`    |
//! | `arrow-38`    | `arrow=38`    |
//! | `arrow-37`    | `arrow=37`    |
//! | `arrow-36`    | `arrow=36`    |
//! | `arrow2-0-17` | `arrow2=0.17` |
//! | `arrow2-0-16` | `arrow2=0.16` |
//!
mod internal;

/// Internal. Do not use
///
/// This module is an internal implementation detail and not subject to any
/// compatibility promises. It re-exports the  arrow impls selected via features
/// to allow usage in doc tests or benchmarks.
///
pub mod _impl {
    #[allow(unused)]
    macro_rules! build_arrow2_crate {
        ($arrow2:ident) => {
            /// Re-export the used arrow2 crate
            pub use $arrow2 as arrow2;
        };
    }

    #[cfg(feature = "arrow2-0-17")]
    build_arrow2_crate!(arrow2_0_17);

    #[cfg(all(feature = "arrow2-0-16", not(feature = "arrow2-0-17")))]
    build_arrow2_crate!(arrow2_0_16);

    #[allow(unused)]
    macro_rules! build_arrow_crate {
        ($arrow_array:ident, $arrow_buffer:ident, $arrow_data:ident, $arrow_schema:ident) => {
            /// A "fake" arrow crate re-exporting the relevant definitions of the
            /// used arrow-* subcrates
            pub mod arrow {
                pub mod array {
                    pub use $arrow_array::array::{
                        make_array, Array, ArrayRef, ArrowPrimitiveType, BooleanArray,
                        DictionaryArray, GenericListArray, LargeStringArray, MapArray, NullArray,
                        OffsetSizeTrait, PrimitiveArray, StringArray, StructArray,
                    };
                    pub use $arrow_array::builder::{
                        BooleanBufferBuilder, BooleanBuilder, GenericStringBuilder,
                        PrimitiveBuilder,
                    };
                    pub use $arrow_data::ArrayData;
                }
                pub mod buffer {
                    pub use $arrow_buffer::buffer::{Buffer, ScalarBuffer};
                }
                pub mod datatypes {
                    pub use $arrow_array::types::{
                        Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
                        Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
                    };
                    pub use $arrow_buffer::ArrowNativeType;
                    pub use $arrow_schema::{DataType, Field, UnionMode};

                    pub use $arrow_array::types::ArrowPrimitiveType;
                }
                pub mod ffi {
                    pub use $arrow_data::ffi::FFI_ArrowArray;
                }
                pub mod error {
                    pub use $arrow_schema::ArrowError;
                }
            }
        };
    }

    #[cfg(feature = "arrow-39")]
    build_arrow_crate!(
        arrow_array_39,
        arrow_buffer_39,
        arrow_data_39,
        arrow_schema_39
    );

    #[cfg(all(feature = "arrow-38", not(feature = "arrow-39")))]
    build_arrow_crate!(
        arrow_array_38,
        arrow_buffer_38,
        arrow_data_38,
        arrow_schema_38
    );

    #[cfg(all(
        feature = "arrow-37",
        not(feature = "arrow-38"),
        not(feature = "arrow-39"),
    ))]
    build_arrow_crate!(
        arrow_array_37,
        arrow_buffer_37,
        arrow_data_37,
        arrow_schema_37
    );

    #[cfg(all(
        feature = "arrow-36",
        not(feature = "arrow-37"),
        not(feature = "arrow-38"),
        not(feature = "arrow-39"),
    ))]
    build_arrow_crate!(
        arrow_array_36,
        arrow_buffer_36,
        arrow_data_36,
        arrow_schema_36
    );

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
}

#[cfg(any(feature = "arrow2-0-17", feature = "arrow2-0-16"))]
pub mod arrow2;

#[cfg(any(
    feature = "arrow-36",
    feature = "arrow-37",
    feature = "arrow-38",
    feature = "arrow-39",
))]
pub mod arrow;

#[cfg(all(
    test,
    any(
        feature = "arrow-36",
        feature = "arrow-37",
        feature = "arrow-38",
        feature = "arrow-39",
    ),
    any(feature = "arrow2-0-17", feature = "arrow2-0-16")
))]
mod test_impls;

#[cfg(test)]
mod test;

pub use crate::internal::error::{Error, Result};

/// The basic machinery powering `serde_arrow`
///
/// This module collects helpers to convert objects to events and back.
///
pub mod base {
    pub use crate::internal::{
        event::Event,
        sink::{accept_events, serialize_into_sink, EventSink},
        source::{deserialize_from_source, EventSource},
    };
}

/// Helpers to configure how Arrow and Rust types are translated into one
/// another
///
/// When tracing the schema using the `serialize_into_fields` methods, the
/// following defaults are used:
///
/// - Strings: `LargeUtf8`, i.e., i64 offsets
/// - Lists: `LargeList`, i.e., i64 offsets
/// - Strings with dictionary encoding: U32 keys and LargeUtf8 values
///   - Rationale: `polars` cannot handle 64 bit keys in its default
///     configuration
///
/// Null-only fields (e.g., fields of type `()` or fields with only `None`
/// entries) result in errors per default.
/// [`TracingOptions::allow_null_fields`][crate::internal::schema::TracingOptions::allow_null_fields]
/// allows to disable this behavior.
///
/// All customization of the types happens via the metadata of the fields
/// structs describing arrays. For example, to let `serde_arrow` handle date
/// time objects that are serialized to strings (chrono's default), use
///
/// ```rust
/// # #[cfg(feature="arrow2")]
/// # fn main() {
/// # use arrow2::datatypes::{DataType, Field};
/// # use serde_arrow::schema::{STRATEGY_KEY, Strategy};
/// # let mut field = Field::new("my_field", DataType::Null, false);
/// field.data_type = DataType::Date64;
/// field.metadata = Strategy::UtcStrAsDate64.into();
/// # }
/// # #[cfg(not(feature="arrow2"))]
/// # fn main() {}
/// ```
///
/// For arrow2, the experimental [find_field_mut][] function may be helpful to
/// modify nested schemas genreated by tracing.
///
/// [find_field_mut]: crate::arrow2::experimental::find_field_mut
///
pub mod schema {
    pub use crate::internal::schema::{Strategy, TracingOptions, STRATEGY_KEY};
}

/// Experimental functionality that is not bound by semver compatibility
///
pub mod experimental {
    pub use crate::internal::{configure, Configuration};
}
