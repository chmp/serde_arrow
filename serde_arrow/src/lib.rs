//! # `serde_arrow` - convert sequences Rust objects to arrow2 arrays
//!
//! Usage (requires one of `arrow2` feature, see below):
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
//! // customized, see serde_arrow::Strategy for details.
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
//! See the [quickstart guide][docs::quickstart] for more examples of how to use
//! this package. See the [implementation notes][docs::implementation] for an
//! explanation of how this package works and its underlying data model.
//!
//! # Features:
//!
//! Which version of `arrow` or `arrow2` is used can be selected via features.
//! Per default no arrow implementation is used. In that case only the base
//! features of `serde_arrow` are availble.
//!
//! The `arrow-*` and `arrow2-*` feature groupss are comptaible with each other.
//! I.e., it is possible to use `arrow` and `arrow2` togehter. Within each group
//! the highest version is selected, if multiple features are activated. E.g,
//! when selecting  `arrow2-0-16` and `arrow2-0-17`, `arrow2=0.17` will be used.
//!
//! Available features:
//!
//! | Feature       | Arrow Version |
//! |---------------|---------------|
//! | `arrow-36`    | `arrow=36`    |
//! | `arrow-35`    | `arrow=35`    |
//! | `arrow2-0-17` | `arrow2=0.17` |
//! | `arrow2-0-16` | `arrow2=0.16` |
//!
//! # Status
//!
//! For an overview over the supported Arrow and Rust types see status section
//! in the [implementation notes][docs::implementation]
//!
mod internal;

/// The arrow implementations used
pub mod impls {
    #[cfg(feature = "arrow2-0-17")]
    pub use arrow2_0_17 as arrow2;

    #[cfg(all(feature = "arrow2-0-16", not(feature = "arrow2-0-17")))]
    pub use arrow2_0_16 as arrow2;

    #[cfg(feature = "arrow-36")]
    pub mod arrow {
        pub use arrow_array_36 as array;
        pub use arrow_buffer_36 as buffer;
        pub use arrow_data_36 as data;
        pub use arrow_schema_36 as schema;
    }

    #[cfg(all(feature = "arrow-35", not(feature = "arrow-36")))]
    pub mod arrow {
        pub use arrow_array_35 as array;
        pub use arrow_buffer_35 as buffer;
        pub use arrow_data_35 as data;
        pub use arrow_schema_35 as schema;
    }
}

#[cfg(any(feature = "arrow2-0-17", feature = "arrow2-0-16"))]
pub mod arrow2;

#[cfg(any(feature = "arrow-36", feature = "arrow-35"))]
pub mod arrow;

#[cfg(test)]
mod test;

pub use internal::error::{Error, Result};

/// Common abstractions used in `serde_arrow`
///
/// The underlying abstraction is a stream of event objects, similar to the
/// tokens of [serde_test](https://docs.rs/serde_test/latest/).
///
pub mod base {
    pub use crate::internal::event::Event;
    pub use crate::internal::sink::{serialize_into_sink, EventSink};
    pub use crate::internal::source::{deserialize_from_source, EventSource};
}

/// Helpers to configure how Arrow and Rust types are translated into one
/// another
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
/// For arrow2, the experimental [find_field_mut][] function may
/// be helpful to modify nested schemas genreated by tracing.
///
/// [find_field_mut]: crate::arrow2::experimental::find_field_mut
///
pub mod schema {
    pub use crate::internal::schema::{Strategy, TracingOptions, STRATEGY_KEY};
}

/// Documentation only modules
pub mod docs {
    #[doc = include_str!("../Implementation.md")]
    #[cfg(not(doctest))]
    pub mod implementation {}

    #[doc = include_str!("../Quickstart.md")]
    #[cfg(not(doctest))]
    pub mod quickstart {}
}
