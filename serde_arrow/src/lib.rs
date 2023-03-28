//! # `serde_arrow` - convert sequences Rust objects to arrow2 arrays
//!
//! Usage (requires the `arrow2` feature):
//!
//! ```rust
//! # use serde::Serialize;
//! # #[cfg(feature = "arrow2")]
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
//! let batch = serialize_into_arrays(&fields, &records)?;
//!
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "arrow2"))]
//! # fn main() { }
//! ```
//!
//! See the [quickstart guide][docs::quickstart] for more examples of how to use
//! this package. See the [implementation notes][docs::implementation] for an
//! explanation of how this package works and its underlying data model.
//!
//! # Features:
//!
//! - `arrow2`: add support to (de)serialize to and from arrow2 arrays. This
//!   feature is activated per default
//!
//! # Status
//!
//! For an overview over the supported Arrow and Rust types see status section
//! in the [implementation notes][docs::implementation]
//!
pub mod impls;
mod internal;

#[cfg(any(feature = "arrow2-0-17", feature = "arrow2-0-16"))]
pub mod arrow2;

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
