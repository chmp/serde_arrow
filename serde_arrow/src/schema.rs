//! Configure how Arrow and Rust types are translated into one another
//!
//! When tracing the schema using the `serialize_into_fields` methods, the
//! following defaults are used:
//!
//! - Strings: `LargeUtf8`, i.e., i64 offsets
//! - Lists: `LargeList`, i.e., i64 offsets
//! - Strings with dictionary encoding: U32 keys and LargeUtf8 values
//!   - Rationale: `polars` cannot handle 64 bit keys in its default
//!     configuration
//!
//! Null-only fields (e.g., fields of type `()` or fields with only `None`
//! entries) result in errors per default.
//! [`TracingOptions::allow_null_fields`][crate::internal::tracing::TracingOptions::allow_null_fields]
//! allows to disable this behavior.
//!
//! All customization of the types happens via the metadata of the fields
//! structs describing arrays. For example, to let `serde_arrow` handle date
//! time objects that are serialized to strings (chrono's default), use
//!
//! ```rust
//! # #[cfg(feature="arrow2")]
//! # fn main() {
//! # use arrow2::datatypes::{DataType, Field};
//! # use serde_arrow::schema::{STRATEGY_KEY, Strategy};
//! # let mut field = Field::new("my_field", DataType::Null, false);
//! field.data_type = DataType::Date64;
//! field.metadata = Strategy::UtcStrAsDate64.into();
//! # }
//! # #[cfg(not(feature="arrow2"))]
//! # fn main() {}
//! ```
pub use crate::internal::{
    schema::{SerdeArrowSchema, Strategy, STRATEGY_KEY},
    tracing::TracingOptions,
};

/// Type alias for SerdeArrowSchema for backwards compatibility
#[deprecated = "serde_arrow::schema::Schema is deprecated. Use serde_arrow::schema::SerdeArrowSchema instead"]
pub type Schema = SerdeArrowSchema;
