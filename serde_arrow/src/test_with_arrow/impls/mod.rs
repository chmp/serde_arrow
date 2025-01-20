//! Tests for serialization
mod utils;

mod arrow_date;
mod arrow_time;
mod arrow_timestamp;

mod serde_i32;
mod serde_i64;

mod bool8;
mod bytes;
mod chrono;
mod dictionary;
mod examples;
mod fixed_size_list;
mod jiff;
mod json_values;
mod list;
mod map;
mod primitives;
mod r#struct;
mod tuple;
mod r#union;
mod wrappers;

mod issue_203_uuid;
mod issue_59_decimals;
mod issue_74_unknown_fields;
mod issue_79_declared_but_missing_fields;
mod issue_90_type_tracing;
