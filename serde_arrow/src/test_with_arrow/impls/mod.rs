//! Tests for serialization
mod utils;

// tests grouped by arrow type
mod arrow_binary;
#[cfg(has_arrow_bytes_view_support)]
mod arrow_binary_view;
mod arrow_date;
mod arrow_decimal;
mod arrow_dictionary;
#[cfg(has_arrow_fixed_binary_support)]
mod arrow_fixed_size_binary;
mod arrow_fixed_size_list;
mod arrow_list;
mod arrow_map;
mod arrow_struct;
mod arrow_time;
mod arrow_timestamp;
mod arrow_union;
#[cfg(has_arrow_bytes_view_support)]
mod arrow_utf8_view;

// tests grouped by serde type
mod serde_i32;
mod serde_i64;

// tests for third party libs
mod third_party_big_decimal;
mod third_party_chrono;
mod third_party_jiff;
mod third_party_rust_decimal;
mod third_party_serde_json;
mod third_party_uuid;

// unsorted tests
mod bool8;
mod examples;
mod primitives;
mod tuple;
mod wrappers;

mod issue_74_unknown_fields;
mod issue_79_declared_but_missing_fields;
mod issue_90_type_tracing;

mod issue_264_enum_dummy_values;
