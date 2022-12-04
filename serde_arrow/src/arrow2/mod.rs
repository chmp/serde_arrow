//! arrow2 dependent functionality (requires the `arrow2` feature)
//!
pub(crate) mod schema;
pub(crate) mod sinks;
pub(crate) mod sources;
mod type_support;

use arrow2::{
    array::Array,
    datatypes::{DataType, Field},
};
use serde::{Deserialize, Serialize};

use self::{
    sinks::build_arrays_builder, sources::build_record_source, type_support::DataTypeDisplay,
};
use crate::{
    base::{deserialize_from_source, error::fail, serialize_into_sink},
    generic::schema::{FieldBuilder, SchemaTracer},
    Result,
};

/// Determine the schema (as a list of fields) for the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// ```rust
/// # use arrow2::datatypes::{Field, DataType};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::serialize_into_fields;
/// #
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let items = vec![
///     Record { a: Some(1.0), b: 2},
///     // ...
/// ];
///
/// let fields = serialize_into_fields(&items).unwrap();
/// let expected = vec![
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ];
///
/// assert_eq!(fields, expected);
/// ```
/// To correctly the type information make sure to:
///
/// - include values for `Option<T>`
/// - include all variants of an enum
/// - include at least single element of a list a map
///
pub fn serialize_into_fields<T>(items: &T) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut schema = SchemaTracer::new();
    serialize_into_sink(&mut schema, items)?;

    let root = schema.to_field("root")?;
    match root.data_type {
        DataType::Struct(fields) => Ok(fields),
        DataType::Null => fail!("No records found to determine schema"),
        dt => fail!("Unexpected root data type {}", DataTypeDisplay(&dt)),
    }
}

/// Build arrays from the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// ```rust
/// # use arrow2::datatypes::{Field, DataType};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::{serialize_into_fields, serialize_into_arrays};
/// #
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let items = vec![
///     Record { a: Some(1.0), b: 2},
///     // ...
/// ];
///
/// let fields = serialize_into_fields(&items).unwrap();
/// let arrays = serialize_into_arrays(&fields, &items).unwrap();
///
/// assert_eq!(arrays.len(), 2);
/// ```
///
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let mut builder = build_arrays_builder(fields)?;
    serialize_into_sink(&mut builder, items)?;
    builder.into_records()
}

/// Deserialize a type from the given arrays
///
/// The type should be a list of records (e.g., a vector of structs).
///
/// ```rust
/// # use arrow2::datatypes::{Field, DataType};
/// # use serde::{Serialize, Deserialize};
/// # use serde_arrow::arrow2::{
/// #   serialize_into_fields,
/// #   serialize_into_arrays,
/// #   deserialize_from_arrays,
/// # };
/// #
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// // provide an example record to get the field information
/// let fields = serialize_into_fields(&[Record { a: Some(1.0), b: 2}]).unwrap();
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serialize_into_arrays(&fields, &items).unwrap();
/// #
///
/// // deserialize the records from arrays
/// let items: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
/// ```
///
pub fn deserialize_from_arrays<'de, T, A>(fields: &[Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    deserialize_from_source(build_record_source(fields, arrays)?)
}
