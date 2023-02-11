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
    sinks::build_array_builder,
    sources::{build_dynamic_source, build_record_source},
    type_support::DataTypeDisplay,
};
use crate::{
    base::{
        deserialize_from_source, error::fail, serialize_into_sink, sink::StripOuterSequenceSink,
        source::AddOuterSequenceSource,
    },
    generic::{
        schema::{FieldBuilder, SchemaTracer, SchemaTracingOptions, Tracer},
        sinks::{ArrayBuilder, StructArrayBuilder},
    },
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
/// let fields = serialize_into_fields(&items, Default::default()).unwrap();
/// let expected = vec![
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ];
///
/// assert_eq!(fields, expected);
/// ```
/// To correctly record the type information make sure to:
///
/// - include values for `Option<T>`
/// - include all variants of an enum
/// - include at least single element of a list a map
///
pub fn serialize_into_fields<T>(items: &T, options: SchemaTracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let mut schema = SchemaTracer::new(options);
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
/// let fields = serialize_into_fields(&items, Default::default()).unwrap();
/// let arrays = serialize_into_arrays(&fields, &items).unwrap();
///
/// assert_eq!(arrays.len(), 2);
/// ```
///
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let mut columnes = Vec::new();
    let mut nullable = Vec::new();
    let mut builders = Vec::new();
    for field in fields {
        columnes.push(field.name.to_owned());
        nullable.push(field.is_nullable);
        builders.push(build_array_builder(field)?);
    }

    let mut builder = StructArrayBuilder::new(columnes, nullable, builders);
    serialize_into_sink(&mut StripOuterSequenceSink::new(&mut builder), items)?;

    builder.into_values()
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
/// let fields = serialize_into_fields(
///     &[Record { a: Some(1.0), b: 2}],
///     Default::default(),
/// ).unwrap();
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serialize_into_arrays(&fields, &items).unwrap();
/// #
///
/// // deserialize the records from arrays
/// let items: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
/// ```
///
pub fn deserialize_from_arrays<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    let source = build_record_source(fields, arrays)?;
    deserialize_from_source(source)
}

/// Determine the schema of an object that represents a single array
///
pub fn serialize_into_field<T>(
    items: &T,
    name: &str,
    options: SchemaTracingOptions,
) -> Result<Field>
where
    T: Serialize + ?Sized,
{
    let mut tracer = Tracer::new(options);
    serialize_into_sink(&mut StripOuterSequenceSink::new(&mut tracer), items)?;
    tracer.to_field(name)
}

/// Serialize an object that represents a single array into an array
///
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<Box<dyn Array>>
where
    T: Serialize + ?Sized,
{
    let mut builder = build_array_builder(field)?;
    serialize_into_sink(&mut StripOuterSequenceSink::new(&mut builder), items).unwrap();
    builder.into_array()
}

/// Deserialize an object that represents a single array from an array
///
pub fn deserialize_from_array<'de, T, A>(field: &Field, array: A) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array> + 'de,
{
    let source = build_dynamic_source(field, array.as_ref())?;
    let source = AddOuterSequenceSource::new(source);
    deserialize_from_source(source)
}
