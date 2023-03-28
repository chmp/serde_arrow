//! arrow2 dependent functionality (requires the `arrow2` feature)
//!
//! Functions to convert `arrow2  arrays from and into Rust objects.
//!
//! The functions come in pairs: some work on single  arrays, i.e., the series
//! of a data frames, some work on multiples arrays, i.e., data frames
//! themselves.
//!
//! | operation | mutliple arrays |  single array  |
//! |---|-----------------|----------------|
//! | schema tracing | [serialize_into_fields] | [serialize_into_field] |
//! | Rust to arrow2 | [serialize_into_arrays] | [serialize_into_array] |
//! | arrow2 to Rust | [deserialize_from_arrays] | [deserialize_from_array] |
//!
//! Functions working on multiple arrays expect sequences of records in Rust,
//! e.g., a vector of structs. Functions working on single arrays expect vectors
//! of arrays elements.
//!
pub(crate) mod display;
pub(crate) mod schema;
pub(crate) mod sinks;
pub(crate) mod sources;
mod type_support;

#[cfg(test)]
mod test;

use crate::impls::arrow2::{
    array::Array,
    datatypes::{DataType, Field},
};
use serde::{Deserialize, Serialize};

use self::{
    sinks::{build_array_builder, build_struct_array_builder_from_fields},
    sources::{build_dynamic_source, build_record_source},
};
use crate::internal::{
    error::{fail, Result},
    generic_sinks::StructArrayBuilder,
    schema::{FieldBuilder, Tracer, TracingOptions},
    sink::{
        serialize_into_sink, ArrayBuilder, DynamicArrayBuilder, EventSerializer, EventSink,
        StripOuterSequenceSink,
    },
    source::{deserialize_from_source, AddOuterSequenceSource},
};

/// Determine the schema (as a list of fields) for the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// ```rust
/// # use serde_arrow::impls::arrow2::datatypes::{DataType, Field};
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
/// - include at least single element of a list or a map
///
pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    let tracer = Tracer::new(options);
    let mut tracer = StripOuterSequenceSink::new(tracer);
    serialize_into_sink(&mut tracer, items)?;
    let root = tracer.into_inner().to_field("root")?;

    match root.data_type {
        DataType::Struct(fields) => Ok(fields),
        DataType::Null => fail!("No records found to determine schema"),
        dt => fail!("Unexpected root data type {}", display::DataType(&dt)),
    }
}

/// Build arrays from the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// To build arrays record by record use [ArraysBuilder].
///
/// ```rust
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
    let builder = build_struct_array_builder_from_fields(fields)?;
    let mut builder = StripOuterSequenceSink::new(builder);
    serialize_into_sink(&mut builder, items)?;

    builder.into_inner().build_arrays()
}

/// Deserialize a type from the given arrays
///
/// The type should be a list of records (e.g., a vector of structs).
///
/// ```rust
/// # use serde_arrow::impls::arrow2::datatypes::{Field, DataType};
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
/// Example:
///
/// ```rust
/// # use serde_arrow::impls::arrow2::{array::Array, datatypes::{DataType, Field}};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::{serialize_into_field, serialize_into_array};
/// #
/// let items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
///
/// let field = serialize_into_field(&items, "floats", Default::default()).unwrap();
/// assert_eq!(field, Field::new("floats", DataType::Float32, false));
/// ```
///
pub fn serialize_into_field<T>(items: &T, name: &str, options: TracingOptions) -> Result<Field>
where
    T: Serialize + ?Sized,
{
    let tracer = Tracer::new(options);
    let mut tracer = StripOuterSequenceSink::new(tracer);
    serialize_into_sink(&mut tracer, items)?;
    tracer.into_inner().to_field(name)
}

/// Serialize an object that represents a single array into an array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::impls::arrow2::{array::Array, datatypes::{DataType, Field}};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::{serialize_into_field, serialize_into_array};
/// #
/// let items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
///
/// let field = Field::new("floats", DataType::Float32, false);
/// let array = serialize_into_array(&field, &items).unwrap();
///
/// assert_eq!(array.len(), 4);
/// ```
///
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<Box<dyn Array>>
where
    T: Serialize + ?Sized,
{
    let builder = build_array_builder(field)?;
    let mut builder = StripOuterSequenceSink::new(builder);
    serialize_into_sink(&mut builder, items).unwrap();
    builder.into_inner().build_array()
}

/// Deserialize an object that represents a single array from an array
///
/// /// Determine the schema of an object that represents a single array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::impls::arrow2::{array::Array, datatypes::{DataType, Field}};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::{
/// #   serialize_into_field,
/// #   serialize_into_array,
/// #   deserialize_from_array,
/// # };
/// let field = Field::new("floats", DataType::Float32, false);
/// # let base_items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
/// # let array = serialize_into_array(&field, &base_items).unwrap();
/// let items: Vec<f32> = deserialize_from_array(&field, &array).unwrap();
/// ```
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

/// Build arrays record by record
///
/// Usage:
///
/// ```rust
/// # use serde_arrow::impls::arrow2::datatypes::{DataType, Field};
/// # use serde::Serialize;
/// # use serde_arrow::arrow2::{ArraysBuilder};
/// #
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }

/// let fields = vec![
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ];
/// let mut builder = ArraysBuilder::new(&fields).unwrap();
///
/// for item in &[
///     Record { a: Some(1.0), b: 2},
///     Record { a: Some(3.0), b: 4},
///     Record { a: Some(5.0), b: 5},
///     // ...
/// ] {
///     builder.push(item).unwrap()
/// }
///  
/// let arrays = builder.build_arrays().unwrap();
/// assert_eq!(arrays.len(), 2);
/// ```
pub struct ArraysBuilder {
    fields: Vec<Field>,
    builder: StructArrayBuilder<DynamicArrayBuilder<Box<dyn Array>>>,
}

impl std::fmt::Debug for ArraysBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArraysBuilder{{ fields: {:?} }}", self.fields)
    }
}

impl ArraysBuilder {
    /// Build a new ArraysBuilder for the given fields
    ///
    /// This method may fail when unsupported data types are encountered in the
    /// given fields.
    ///
    pub fn new(fields: &[Field]) -> Result<Self> {
        let fields = fields.to_vec();
        let builder = build_struct_array_builder_from_fields(&fields)?;

        Ok(Self { fields, builder })
    }

    /// Add a single record to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        item.serialize(EventSerializer(&mut self.builder))?;
        Ok(())
    }

    /// Add multiple records to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        let mut builder = StripOuterSequenceSink::new(&mut self.builder);
        items.serialize(EventSerializer(&mut builder))?;
        Ok(())
    }

    /// Build the arrays built from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        let mut builder = build_struct_array_builder_from_fields(&self.fields)?;
        std::mem::swap(&mut builder, &mut self.builder);

        builder.finish()?;
        builder.build_arrays()
    }
}

/// Experimental functionality that is not subject to semver compatibility
pub mod experimental {
    use crate::impls::arrow2::datatypes::Field;

    use super::display;

    /// Format the fields as a string
    ///
    /// The fields are displayed as Rust code that allows to build the fields in
    /// code. The following symbols of the `arrow2::datatypes `module are
    /// assumed to be in scope `DataType`, `Field`, `Metadata`.
    ///
    pub fn format_fields(fields: &[Field]) -> String {
        display::Fields(fields).to_string()
    }

    pub use super::schema::find_field_mut;
}
