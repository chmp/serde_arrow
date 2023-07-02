//! Support for the `arrow` crate (requires one the `arrow-*` features)
//!
//! Functions to convert Rust objects into arrow Arrays. Deserialization from
//! `arrow` arrays to Rust objects is not yet supported.
//!
#![deny(missing_docs)]
mod deserialization;
mod schema;
pub(crate) mod serialization;
mod type_support;

use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::{Array, ArrayRef},
        datatypes::Field,
    },
    internal::{
        self,
        error::Result,
        schema::{GenericField, TracingOptions},
        serialization::{compile_serialization, CompilationOptions, Interpreter},
        sink::serialize_into_sink,
        source::deserialize_from_source,
    },
};

/// Determine the schema (as a list of fields) for the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// To correctly record the type information make sure to:
///
/// - include values for `Option<T>`
/// - include all variants of an enum
/// - include at least single element of a list or a map
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// #
/// use arrow::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::arrow::serialize_into_fields;
///
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
///
pub fn serialize_into_fields<T>(items: &T, options: TracingOptions) -> Result<Vec<Field>>
where
    T: Serialize + ?Sized,
{
    internal::serialize_into_fields(items, options)?
        .iter()
        .map(|f| f.try_into())
        .collect()
}

/// Determine the schema of an object that represents a single array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{DataType, Field};
/// use serde_arrow::arrow::serialize_into_field;
///
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
    let field = internal::serialize_into_field(items, name, options)?;
    (&field).try_into()
}

/// Build arrays from the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// Example:
///
/// ```rust
/// use serde::Serialize;
/// use serde_arrow::arrow::{serialize_into_fields, serialize_into_arrays};
///
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
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<ArrayRef>>
where
    T: Serialize + ?Sized,
{
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;

    let program = compile_serialization(&fields, CompilationOptions::default())?;
    let mut interpreter = Interpreter::new(program);
    serialize_into_sink(&mut interpreter, items)?;
    interpreter.build_arrow_arrays()
}

/// TODO: document
pub fn deserialize_from_arrays<'de, T, A>(fields: &'de [Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    use crate::internal::{
        common::{BufferExtract, Buffers},
        deserialization,
    };

    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;

    let num_items = arrays
        .iter()
        .map(|a| a.as_ref().len())
        .min()
        .unwrap_or_default();

    let mut buffers = Buffers::new();
    let mut mappings = Vec::new();
    for (field, array) in fields.iter().zip(arrays.iter()) {
        mappings.push(array.as_ref().extract_buffers(field, &mut buffers)?);
    }

    let interpreter = deserialization::compile_deserialization(
        num_items,
        &mappings,
        buffers,
        deserialization::CompilationOptions::default(),
    )?;
    deserialize_from_source(interpreter)
}

/// Serialize an object that represents a single array into an array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// #
/// use arrow::datatypes::{DataType, Field};
/// use serde_arrow::arrow::serialize_into_array;
///
/// let items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];
///
/// let field = Field::new("floats", DataType::Float32, false);
/// let array = serialize_into_array(&field, &items).unwrap();
///
/// assert_eq!(array.len(), 4);
/// ```
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<ArrayRef>
where
    T: Serialize + ?Sized,
{
    let field: GenericField = field.try_into()?;

    let program = compile_serialization(
        std::slice::from_ref(&field),
        CompilationOptions::default().wrap_with_struct(false),
    )?;
    let mut interpreter = Interpreter::new(program);
    serialize_into_sink(&mut interpreter, items)?;
    interpreter.build_arrow_array()
}

/// TODO: document
pub fn deserialize_from_array<'de, T, A>(field: &'de Field, array: &'de A) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array> + 'de + ?Sized,
{
    internal::deserialize_from_array(field, array.as_ref())
}

/// Build a single array item by item
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{Field, DataType};
/// use serde_arrow::arrow::ArrayBuilder;
///
/// let field = Field::new("value", DataType::Int64, false);
/// let mut builder = ArrayBuilder::new(&field).unwrap();
///
/// builder.push(&-1_i64).unwrap();
/// builder.push(&2_i64).unwrap();
/// builder.push(&-3_i64).unwrap();
///
/// builder.extend(&[4_i64, -5, 6]).unwrap();
///
/// let array = builder.build_array().unwrap();
/// assert_eq!(array.len(), 6);
/// ```
pub struct ArrayBuilder(internal::GenericBuilder);

impl ArrayBuilder {
    /// Construct a new build for the given field
    ///
    /// This method may fail for an unsupported data type of the given field.
    ///
    pub fn new(field: &Field) -> Result<Self> {
        Ok(Self(internal::GenericBuilder::new_for_array(
            GenericField::try_from(field)?,
        )?))
    }

    /// Add a single item to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.0.push(item)
    }

    /// Add multiple items to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.0.extend(items)
    }

    /// Build the array from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_array(&mut self) -> Result<ArrayRef> {
        self.0 .0.build_arrow_array()
    }
}

/// Build arrays record by record
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::arrow::{ArraysBuilder};
///
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
/// builder.push(&Record { a: Some(1.0), b: 2}).unwrap();
/// builder.push(&Record { a: Some(3.0), b: 4}).unwrap();
/// builder.push(&Record { a: Some(5.0), b: 5}).unwrap();
///
/// builder.extend(&[
///     Record { a: Some(6.0), b: 7},
///     Record { a: Some(8.0), b: 9},
///     Record { a: Some(10.0), b: 11},
/// ]).unwrap();
///
/// let arrays = builder.build_arrays().unwrap();
///
/// assert_eq!(arrays.len(), 2);
/// assert_eq!(arrays[0].len(), 6);
/// ```
pub struct ArraysBuilder(internal::GenericBuilder);

impl std::fmt::Debug for ArraysBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArraysBuilder<...>")
    }
}

impl ArraysBuilder {
    /// Build a new ArraysBuilder for the given fields
    ///
    /// This method may fail when unsupported data types are encountered in the
    /// given fields.
    ///
    pub fn new(fields: &[Field]) -> Result<Self> {
        let fields = fields
            .iter()
            .map(GenericField::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(internal::GenericBuilder::new_for_arrays(&fields)?))
    }

    /// Add a single record to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.0.push(item)
    }

    /// Add multiple records to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.0.extend(items)
    }

    /// Build the arrays from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        self.0 .0.build_arrow_arrays()
    }
}
