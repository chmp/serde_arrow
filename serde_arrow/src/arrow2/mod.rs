//! Support for the `arrow2` crate (requires one the `arrow2-*` features)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
#![deny(missing_docs)]
pub(crate) mod display;
pub(crate) mod schema;
pub(crate) mod sinks;
pub(crate) mod sources;
mod type_support;

#[cfg(test)]
mod test;

use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow2::{array::Array, datatypes::Field},
    internal::{
        self,
        error::Result,
        schema::{GenericField, TracingOptions},
        source::{deserialize_from_source, AddOuterSequenceSource},
    },
};

use self::{
    sinks::Arrow2PrimitiveBuilders,
    sources::{build_dynamic_source, build_record_source},
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
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// #
/// use arrow2::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::arrow2::serialize_into_fields;
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

/// Build arrays from the given items
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs).
///
/// To build arrays record by record use [ArraysBuilder].
///
/// ```rust
/// use serde::Serialize;
/// use serde_arrow::arrow2::{serialize_into_fields, serialize_into_arrays};
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
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;
    internal::serialize_into_arrays::<T, Arrow2PrimitiveBuilders>(&fields, items)
}

/// Deserialize a type from the given arrays
///
/// The type should be a list of records (e.g., a vector of structs).
///
/// ```rust
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::{
///     arrow2::{
///         deserialize_from_arrays,
///         serialize_into_arrays,
///         serialize_into_fields,
///     },
///     schema::TracingOptions,
/// };
///
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// // provide an example record to get the field information
/// let fields = serialize_into_fields(
///     &[Record { a: Some(1.0), b: 2}],
///     TracingOptions::default(),
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
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// use arrow2::datatypes::{DataType, Field};
/// use serde_arrow::arrow2::serialize_into_field;
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

/// Serialize a sequence of objects representing a single array into an array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// #
/// use arrow2::datatypes::{DataType, Field};
/// use serde_arrow::arrow2::serialize_into_array;
///
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
    let field: GenericField = field.try_into()?;
    internal::serialize_into_array::<T, Arrow2PrimitiveBuilders>(&field, items)
}

/// Deserialize a sequence of objects from a single array
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// #
/// use arrow2::{array::Array, datatypes::{DataType, Field}};
/// use serde_arrow::arrow2::{
///   serialize_into_array,
///   deserialize_from_array,
/// };
///
/// let field = Field::new("floats", DataType::Float32, false);
///
/// let array = serialize_into_array(&field,  &vec![1.0_f32, 2.0, 3.0]).unwrap();
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

/// Build a single array item by item
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// use arrow2::datatypes::{Field, DataType};
/// use serde_arrow::arrow2::ArrayBuilder;
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
pub struct ArrayBuilder {
    inner: internal::GenericArrayBuilder<Arrow2PrimitiveBuilders>,
}

impl ArrayBuilder {
    /// Construct a new build for the given field
    ///
    /// This method may fail for an unsupported data type of the given field.
    ///
    pub fn new(field: &Field) -> Result<Self> {
        Ok(Self {
            inner: internal::GenericArrayBuilder::new(GenericField::try_from(field)?)?,
        })
    }

    /// Add a single item to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.inner.push(item)
    }

    /// Add multiple items to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.inner.extend(items)
    }

    /// Build the array from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_array(&mut self) -> Result<Box<dyn Array>> {
        self.inner.build_array()
    }
}

/// Build arrays record by record
///
/// Example:
///
/// ```rust
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// use arrow2::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::arrow2::{ArraysBuilder};
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
pub struct ArraysBuilder {
    inner: internal::GenericArraysBuilder<Arrow2PrimitiveBuilders>,
}

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
        Ok(Self {
            inner: internal::GenericArraysBuilder::new(fields)?,
        })
    }

    /// Add a single record to the arrays
    ///
    pub fn push<T: Serialize + ?Sized>(&mut self, item: &T) -> Result<()> {
        self.inner.push(item)
    }

    /// Add multiple records to the arrays
    ///
    pub fn extend<T: Serialize + ?Sized>(&mut self, items: &T) -> Result<()> {
        self.inner.extend(items)
    }

    /// Build the arrays from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        self.inner.build_arrays()
    }
}

/// Experimental functionality that is not subject to semver compatibility
pub mod experimental {
    pub use super::schema::find_field_mut;
}
