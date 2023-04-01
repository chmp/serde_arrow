//! Support for the arrow crate (requires one the `arrow-*` features)
//!
mod schema;
mod sinks;
mod type_support;

#[cfg(test)]
mod test;

use serde::Serialize;

use crate::{
    impls::arrow::{
        array::{self, ArrayRef},
        schema::Field,
    },
    internal::{
        self,
        error::Result,
        schema::{GenericField, TracingOptions},
    },
};

use self::sinks::ArrowPrimitiveBuilders;

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
pub fn serialize_into_arrays<T>(fields: &[Field], items: &T) -> Result<Vec<ArrayRef>>
where
    T: Serialize + ?Sized,
{
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;
    let arrays = internal::serialize_into_arrays::<T, ArrowPrimitiveBuilders>(&fields, items)?;
    Ok(arrays.into_iter().map(array::make_array).collect())
}

/// Serialize an object that represents a single array into an array
///
pub fn serialize_into_array<T>(field: &Field, items: &T) -> Result<ArrayRef>
where
    T: Serialize + ?Sized,
{
    let field: GenericField = field.try_into()?;
    let data = internal::serialize_into_array::<T, ArrowPrimitiveBuilders>(&field, items)?;
    Ok(array::make_array(data))
}

/// Build arrays record by record
///
pub struct ArraysBuilder {
    inner: internal::ArraysBuilder<ArrowPrimitiveBuilders>,
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
            inner: internal::ArraysBuilder::new(fields)?,
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

    /// Build the arrays built from the rows pushed to far.
    ///
    /// This operation will reset the underlying buffers and start a new batch.
    ///
    pub fn build_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        let data = self.inner.build_arrays()?;
        Ok(data.into_iter().map(array::make_array).collect())
    }
}
