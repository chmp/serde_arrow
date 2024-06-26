#![deny(missing_docs)]
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::{Array, ArrayRef, RecordBatch},
        datatypes::FieldRef,
    },
    internal::{
        array_builder::ArrayBuilder, deserializer::Deserializer, error::Result,
        schema::SerdeArrowSchema, serializer::Serializer,
    },
};

/// Build arrow arrays from the given items  (*requires one of the `arrow-*`
/// features*)
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`ArrayBuilder`]. To construct a record
/// batch, consider using [`to_record_batch`].
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::FieldRef;
/// use serde::{Serialize, Deserialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Serialize, Deserialize)]
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
/// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
/// let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # Ok(())
/// # }
/// ```
///
pub fn to_arrow<T: Serialize + ?Sized>(fields: &[FieldRef], items: &T) -> Result<Vec<ArrayRef>> {
    let builder = ArrayBuilder::new(SerdeArrowSchema::try_from(fields)?)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .to_arrow()
}

/// Deserialize items from arrow arrays (*requires one of the `arrow-*`
/// features*)
///
/// The type should be a list of records (e.g., a vector of structs). To
/// deserialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::FieldRef;
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow_arrays();
/// #
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
/// let items: Vec<Record> = serde_arrow::from_arrow(&fields, &arrays)?;
/// # Ok(())
/// # }
/// ```
///
pub fn from_arrow<'de, T, A>(fields: &[FieldRef], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    T::deserialize(Deserializer::from_arrow(fields, arrays)?)
}

/// Build a record batch from the given items  (*requires one of the `arrow-*`
/// features*)
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`ArrayBuilder`].
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::FieldRef;
/// use serde::{Serialize, Deserialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Serialize, Deserialize)]
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
/// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
/// let record_batch = serde_arrow::to_record_batch(&fields, &items)?;
///
/// assert_eq!(record_batch.num_columns(), 2);
/// assert_eq!(record_batch.num_rows(), 1);
/// # Ok(())
/// # }
/// ```
pub fn to_record_batch<T: Serialize + ?Sized>(
    fields: &[FieldRef],
    items: &T,
) -> Result<RecordBatch> {
    let builder = ArrayBuilder::from_arrow(fields)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .to_record_batch()
}

/// Deserialize items from a record batch (*requires one of the `arrow-*`
/// features*)
///
/// The type should be a list of records (e.g., a vector of structs). To
/// deserialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # let record_batch = serde_arrow::_impl::docs::defs::example_record_batch();
/// #
/// use serde::Deserialize;
///
/// ##[derive(Deserialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let items: Vec<Record> = serde_arrow::from_record_batch(&record_batch)?;
/// # Ok(())
/// # }
/// ```
///
pub fn from_record_batch<'de, T: Deserialize<'de>>(record_batch: &'de RecordBatch) -> Result<T> {
    T::deserialize(Deserializer::from_record_batch(record_batch)?)
}
