#![deny(missing_docs)]
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::{Array, ArrayRef},
        datatypes::Field,
    },
    internal::{
        common::Mut,
        error::Result,
        schema::{GenericField, SerdeArrowSchema},
        serialization::OuterSequenceBuilder,
    },
};

use super::deserialization::build_deserializer;

/// Build arrow arrays record by record (*requires one of the `arrow-*`
/// features*)
///
/// The given items should be records (e.g., structs). To serialize items
/// encoding single values consider the [`Items`][crate::utils::Items] and
/// [`Item`][crate::utils::Item] wrappers.
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow as arrow;
/// use arrow::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::ArrowBuilder;
///
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let mut builder = ArrowBuilder::new(&[
///     Field::new("a", DataType::Float32, true),
///     Field::new("b", DataType::UInt64, false),
/// ])?;
///
/// builder.push(&Record { a: Some(1.0), b: 2})?;
/// builder.push(&Record { a: Some(3.0), b: 4})?;
/// builder.push(&Record { a: Some(5.0), b: 5})?;
///
/// builder.extend(&[
///     Record { a: Some(6.0), b: 7},
///     Record { a: Some(8.0), b: 9},
///     Record { a: Some(10.0), b: 11},
/// ])?;
///
/// let arrays = builder.build_arrays()?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # assert_eq!(arrays[0].len(), 6);
/// # Ok(())
/// # }
/// ```
pub struct ArrowBuilder(OuterSequenceBuilder);

impl std::fmt::Debug for ArrowBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArrowBuilder<...>")
    }
}

impl ArrowBuilder {
    /// Build a new ArrowBuilder for the given fields
    ///
    /// This method may fail when unsupported data types are encountered in the
    /// given fields.
    ///
    pub fn new(fields: &[Field]) -> Result<Self> {
        let schema = SerdeArrowSchema::from_arrow_fields(fields)?;
        Ok(Self(OuterSequenceBuilder::new(&schema)?))
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
        self.0.build_arrow_arrays()
    }
}

/// Build arrow arrays from the given items  (*requires one of the `arrow-*`
/// features*))
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`ArrowBuilder`].
///
/// Example:
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow;
/// use arrow::datatypes::Field;
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
/// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # Ok(())
/// # }
/// ```
///
/// The arrays can be used as is to construct a record batch
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow::datatypes::Field;
/// # use serde::{Serialize, Deserialize};
/// # use serde_arrow::schema::{SchemaLike, TracingOptions};
/// # mod arrow {
/// #   pub mod array { pub use serde_arrow::_impl::arrow::_raw::array::RecordBatch;  }
/// #   pub mod datatypes { pub use serde_arrow::_impl::arrow::_raw::schema::Schema; }
/// # }
/// # #[derive(Serialize, Deserialize)]
/// # struct Record { a: Option<f32>, b: u64 }
/// # let items = vec![ Record { a: Some(1.0), b: 2}, ];
/// # let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// # let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// use arrow::{array::RecordBatch, datatypes::Schema};
/// use std::sync::Arc;
///
/// let schema = Schema::new(fields);
/// let record_batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
///
/// assert_eq!(record_batch.num_columns(), 2);
/// assert_eq!(record_batch.num_rows(), 1);
/// # Ok(())
/// # }
/// ```
///
pub fn to_arrow<T: Serialize + ?Sized>(fields: &[Field], items: &T) -> Result<Vec<ArrayRef>> {
    let mut builder = ArrowBuilder::new(fields)?;
    builder.extend(items)?;
    builder.build_arrays()
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
/// use arrow::datatypes::Field;
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// # let items = &[Record { a: Some(1.0), b: 2}];
/// # let arrays = serde_arrow::to_arrow(&fields, &items)?;
/// #
/// let items: Vec<Record> = serde_arrow::from_arrow(&fields, &arrays)?;
/// # Ok(())
/// # }
/// ```
///
pub fn from_arrow<'de, T, A>(fields: &[Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    let fields = fields
        .iter()
        .map(GenericField::try_from)
        .collect::<Result<Vec<_>>>()?;
    let arrays = arrays
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();

    let mut deserializer = build_deserializer(&fields, &arrays)?;
    let res = T::deserialize(Mut(&mut deserializer))?;
    Ok(res)
}
