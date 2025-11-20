use marrow::{array::Array, datatypes::Field, view::View};
use serde::{Deserialize, Serialize};

use crate::internal::{
    array_builder::ArrayBuilder, deserializer::Deserializer, error::Result, serializer::Serializer,
};

/// Build [marrow array][marrow::array::Array] from the given items
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
/// use marrow::{array::{Array, PrimitiveArray}, datatypes::Field};
/// use serde::{Serialize, Deserialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Debug, PartialEq, Serialize, Deserialize)]
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
/// let arrays = serde_arrow::to_marrow(&fields, &items)?;
///
/// assert_eq!(
///     arrays,
///     vec![
///         Array::Float32(PrimitiveArray {
///             validity: Some(marrow::bit_vec![true]),
///             values: vec![1.0],
///         }),
///         Array::UInt64(PrimitiveArray {
///             validity: None,
///             values: vec![2],
///         }),
///     ],
/// );
/// # Ok(())
/// # }
/// ```
///
pub fn to_marrow<T: Serialize>(fields: &[Field], items: T) -> Result<Vec<Array>> {
    let builder = ArrayBuilder::from_marrow(fields)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .into_marrow()
}

/// Deserialize items from [marrow views][marrow::view::View]
///
/// The type should be a list of records (e.g., a vector of structs). To
/// deserialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// use marrow::{datatypes::Field, view::{BitsWithOffset, View, PrimitiveView}};
/// use serde::{Deserialize, Serialize};
/// use serde_arrow::schema::{SchemaLike, TracingOptions};
///
/// ##[derive(Debug, PartialEq, Deserialize, Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let views = vec![
///     View::Float32(PrimitiveView {
///         validity: Some(BitsWithOffset {
///             offset: 0,
///             data: &const { marrow::bit_array![true, false, true] },
///         }),
///         values: &[13.0, 0.0, 17.0],
///     }),
///     View::UInt64(PrimitiveView {
///         validity: None,
///         values: &[21, 42, 84],
///     }),
/// ];
///
/// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
/// let items: Vec<Record> = serde_arrow::from_marrow(&fields, &views)?;
///
/// assert_eq!(
///     items,
///     vec![
///         Record { a: Some(13.0), b: 21 },
///         Record { a: None, b: 42 },
///         Record { a: Some(17.0), b: 84 },
///     ],
/// );
/// # Ok(())
/// # }
/// ```
///
pub fn from_marrow<'de, T>(fields: &[Field], views: &'de [View]) -> Result<T>
where
    T: Deserialize<'de>,
{
    T::deserialize(Deserializer::from_marrow(fields, views)?)
}

impl ArrayBuilder {
    /// Build an array builder from [`marrow::Field`s][Field]
    pub fn from_marrow(fields: &[Field]) -> Result<Self> {
        ArrayBuilder::from_marrow_vec(fields.to_vec())
    }

    /// Construct [`marrow::Array`s][Array] and reset the builder
    pub fn to_marrow(&mut self) -> Result<Vec<Array>> {
        let (arrays, _) = self.take().into_arrays_and_field_metas()?;
        Ok(arrays)
    }

    /// Consume the builder and construct the arrays
    pub fn into_marrow(self) -> Result<Vec<Array>> {
        Ok(self.into_arrays_and_field_metas()?.0)
    }
}

impl<'de> Deserializer<'de> {
    /// Build a deserializer from [`marrow::Field`s][Field] and [`marrow::View`s][View]
    pub fn from_marrow(fields: &[Field], views: &[View<'de>]) -> Result<Self> {
        Self::new(fields, views.to_vec())
    }
}
