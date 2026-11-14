use marrow::{
    array::Array,
    datatypes::Field,
    view::{TrustedView, View},
};
use serde::{Deserialize, Serialize};

use crate::internal::{
    array_builder::ArrayBuilder, deserializer::Deserializer, error::Result, serializer::Serializer,
};

/// Deserialize from views whose UTF-8 validity has been asserted by the caller.
///
/// # Safety boundary
///
/// Constructing each [`TrustedView`] is unsafe. The caller must guarantee that
/// every accessible string, including strings in nested views, is valid UTF-8.
/// Existing [`from_marrow`] calls continue to validate strings as they are read.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// use marrow::{
///     datatypes::{DataType, Field},
///     view::{BytesView, View},
/// };
/// use marrow::view::TrustedView;
/// use serde_arrow::{from_marrow_trusted, utils::Item};
///
/// let fields = [Field {
///     name: String::from("item"),
///     data_type: DataType::Utf8,
///     ..Field::default()
/// }];
/// let view = View::Utf8(BytesView {
///     validity: None,
///     offsets: &[0_i32, 2],
///     data: b"hi",
/// });
/// // SAFETY: the only accessible string is the valid UTF-8 value "hi".
/// let views = [unsafe { TrustedView::new(view) }];
/// let values: Vec<Item<String>> = from_marrow_trusted(&fields, &views)?;
/// assert_eq!(values[0].0, "hi");
/// # Ok(())
/// # }
/// ```
pub fn from_marrow_trusted<'de, T>(fields: &[Field], views: &[TrustedView<'de>]) -> Result<T>
where
    T: Deserialize<'de>,
{
    T::deserialize(Deserializer::from_marrow_trusted(fields, views)?)
}

/// Build [marrow arrays][marrow::array::Array] from the given items
///
/// `items` should be given in the form of a list of records (e.g., a vector of
/// structs). To serialize single values, use the [`Items`][crate::utils::Items]
/// wrapper.
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
/// deserialize single values, use the [`Items`][crate::utils::Items] wrapper.
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
        self.take().into_marrow()
    }

    /// Consume the builder and construct the arrays
    pub fn into_marrow(self) -> Result<Vec<Array>> {
        let (arrays, _) = self.into_arrays_and_field_metas()?;
        Ok(arrays)
    }
}

impl<'de> Deserializer<'de> {
    /// Build a deserializer from [`marrow::Field`s][Field] and [`marrow::View`s][View]
    pub fn from_marrow(fields: &[Field], views: &[View<'de>]) -> Result<Self> {
        Self::new(fields, views.to_vec())
    }

    /// Build a deserializer from views with caller-asserted UTF-8 validity.
    pub fn from_marrow_trusted(fields: &[Field], views: &[TrustedView<'de>]) -> Result<Self> {
        // SAFETY: every TrustedView can only be constructed with the required guarantee.
        let utf8 = unsafe { crate::internal::utils::array_view_ext::Utf8Check::trusted() };
        Self::with_utf8_check(
            fields,
            views.iter().cloned().map(View::from).collect(),
            utf8,
        )
    }
}
