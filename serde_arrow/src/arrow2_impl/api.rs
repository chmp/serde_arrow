//! Support for the `arrow2` crate (requires one the `arrow2-*` features)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow2::{array::Array, datatypes::Field},
    internal::{
        array_builder::ArrayBuilder, deserializer::Deserializer, error::Result,
        schema::SerdeArrowSchema, serializer::Serializer,
    },
};

/// Build arrow2 arrays record by record (*requires one of the `arrow2-*`
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
/// # use serde_arrow::_impl::arrow2 as arrow2;
/// use arrow2::datatypes::{DataType, Field};
/// use serde::Serialize;
/// use serde_arrow::Arrow2Builder;
///
/// ##[derive(Serialize)]
/// struct Record {
///     a: Option<f32>,
///     b: u64,
/// }
///
/// let mut builder = Arrow2Builder::new(&[
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
pub struct Arrow2Builder(ArrayBuilder);

impl std::fmt::Debug for Arrow2Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Arrow2Builder<...>")
    }
}

impl Arrow2Builder {
    /// Build a new Arrow2Builder for the given fields
    ///
    /// This method may fail when unsupported data types are encountered in the
    /// given fields.
    ///
    pub fn new(fields: &[Field]) -> Result<Self> {
        let schema = SerdeArrowSchema::from_arrow2_fields(fields)?;
        Ok(Self(ArrayBuilder::new(schema)?))
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
    pub fn build_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        self.0.to_arrow2()
    }
}

/// Build arrow2 arrays from the given items  (*requires one of the `arrow2-*`
/// features*)
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`Arrow2Builder`].
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow2;
/// use arrow2::datatypes::Field;
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
/// let arrays = serde_arrow::to_arrow2(&fields, &items)?;
/// #
/// # assert_eq!(arrays.len(), 2);
/// # Ok(())
/// # }
/// ```
///
pub fn to_arrow2<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
where
    T: Serialize + ?Sized,
{
    let mut builder = ArrayBuilder::from_arrow2(fields)?;
    items.serialize(Serializer::new(&mut builder))?;
    builder.to_arrow2()
}

/// Deserialize items from the given arrow2 arrays  (*requires one of the
/// `arrow2-*` features*)
///
/// The type should be a list of records (e.g., a vector of structs). To
/// deserialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// ```rust
/// # fn main() -> serde_arrow::Result<()> {
/// # use serde_arrow::_impl::arrow2;
/// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow2_arrays();
/// use arrow2::datatypes::Field;
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
/// let items: Vec<Record> = serde_arrow::from_arrow2(&fields, &arrays)?;
/// # Ok(())
/// # }
/// ```
///
pub fn from_arrow2<'de, T, A>(fields: &[Field], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    let deserializer = Deserializer::from_arrow2(fields, arrays)?;
    T::deserialize(deserializer)
}
