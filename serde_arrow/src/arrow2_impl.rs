//! Support for the `arrow2` crate (requires one the `arrow2-*` features)
//!
//! Functions to convert Rust objects into Arrow arrays and back.
//!
#![deny(missing_docs)]
use serde::{Deserialize, Serialize};

use marrow::{datatypes::Field, error::MarrowError, view::View};

use crate::{
    _impl::arrow2::{array::Array, datatypes::Field as ArrowField},
    internal::{
        array_builder::ArrayBuilder,
        deserializer::Deserializer,
        error::{fail, Error, Result},
        schema::{SchemaLike, Sealed, SerdeArrowSchema, TracingOptions},
        serializer::Serializer,
    },
};

/// Build arrow2 arrays from the given items  (*requires one of the `arrow2-*`
/// features*)
///
/// `items` should be given in the form a list of records (e.g., a vector of
/// structs). To serialize items encoding single values consider the
/// [`Items`][crate::utils::Items] wrapper.
///
/// To build arrays record by record use [`ArrayBuilder`].
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
pub fn to_arrow2<T: Serialize>(fields: &[ArrowField], items: T) -> Result<Vec<Box<dyn Array>>> {
    let builder = ArrayBuilder::from_arrow2(fields)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .to_arrow2()
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
pub fn from_arrow2<'de, T, A>(fields: &[ArrowField], arrays: &'de [A]) -> Result<T>
where
    T: Deserialize<'de>,
    A: AsRef<dyn Array>,
{
    let deserializer = Deserializer::from_arrow2(fields, arrays)?;
    T::deserialize(deserializer)
}

/// Support `arrow2` (*requires one of the `arrow2-*` features*)
impl crate::internal::array_builder::ArrayBuilder {
    /// Build an ArrayBuilder from `arrow2` fields (*requires one of the
    /// `arrow2-*` features*)
    pub fn from_arrow2(fields: &[ArrowField]) -> Result<Self> {
        Self::new(SerdeArrowSchema::try_from(fields)?)
    }

    /// Construct `arrow2` arrays and reset the builder (*requires one of the
    /// `arrow2-*` features*)
    pub fn to_arrow2(&mut self) -> Result<Vec<Box<dyn Array>>> {
        Ok(self
            .to_marrow()?
            .into_iter()
            .map(Box::<dyn Array>::try_from)
            .collect::<Result<_, MarrowError>>()?)
    }
}

impl<'de> Deserializer<'de> {
    /// Build a deserializer from `arrow2` arrays (*requires one of the
    /// `arrow2-*` features*)
    ///
    /// Usage:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # use serde_arrow::_impl::arrow2;
    /// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow2_arrays();
    /// use arrow2::datatypes::Field;
    /// use serde::{Deserialize, Serialize};
    /// use serde_arrow::{Deserializer, schema::{SchemaLike, TracingOptions}};
    ///
    /// ##[derive(Deserialize, Serialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let fields = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// let deserializer = Deserializer::from_arrow2(&fields, &arrays)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_arrow2<A>(fields: &[ArrowField], arrays: &'de [A]) -> Result<Self>
    where
        A: AsRef<dyn Array>,
    {
        if fields.len() != arrays.len() {
            fail!(
                "different number of fields ({}) and arrays ({})",
                fields.len(),
                arrays.len()
            );
        }

        let fields = fields
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>, MarrowError>>()?;
        let views = arrays
            .iter()
            .map(|array| View::try_from(array.as_ref()))
            .collect::<Result<Vec<_>, MarrowError>>()?;

        Deserializer::new(&fields, views)
    }
}

impl TryFrom<SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        Vec::<ArrowField>::try_from(&value)
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        Ok(value
            .fields
            .iter()
            .map(ArrowField::try_from)
            .collect::<Result<_, MarrowError>>()?)
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow2::datatype::Field>` (*requires one of the
/// `arrow2-*` features*)
impl SchemaLike for Vec<ArrowField> {
    fn from_value<T: serde::Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: serde::Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: serde::Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}
