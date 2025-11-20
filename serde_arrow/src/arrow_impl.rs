//! Support for the `arrow` crate (*requires one the `arrow-*` features*)
//!
//! Functions to convert Rust objects into arrow Arrays. Deserialization from
//! `arrow` arrays to Rust objects is not yet supported.
//!
#![deny(missing_docs)]
use std::sync::Arc;

use marrow::{datatypes::Field, error::MarrowError, view::View};
use serde::{Deserialize, Serialize};

use crate::{
    _impl::arrow::{
        array::{Array, ArrayRef, RecordBatch},
        datatypes::{Field as ArrowField, FieldRef, Schema},
    },
    internal::{
        array_builder::ArrayBuilder,
        deserializer::Deserializer,
        error::{fail, Error, Result},
        schema::extensions::{Bool8Field, FixedShapeTensorField, VariableShapeTensorField},
        schema::{SchemaLike, Sealed, SerdeArrowSchema, TracingOptions},
        serializer::Serializer,
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
pub fn to_arrow<T: Serialize>(fields: &[FieldRef], items: T) -> Result<Vec<ArrayRef>> {
    let builder = ArrayBuilder::from_arrow(fields)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .into_arrow()
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
pub fn to_record_batch<T: Serialize>(fields: &[FieldRef], items: &T) -> Result<RecordBatch> {
    let builder = ArrayBuilder::from_arrow(fields)?;
    items
        .serialize(Serializer::new(builder))?
        .into_inner()
        .into_record_batch()
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

/// Support `arrow` (*requires one of the `arrow-*` features*)
impl crate::internal::array_builder::ArrayBuilder {
    /// Build an ArrayBuilder from `arrow` fields (*requires one of the
    /// `arrow-*` features*)
    pub fn from_arrow(fields: &[FieldRef]) -> Result<Self> {
        let fields = fields_from_field_refs(fields)?;
        Self::new(SerdeArrowSchema { fields })
    }

    /// Construct `arrow` arrays and reset the builder (*requires one of the
    /// `arrow-*` features*)
    pub fn to_arrow(&mut self) -> Result<Vec<ArrayRef>> {
        Ok(self
            .to_marrow()?
            .into_iter()
            .map(ArrayRef::try_from)
            .collect::<Result<_, MarrowError>>()?)
    }

    /// Consume the builder and construct the `arrow` arrays (*requires one of
    /// the `arrow-*` features*)
    pub fn into_arrow(self) -> Result<Vec<ArrayRef>> {
        Ok(self
            .into_arrays_and_field_metas()?
            .0
            .into_iter()
            .map(ArrayRef::try_from)
            .collect::<Result<_, MarrowError>>()?)
    }

    /// Construct a [`RecordBatch`] and reset the builder (*requires one of the
    /// `arrow-*` features*)
    pub fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let mut arrays = Vec::with_capacity(self.builder.num_fields());
        let mut fields = Vec::with_capacity(self.builder.num_fields());

        for builder in &mut self.builder.0.fields {
            let (array, meta) = builder.take().into_array_and_field_meta()?;
            let array = ArrayRef::try_from(array)?;
            let field = FieldRef::new(
                ArrowField::new(&meta.name, array.data_type().clone(), meta.nullable)
                    .with_metadata(meta.metadata.clone()),
            );
            arrays.push(array);
            fields.push(field)
        }

        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|err| Error::custom_from(err.to_string(), err))
    }

    /// Construct a [`RecordBatch`] and consume the builder (*requires one of the
    /// `arrow-*` features*)
    pub fn into_record_batch(self) -> Result<RecordBatch> {
        let (arrays, metas) = self.into_arrays_and_field_metas()?;

        let arrays = arrays
            .into_iter()
            .map(ArrayRef::try_from)
            .collect::<Result<Vec<ArrayRef>, MarrowError>>()?;

        let mut fields = Vec::with_capacity(arrays.len());
        for (array, meta) in std::iter::zip(&arrays, metas) {
            fields.push(FieldRef::new(
                ArrowField::new(meta.name, array.data_type().clone(), meta.nullable)
                    .with_metadata(meta.metadata),
            ));
        }

        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|err| Error::custom_from(err.to_string(), err))
    }
}

impl<'de> Deserializer<'de> {
    /// Construct a new deserializer from `arrow` arrays (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let (_, arrays) = serde_arrow::_impl::docs::defs::example_arrow_arrays();
    /// # use serde_arrow::_impl::arrow;
    /// use arrow::datatypes::FieldRef;
    /// use serde::{Deserialize, Serialize};
    /// use serde_arrow::{Deserializer, schema::{SchemaLike, TracingOptions}};
    ///
    /// ##[derive(Deserialize, Serialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let fields = Vec::<FieldRef>::from_type::<Record>(TracingOptions::default())?;
    ///
    /// let deserializer = Deserializer::from_arrow(&fields, &arrays)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_arrow<A>(fields: &[FieldRef], arrays: &'de [A]) -> Result<Self>
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

        let fields = fields_from_field_refs(fields)?;

        let mut views = Vec::new();
        for array in arrays {
            views.push(View::try_from(array.as_ref())?);
        }

        Deserializer::new(&fields, views)
    }

    /// Construct a new deserializer from a record batch (*requires one of the
    /// `arrow-*` features*)
    ///
    /// Usage:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::Result<()> {
    /// # let record_batch = serde_arrow::_impl::docs::defs::example_record_batch();
    /// #
    /// use serde::Deserialize;
    /// use serde_arrow::Deserializer;
    ///
    /// ##[derive(Deserialize)]
    /// struct Record {
    ///     a: Option<f32>,
    ///     b: u64,
    /// }
    ///
    /// let deserializer = Deserializer::from_record_batch(&record_batch)?;
    /// let items = Vec::<Record>::deserialize(deserializer)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn from_record_batch(record_batch: &'de RecordBatch) -> Result<Self> {
        let schema = record_batch.schema();
        Deserializer::from_arrow(schema.fields(), record_batch.columns())
    }
}

fn fields_from_field_refs(fields: &[FieldRef]) -> Result<Vec<Field>> {
    Ok(fields
        .iter()
        .map(|field| Field::try_from(field.as_ref()))
        .collect::<Result<_, MarrowError>>()?)
}

impl TryFrom<SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
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

impl TryFrom<SerdeArrowSchema> for Vec<FieldRef> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<FieldRef> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        Ok(value
            .fields
            .iter()
            .map(|f| Ok(Arc::new(ArrowField::try_from(f)?)))
            .collect::<Result<_, MarrowError>>()?)
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> Result<Self> {
        Ok(Self {
            fields: fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl<'a> TryFrom<&'a [FieldRef]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [FieldRef]) -> Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(|f| Field::try_from(f.as_ref()))
                .collect::<Result<_, MarrowError>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow::datatype::Field>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<ArrowField> {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}

impl Sealed for Vec<FieldRef> {}

/// Schema support for `Vec<arrow::datatype::FieldRef>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<FieldRef> {
    fn from_value<T: Serialize>(value: T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}

macro_rules! impl_try_from_ext_type {
    ($ty:ty) => {
        impl TryFrom<&$ty> for ArrowField {
            type Error = Error;

            fn try_from(value: &$ty) -> Result<Self, Self::Error> {
                Ok(Self::try_from(&Field::try_from(value)?)?)
            }
        }

        impl TryFrom<$ty> for ArrowField {
            type Error = Error;

            fn try_from(value: $ty) -> Result<Self, Self::Error> {
                Self::try_from(&value)
            }
        }
    };
}

impl_try_from_ext_type!(Bool8Field);
impl_try_from_ext_type!(FixedShapeTensorField);
impl_try_from_ext_type!(VariableShapeTensorField);
