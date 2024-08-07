use std::sync::Arc;

use crate::{
    _impl::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, FieldRef, TimeUnit as ArrowTimeUnit,
        UnionMode as ArrowUnionMode,
    },
    internal::{
        arrow::{DataType, Field, TimeUnit, UnionMode},
        error::{fail, Error, Result},
        schema::{validate_field, SchemaLike, Sealed, SerdeArrowSchema},
    },
};

impl TryFrom<SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        value.fields.iter().map(ArrowField::try_from).collect()
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
        value
            .fields
            .iter()
            .map(|f| Ok(Arc::new(ArrowField::try_from(f)?)))
            .collect()
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> Result<Self> {
        Ok(Self {
            fields: fields.iter().map(Field::try_from).collect::<Result<_>>()?,
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
                .collect::<Result<_>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow::datatype::Field>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<ArrowField> {
    fn from_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: serde::Deserialize<'de> + ?Sized>(
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: serde::Serialize + ?Sized>(
        samples: &T,
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}

impl Sealed for Vec<FieldRef> {}

/// Schema support for `Vec<arrow::datatype::FieldRef>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<FieldRef> {
    fn from_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.try_into()
    }

    fn from_type<'de, T: serde::Deserialize<'de> + ?Sized>(
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.try_into()
    }

    fn from_samples<T: serde::Serialize + ?Sized>(
        samples: &T,
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.try_into()
    }
}

impl TryFrom<&ArrowDataType> for DataType {
    type Error = Error;

    fn try_from(value: &ArrowDataType) -> Result<DataType> {
        use {ArrowDataType as AT, DataType as T, Field as F};
        match value {
            AT::Boolean => Ok(T::Boolean),
            AT::Null => Ok(T::Null),
            AT::Int8 => Ok(T::Int8),
            AT::Int16 => Ok(T::Int16),
            AT::Int32 => Ok(T::Int32),
            AT::Int64 => Ok(T::Int64),
            AT::UInt8 => Ok(T::UInt8),
            AT::UInt16 => Ok(T::UInt16),
            AT::UInt32 => Ok(T::UInt32),
            AT::UInt64 => Ok(T::UInt64),
            AT::Float16 => Ok(T::Float16),
            AT::Float32 => Ok(T::Float32),
            AT::Float64 => Ok(T::Float64),
            AT::Utf8 => Ok(T::Utf8),
            AT::LargeUtf8 => Ok(T::LargeUtf8),
            AT::Date32 => Ok(T::Date32),
            AT::Date64 => Ok(T::Date64),
            AT::Decimal128(precision, scale) => Ok(T::Decimal128(*precision, *scale)),
            AT::Time32(unit) => Ok(T::Time32(unit.clone().into())),
            AT::Time64(unit) => Ok(T::Time64(unit.clone().into())),
            AT::Timestamp(unit, tz) => Ok(T::Timestamp(
                unit.clone().into(),
                tz.as_ref().map(|s| s.to_string()),
            )),
            AT::Duration(unit) => Ok(T::Duration(unit.clone().into())),
            AT::Binary => Ok(T::Binary),
            AT::LargeBinary => Ok(T::LargeBinary),
            AT::FixedSizeBinary(n) => Ok(T::FixedSizeBinary(*n)),
            AT::List(field) => Ok(T::List(F::try_from(field.as_ref())?.into())),
            AT::LargeList(field) => Ok(T::LargeList(F::try_from(field.as_ref())?.into())),
            AT::FixedSizeList(field, n) => {
                Ok(T::FixedSizeList(F::try_from(field.as_ref())?.into(), *n))
            }
            AT::Map(field, sorted) => Ok(T::Map(F::try_from(field.as_ref())?.into(), *sorted)),
            AT::Struct(in_fields) => {
                let mut fields = Vec::new();
                for field in in_fields {
                    fields.push(field.as_ref().try_into()?);
                }
                Ok(T::Struct(fields))
            }
            AT::Dictionary(key, value) => Ok(T::Dictionary(
                T::try_from(key.as_ref())?.into(),
                T::try_from(value.as_ref())?.into(),
                false,
            )),
            AT::Union(in_fields, mode) => {
                let mut fields = Vec::new();
                for (type_id, field) in in_fields.iter() {
                    fields.push((type_id, F::try_from(field.as_ref())?));
                }
                Ok(T::Union(fields, (*mode).into()))
            }
            data_type => fail!("Unsupported arrow data type {data_type}"),
        }
    }
}

impl TryFrom<&ArrowField> for Field {
    type Error = Error;

    fn try_from(field: &ArrowField) -> Result<Self> {
        let field = Field {
            name: field.name().to_owned(),
            data_type: DataType::try_from(field.data_type())?,
            metadata: field.metadata().clone(),
            nullable: field.is_nullable(),
        };
        validate_field(&field)?;
        Ok(field)
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = Error;

    fn try_from(value: &DataType) -> std::result::Result<Self, Self::Error> {
        use {ArrowDataType as AT, ArrowField as AF, DataType as T};
        match value {
            T::Boolean => Ok(AT::Boolean),
            T::Null => Ok(AT::Null),
            T::Int8 => Ok(AT::Int8),
            T::Int16 => Ok(AT::Int16),
            T::Int32 => Ok(AT::Int32),
            T::Int64 => Ok(AT::Int64),
            T::UInt8 => Ok(AT::UInt8),
            T::UInt16 => Ok(AT::UInt16),
            T::UInt32 => Ok(AT::UInt32),
            T::UInt64 => Ok(AT::UInt64),
            T::Float16 => Ok(AT::Float16),
            T::Float32 => Ok(AT::Float32),
            T::Float64 => Ok(AT::Float64),
            T::Utf8 => Ok(AT::Utf8),
            T::LargeUtf8 => Ok(AT::LargeUtf8),
            T::Date32 => Ok(AT::Date32),
            T::Date64 => Ok(AT::Date64),
            T::Decimal128(precision, scale) => Ok(AT::Decimal128(*precision, *scale)),
            T::Time32(unit) => Ok(AT::Time32((*unit).into())),
            T::Time64(unit) => Ok(AT::Time64((*unit).into())),
            T::Timestamp(unit, tz) => Ok(AT::Timestamp(
                (*unit).into(),
                tz.as_ref().map(|s| s.to_string().into()),
            )),
            T::Duration(unit) => Ok(AT::Duration((*unit).into())),
            T::Binary => Ok(AT::Binary),
            T::LargeBinary => Ok(AT::LargeBinary),
            T::FixedSizeBinary(n) => Ok(AT::FixedSizeBinary(*n)),
            T::List(field) => Ok(AT::List(AF::try_from(field.as_ref())?.into())),
            T::LargeList(field) => Ok(AT::LargeList(AF::try_from(field.as_ref())?.into())),
            T::FixedSizeList(field, n) => {
                Ok(AT::FixedSizeList(AF::try_from(field.as_ref())?.into(), *n))
            }
            T::Map(field, sorted) => Ok(AT::Map(AF::try_from(field.as_ref())?.into(), *sorted)),
            T::Struct(in_fields) => {
                let mut fields: Vec<FieldRef> = Vec::new();
                for field in in_fields {
                    fields.push(AF::try_from(field)?.into());
                }
                Ok(AT::Struct(fields.into()))
            }
            T::Dictionary(key, value, _sorted) => Ok(AT::Dictionary(
                AT::try_from(key.as_ref())?.into(),
                AT::try_from(value.as_ref())?.into(),
            )),
            T::Union(in_fields, mode) => {
                let mut fields = Vec::new();
                for (type_id, field) in in_fields {
                    fields.push((*type_id, Arc::new(AF::try_from(field)?)));
                }
                Ok(AT::Union(fields.into_iter().collect(), (*mode).into()))
            }
        }
    }
}

impl TryFrom<&Field> for ArrowField {
    type Error = Error;

    fn try_from(value: &Field) -> Result<Self> {
        let mut field = ArrowField::new(
            &value.name,
            ArrowDataType::try_from(&value.data_type)?,
            value.nullable,
        );
        field.set_metadata(value.metadata.clone());

        Ok(field)
    }
}

macro_rules! impl_from_one_to_one {
    (
        $src_ty:ty => $dst_ty:ty,
        [
            $($src_variant:ident => $dst_variant:ident),*
        ]
    ) => {
        impl From<$dst_ty> for $src_ty {
            fn from(value: $dst_ty) -> Self {
                match value {
                    $(<$dst_ty>::$dst_variant => <$src_ty>::$src_variant,)*
                }
            }
        }

        impl From<$src_ty> for $dst_ty {
            fn from(value: $src_ty) -> Self {
                match value {
                    $(<$src_ty>::$src_variant => <$dst_ty>::$dst_variant,)*
                }
            }
        }
    };
}

impl_from_one_to_one!(
    TimeUnit => ArrowTimeUnit,
    [Second => Second, Millisecond => Millisecond, Microsecond => Microsecond, Nanosecond => Nanosecond]
);

impl_from_one_to_one!(UnionMode => ArrowUnionMode, [Sparse => Sparse, Dense => Dense]);
