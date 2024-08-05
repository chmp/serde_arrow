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
        use {DataType as T, TimeUnit as U};
        match value {
            ArrowDataType::Boolean => Ok(T::Boolean),
            ArrowDataType::Null => Ok(T::Null),
            ArrowDataType::Int8 => Ok(T::Int8),
            ArrowDataType::Int16 => Ok(T::Int16),
            ArrowDataType::Int32 => Ok(T::Int32),
            ArrowDataType::Int64 => Ok(T::Int64),
            ArrowDataType::UInt8 => Ok(T::UInt8),
            ArrowDataType::UInt16 => Ok(T::UInt16),
            ArrowDataType::UInt32 => Ok(T::UInt32),
            ArrowDataType::UInt64 => Ok(T::UInt64),
            ArrowDataType::Float16 => Ok(T::Float16),
            ArrowDataType::Float32 => Ok(T::Float32),
            ArrowDataType::Float64 => Ok(T::Float64),
            ArrowDataType::Utf8 => Ok(T::Utf8),
            ArrowDataType::LargeUtf8 => Ok(T::LargeUtf8),
            ArrowDataType::Date32 => Ok(T::Date32),
            ArrowDataType::Date64 => Ok(T::Date64),
            ArrowDataType::Decimal128(precision, scale) => Ok(T::Decimal128(*precision, *scale)),
            ArrowDataType::Time32(ArrowTimeUnit::Second) => Ok(T::Time32(U::Second)),
            ArrowDataType::Time32(ArrowTimeUnit::Millisecond) => Ok(T::Time32(U::Millisecond)),
            ArrowDataType::Time32(unit) => fail!("Invalid time unit {unit:?} for Time32"),
            ArrowDataType::Time64(ArrowTimeUnit::Microsecond) => Ok(T::Time64(U::Microsecond)),
            ArrowDataType::Time64(ArrowTimeUnit::Nanosecond) => Ok(T::Time64(U::Nanosecond)),
            ArrowDataType::Time64(unit) => fail!("Invalid time unit {unit:?} for Time64"),
            ArrowDataType::Timestamp(ArrowTimeUnit::Second, tz) => {
                Ok(T::Timestamp(U::Second, tz.as_ref().map(|s| s.to_string())))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, tz) => Ok(T::Timestamp(
                U::Millisecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, tz) => Ok(T::Timestamp(
                U::Microsecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, tz) => Ok(T::Timestamp(
                U::Nanosecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            ArrowDataType::Duration(ArrowTimeUnit::Second) => Ok(T::Duration(U::Second)),
            ArrowDataType::Duration(ArrowTimeUnit::Millisecond) => Ok(T::Duration(U::Millisecond)),
            ArrowDataType::Duration(ArrowTimeUnit::Microsecond) => Ok(T::Duration(U::Microsecond)),
            ArrowDataType::Duration(ArrowTimeUnit::Nanosecond) => Ok(T::Duration(U::Nanosecond)),
            ArrowDataType::Binary => Ok(T::Binary),
            ArrowDataType::LargeBinary => Ok(T::LargeBinary),
            ArrowDataType::FixedSizeBinary(n) => Ok(T::FixedSizeBinary(*n)),
            _ => fail!("Only primitive data types can be converted to T"),
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
        todo!()
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

impl From<TimeUnit> for ArrowTimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

impl From<ArrowUnionMode> for UnionMode {
    fn from(value: ArrowUnionMode) -> Self {
        match value {
            ArrowUnionMode::Dense => UnionMode::Dense,
            ArrowUnionMode::Sparse => UnionMode::Sparse,
        }
    }
}

impl From<UnionMode> for ArrowUnionMode {
    fn from(value: UnionMode) -> Self {
        match value {
            UnionMode::Dense => ArrowUnionMode::Dense,
            UnionMode::Sparse => ArrowUnionMode::Sparse,
        }
    }
}
