use crate::{
    _impl::arrow2::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, IntegerType, TimeUnit as ArrowTimeUnit,
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
        Vec::<ArrowField>::try_from(&value)
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<ArrowField> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        value.fields.iter().map(ArrowField::try_from).collect()
    }
}

impl<'a> TryFrom<&'a [ArrowField]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [ArrowField]) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(Self {
            fields: fields.iter().map(Field::try_from).collect::<Result<_>>()?,
        })
    }
}

impl Sealed for Vec<ArrowField> {}

/// Schema support for `Vec<arrow2::datatype::Field>` (*requires one of the
/// `arrow2-*` features*)
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

impl TryFrom<&ArrowDataType> for DataType {
    type Error = Error;

    fn try_from(value: &ArrowDataType) -> Result<Self> {
        use {ArrowDataType as AT, DataType as T};
        match value {
            AT::Null => Ok(T::Null),
            AT::Boolean => Ok(T::Boolean),
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
            AT::Date32 => Ok(T::Date32),
            AT::Date64 => Ok(T::Date64),
            AT::Time32(unit) => Ok(T::Time32((*unit).into())),
            AT::Time64(unit) => Ok(T::Time64((*unit).into())),
            AT::Duration(unit) => Ok(T::Duration((*unit).into())),
            AT::Timestamp(unit, tz) => Ok(T::Timestamp((*unit).into(), tz.clone())),
            AT::Decimal(precision, scale) => {
                if *precision > u8::MAX as usize || *scale > i8::MAX as usize {
                    fail!("cannot represent precision / scale of the decimal");
                }
                Ok(T::Decimal128(*precision as u8, *scale as i8))
            }
            AT::Utf8 => Ok(T::Utf8),
            AT::LargeUtf8 => Ok(T::LargeUtf8),
            AT::Binary => Ok(T::Binary),
            AT::LargeBinary => Ok(T::LargeBinary),
            AT::FixedSizeBinary(n) => Ok(T::FixedSizeBinary(i32::try_from(*n)?)),
            AT::List(entry) => Ok(T::List(Box::new(entry.as_ref().try_into()?))),
            AT::LargeList(entry) => Ok(T::LargeList(Box::new(entry.as_ref().try_into()?))),
            AT::FixedSizeList(entry, n) => Ok(T::FixedSizeList(
                Box::new(entry.as_ref().try_into()?),
                i32::try_from(*n)?,
            )),
            AT::Map(field, sorted) => Ok(T::Map(Box::new(field.as_ref().try_into()?), *sorted)),
            AT::Struct(fields) => {
                let mut res_fields = Vec::new();
                for field in fields {
                    res_fields.push(Field::try_from(field)?);
                }
                Ok(T::Struct(res_fields))
            }
            AT::Dictionary(key, value, sorted) => {
                let key = match key {
                    IntegerType::Int8 => T::Int8,
                    IntegerType::Int16 => T::Int16,
                    IntegerType::Int32 => T::Int32,
                    IntegerType::Int64 => T::Int64,
                    IntegerType::UInt8 => T::UInt8,
                    IntegerType::UInt16 => T::UInt16,
                    IntegerType::UInt32 => T::UInt32,
                    IntegerType::UInt64 => T::UInt64,
                };
                Ok(T::Dictionary(
                    Box::new(key),
                    Box::new(value.as_ref().try_into()?),
                    *sorted,
                ))
            }
            AT::Union(fields, type_ids, mode) => todo!(),
            dt => fail!("Cannot convert data type {dt:?} to internal data type"),
        }
    }
}

impl TryFrom<&ArrowField> for Field {
    type Error = Error;

    fn try_from(field: &ArrowField) -> Result<Self> {
        let field = Field {
            name: field.name.to_owned(),
            data_type: DataType::try_from(&field.data_type)?,
            nullable: field.is_nullable,
            metadata: field.metadata.clone().into_iter().collect(),
        };
        validate_field(&field)?;
        Ok(field)
    }
}

impl TryFrom<&DataType> for ArrowDataType {
    type Error = Error;

    fn try_from(value: &DataType) -> std::result::Result<Self, Self::Error> {
        use {ArrowDataType as AT, DataType as T};
        match value {
            T::Null => Ok(AT::Null),
            T::Boolean => Ok(AT::Boolean),
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
            T::Date32 => Ok(AT::Date32),
            T::Date64 => Ok(AT::Date64),
            T::Duration(unit) => Ok(AT::Duration((*unit).into())),
            T::Time32(unit) => Ok(AT::Time32((*unit).into())),
            T::Time64(unit) => Ok(AT::Time64((*unit).into())),
            T::Timestamp(unit, tz) => Ok(AT::Timestamp((*unit).into(), tz.clone())),
            T::Decimal128(precision, scale) => {
                if *scale < 0 {
                    fail!("arrow2 does not support decimals with negative scale");
                }
                Ok(AT::Decimal((*precision).try_into()?, (*scale).try_into()?))
            }
            _ => todo!(),
        }
    }
}

impl TryFrom<&Field> for ArrowField {
    type Error = Error;

    fn try_from(value: &Field) -> Result<Self> {
        Ok(ArrowField {
            name: value.name.to_owned(),
            data_type: ArrowDataType::try_from(&value.data_type)?,
            is_nullable: value.nullable,
            metadata: value.metadata.clone().into_iter().collect(),
        })
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

impl From<ArrowTimeUnit> for TimeUnit {
    fn from(value: ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => Self::Second,
            ArrowTimeUnit::Millisecond => Self::Millisecond,
            ArrowTimeUnit::Microsecond => Self::Microsecond,
            ArrowTimeUnit::Nanosecond => Self::Nanosecond,
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
