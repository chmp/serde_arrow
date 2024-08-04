use std::sync::Arc;

use crate::{
    _impl::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit as ArrowTimeUnit, UnionMode},
    internal::{
        arrow::TimeUnit,
        error::{error, fail, Error, Result},
        schema::{
            merge_strategy_with_metadata, split_strategy_from_metadata, GenericDataType,
            GenericField, SchemaLike, Sealed, SerdeArrowSchema,
        },
    },
};

impl TryFrom<SerdeArrowSchema> for Vec<Field> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        (&value).try_into()
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<Field> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        value.fields.iter().map(Field::try_from).collect()
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
            .map(|f| Ok(Arc::new(Field::try_from(f)?)))
            .collect()
    }
}

impl<'a> TryFrom<&'a [Field]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [Field]) -> Result<Self> {
        Ok(Self {
            fields: fields
                .iter()
                .map(GenericField::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl<'a> TryFrom<&'a [FieldRef]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [FieldRef]) -> Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(|f| GenericField::try_from(f.as_ref()))
                .collect::<Result<_>>()?,
        })
    }
}

impl Sealed for Vec<Field> {}

/// Schema support for `Vec<arrow::datatype::Field>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<Field> {
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

impl TryFrom<&DataType> for GenericDataType {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<GenericDataType> {
        use {GenericDataType as T, TimeUnit as U};
        match value {
            DataType::Boolean => Ok(T::Bool),
            DataType::Null => Ok(T::Null),
            DataType::Int8 => Ok(T::I8),
            DataType::Int16 => Ok(T::I16),
            DataType::Int32 => Ok(T::I32),
            DataType::Int64 => Ok(T::I64),
            DataType::UInt8 => Ok(T::U8),
            DataType::UInt16 => Ok(T::U16),
            DataType::UInt32 => Ok(T::U32),
            DataType::UInt64 => Ok(T::U64),
            DataType::Float16 => Ok(T::F16),
            DataType::Float32 => Ok(T::F32),
            DataType::Float64 => Ok(T::F64),
            DataType::Utf8 => Ok(T::Utf8),
            DataType::LargeUtf8 => Ok(T::LargeUtf8),
            DataType::Date32 => Ok(T::Date32),
            DataType::Date64 => Ok(T::Date64),
            DataType::Decimal128(precision, scale) => Ok(T::Decimal128(*precision, *scale)),
            DataType::Time32(ArrowTimeUnit::Second) => Ok(T::Time32(U::Second)),
            DataType::Time32(ArrowTimeUnit::Millisecond) => Ok(T::Time32(U::Millisecond)),
            DataType::Time32(unit) => fail!("Invalid time unit {unit:?} for Time32"),
            DataType::Time64(ArrowTimeUnit::Microsecond) => Ok(T::Time64(U::Microsecond)),
            DataType::Time64(ArrowTimeUnit::Nanosecond) => Ok(T::Time64(U::Nanosecond)),
            DataType::Time64(unit) => fail!("Invalid time unit {unit:?} for Time64"),
            DataType::Timestamp(ArrowTimeUnit::Second, tz) => {
                Ok(T::Timestamp(U::Second, tz.as_ref().map(|s| s.to_string())))
            }
            DataType::Timestamp(ArrowTimeUnit::Millisecond, tz) => Ok(T::Timestamp(
                U::Millisecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, tz) => Ok(T::Timestamp(
                U::Microsecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, tz) => Ok(T::Timestamp(
                U::Nanosecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Duration(ArrowTimeUnit::Second) => Ok(T::Duration(U::Second)),
            DataType::Duration(ArrowTimeUnit::Millisecond) => Ok(T::Duration(U::Millisecond)),
            DataType::Duration(ArrowTimeUnit::Microsecond) => Ok(T::Duration(U::Microsecond)),
            DataType::Duration(ArrowTimeUnit::Nanosecond) => Ok(T::Duration(U::Nanosecond)),
            DataType::Binary => Ok(T::Binary),
            DataType::LargeBinary => Ok(T::LargeBinary),
            DataType::FixedSizeBinary(n) => Ok(T::FixedSizeBinary(*n)),
            _ => fail!("Only primitive data types can be converted to T"),
        }
    }
}

impl TryFrom<&Field> for GenericField {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self> {
        let metadata = field.metadata().clone();
        let (metadata, strategy) = split_strategy_from_metadata(metadata)?;

        let name = field.name().to_owned();
        let nullable = field.is_nullable();

        let mut children = Vec::<GenericField>::new();
        let data_type = match field.data_type() {
            DataType::List(field) => {
                children.push(GenericField::try_from(field.as_ref())?);
                GenericDataType::List
            }
            DataType::LargeList(field) => {
                children.push(field.as_ref().try_into()?);
                GenericDataType::LargeList
            }
            DataType::FixedSizeList(field, n) => {
                children.push(field.as_ref().try_into()?);
                GenericDataType::FixedSizeList(*n)
            }
            DataType::Struct(fields) => {
                for field in fields {
                    children.push(field.as_ref().try_into()?);
                }
                GenericDataType::Struct
            }
            DataType::Map(field, _) => {
                children.push(field.as_ref().try_into()?);
                GenericDataType::Map
            }
            DataType::Union(fields, mode) => {
                if !matches!(mode, UnionMode::Dense) {
                    fail!("Only dense unions are supported at the moment");
                }

                for (pos, (idx, field)) in fields.iter().enumerate() {
                    if pos as i8 != idx {
                        fail!("Union types with non-sequential field indices are not supported");
                    }
                    children.push(field.as_ref().try_into()?);
                }
                GenericDataType::Union
            }
            DataType::Dictionary(key_type, value_type) => {
                children.push(GenericField::new("", key_type.as_ref().try_into()?, false));
                children.push(GenericField::new(
                    "",
                    value_type.as_ref().try_into()?,
                    false,
                ));
                GenericDataType::Dictionary
            }
            dt => dt.try_into()?,
        };

        let field = GenericField {
            name,
            data_type,
            metadata,
            strategy,
            children,
            nullable,
        };
        field.validate()?;

        Ok(field)
    }
}

impl TryFrom<&GenericField> for Field {
    type Error = Error;

    fn try_from(value: &GenericField) -> Result<Self> {
        use {GenericDataType as T, TimeUnit as U};

        let data_type = match &value.data_type {
            T::Null => DataType::Null,
            T::Bool => DataType::Boolean,
            T::I8 => DataType::Int8,
            T::I16 => DataType::Int16,
            T::I32 => DataType::Int32,
            T::I64 => DataType::Int64,
            T::U8 => DataType::UInt8,
            T::U16 => DataType::UInt16,
            T::U32 => DataType::UInt32,
            T::U64 => DataType::UInt64,
            T::F16 => DataType::Float16,
            T::F32 => DataType::Float32,
            T::F64 => DataType::Float64,
            T::Date32 => DataType::Date32,
            T::Date64 => DataType::Date64,
            T::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            T::Utf8 => DataType::Utf8,
            T::LargeUtf8 => DataType::LargeUtf8,
            T::List => DataType::List(
                Box::<Field>::new(
                    value
                        .children
                        .first()
                        .ok_or_else(|| error!("List must a single child"))?
                        .try_into()?,
                )
                .into(),
            ),
            T::LargeList => DataType::LargeList(
                Box::<Field>::new(
                    value
                        .children
                        .first()
                        .ok_or_else(|| error!("List must a single child"))?
                        .try_into()?,
                )
                .into(),
            ),
            T::FixedSizeList(n) => DataType::FixedSizeList(
                Box::<Field>::new(
                    value
                        .children
                        .first()
                        .ok_or_else(|| error!("List must a single child"))?
                        .try_into()?,
                )
                .into(),
                *n,
            ),
            T::Binary => DataType::Binary,
            T::LargeBinary => DataType::LargeBinary,
            T::FixedSizeBinary(n) => DataType::FixedSizeBinary(*n),
            T::Struct => DataType::Struct(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<_>>()?,
            ),
            T::Map => {
                let element_field: Field = value
                    .children
                    .first()
                    .ok_or_else(|| error!("Map must a single child"))?
                    .try_into()?;
                DataType::Map(Box::new(element_field).into(), false)
            }
            T::Union => {
                let mut fields = Vec::new();
                for (idx, field) in value.children.iter().enumerate() {
                    fields.push((idx as i8, std::sync::Arc::new(Field::try_from(field)?)));
                }
                DataType::Union(fields.into_iter().collect(), UnionMode::Dense)
            }
            T::Dictionary => {
                let Some(key_field) = value.children.first() else {
                    fail!("Dictionary must a two children");
                };
                let val_field: Field = value
                    .children
                    .get(1)
                    .ok_or_else(|| error!("Dictionary must a two children"))?
                    .try_into()?;

                let key_type = match &key_field.data_type {
                    GenericDataType::U8 => DataType::UInt8,
                    GenericDataType::U16 => DataType::UInt16,
                    GenericDataType::U32 => DataType::UInt32,
                    GenericDataType::U64 => DataType::UInt64,
                    GenericDataType::I8 => DataType::Int8,
                    GenericDataType::I16 => DataType::Int16,
                    GenericDataType::I32 => DataType::Int32,
                    GenericDataType::I64 => DataType::Int64,
                    _ => fail!("Invalid key type for dictionary"),
                };

                DataType::Dictionary(Box::new(key_type), Box::new(val_field.data_type().clone()))
            }
            T::Time32(U::Second) => DataType::Time32(ArrowTimeUnit::Second),
            T::Time32(U::Millisecond) => DataType::Time32(ArrowTimeUnit::Millisecond),
            T::Time32(unit) => fail!("invalid time unit {unit} for Time32"),
            T::Time64(U::Microsecond) => DataType::Time64(ArrowTimeUnit::Microsecond),
            T::Time64(U::Nanosecond) => DataType::Time64(ArrowTimeUnit::Nanosecond),
            T::Time64(unit) => fail!("invalid time unit {unit} for Time64"),
            T::Timestamp(unit, tz) => {
                DataType::Timestamp((*unit).into(), tz.clone().map(|s| s.into()))
            }
            T::Duration(unit) => DataType::Duration((*unit).into()),
        };

        let metadata =
            merge_strategy_with_metadata(value.metadata.clone(), value.strategy.clone())?;

        let mut field = Field::new(&value.name, data_type, value.nullable);
        field.set_metadata(metadata);

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
