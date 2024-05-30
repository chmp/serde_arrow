use crate::{
    _impl::arrow2::datatypes::{DataType, Field, IntegerType, TimeUnit, UnionMode},
    internal::{
        error::{error, fail, Error, Result},
        schema::{
            GenericDataType, GenericField, GenericTimeUnit, SchemaLike, Sealed, SerdeArrowSchema,
            Strategy, STRATEGY_KEY,
        },
    },
};

/// Support for arrow2 types (*requires one of the `arrow2-*` features*)
impl SerdeArrowSchema {
    /// Build a new Schema object from fields
    pub fn from_arrow2_fields(fields: &[Field]) -> Result<Self> {
        Self::try_from(fields)
    }

    /// This method is deprecated. Use
    /// [`to_arrow2_fields`][SerdeArrowSchema::to_arrow2_fields] instead:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::schema::{SerdeArrowSchema, SchemaLike, TracingOptions};
    /// # #[derive(serde::Deserialize)]
    /// # struct Item { a: u32 }
    /// # let schema = SerdeArrowSchema::from_type::<Item>(TracingOptions::default()).unwrap();
    /// # let fields =
    /// schema.to_arrow2_fields()?
    /// # ;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated = "The method `get_arrow2_fields` is deprecated. Use `to_arrow2_fields` instead"]
    pub fn get_arrow2_fields(&self) -> Result<Vec<Field>> {
        Vec::<Field>::try_from(self)
    }

    /// Build a vec of fields from a  Schema object
    pub fn to_arrow2_fields(&self) -> Result<Vec<Field>> {
        Vec::<Field>::try_from(self)
    }
}

impl TryFrom<SerdeArrowSchema> for Vec<Field> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        Vec::<Field>::try_from(&value)
    }
}

impl<'a> TryFrom<&'a SerdeArrowSchema> for Vec<Field> {
    type Error = Error;

    fn try_from(value: &'a SerdeArrowSchema) -> Result<Self> {
        value.fields.iter().map(Field::try_from).collect()
    }
}

impl<'a> TryFrom<&'a [Field]> for SerdeArrowSchema {
    type Error = Error;

    fn try_from(fields: &'a [Field]) -> std::prelude::v1::Result<Self, Self::Error> {
        Ok(Self {
            fields: fields
                .iter()
                .map(GenericField::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl Sealed for Vec<Field> {}

/// Schema support for `Vec<arrow2::datatype::Field>` (*requires one of the
/// `arrow2-*` features*)
impl SchemaLike for Vec<Field> {
    fn from_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.to_arrow2_fields()
    }

    fn from_type<'de, T: serde::Deserialize<'de> + ?Sized>(
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.to_arrow2_fields()
    }

    fn from_samples<T: serde::Serialize + ?Sized>(
        samples: &T,
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.to_arrow2_fields()
    }
}

impl TryFrom<&Field> for GenericField {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self> {
        use {GenericDataType as T, GenericTimeUnit as U};

        let strategy: Option<Strategy> = match field.metadata.get(STRATEGY_KEY) {
            Some(strategy_str) => Some(strategy_str.parse::<Strategy>()?),
            None => None,
        };
        let name = field.name.to_owned();
        let nullable = field.is_nullable;

        let mut children = Vec::<GenericField>::new();
        let data_type = match &field.data_type {
            DataType::Boolean => T::Bool,
            DataType::Null => T::Null,
            DataType::Int8 => T::I8,
            DataType::Int16 => T::I16,
            DataType::Int32 => T::I32,
            DataType::Int64 => T::I64,
            DataType::UInt8 => T::U8,
            DataType::UInt16 => T::U16,
            DataType::UInt32 => T::U32,
            DataType::UInt64 => T::U64,
            DataType::Float16 => T::F16,
            DataType::Float32 => T::F32,
            DataType::Float64 => T::F64,
            DataType::Utf8 => T::Utf8,
            DataType::LargeUtf8 => T::LargeUtf8,
            DataType::Date32 => T::Date32,
            DataType::Date64 => T::Date64,
            DataType::Decimal(precision, scale) => {
                if *precision > u8::MAX as usize || *scale > i8::MAX as usize {
                    fail!("cannot represent precision / scale of the decimal");
                }
                T::Decimal128(*precision as u8, *scale as i8)
            }
            DataType::Time32(TimeUnit::Second) => T::Time32(U::Second),
            DataType::Time32(TimeUnit::Millisecond) => T::Time32(U::Millisecond),
            DataType::Time32(unit) => fail!("Invalid time unit {unit:?} for Time32"),
            DataType::Time64(TimeUnit::Microsecond) => T::Time64(U::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond) => T::Time64(U::Nanosecond),
            DataType::Time64(unit) => fail!("Invalid time unit {unit:?} for Time64"),
            DataType::Timestamp(TimeUnit::Second, tz) => T::Timestamp(U::Second, tz.clone()),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                T::Timestamp(U::Millisecond, tz.clone())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                T::Timestamp(U::Microsecond, tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                T::Timestamp(U::Nanosecond, tz.clone())
            }
            DataType::Duration(TimeUnit::Second) => T::Duration(U::Second),
            DataType::Duration(TimeUnit::Millisecond) => T::Duration(U::Millisecond),
            DataType::Duration(TimeUnit::Microsecond) => T::Duration(U::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond) => T::Duration(U::Nanosecond),
            DataType::List(field) => {
                children.push(GenericField::try_from(field.as_ref())?);
                T::List
            }
            DataType::LargeList(field) => {
                children.push(field.as_ref().try_into()?);
                T::LargeList
            }
            DataType::Struct(fields) => {
                for field in fields {
                    children.push(field.try_into()?);
                }
                T::Struct
            }
            DataType::Map(field, _) => {
                children.push(field.as_ref().try_into()?);
                T::Map
            }
            DataType::Union(fields, field_indices, mode) => {
                if field_indices.is_some() {
                    fail!("Union types with explicit field indices are not supported");
                }
                if !mode.is_dense() {
                    fail!("Only dense unions are supported at the moment");
                }

                for field in fields {
                    children.push(field.try_into()?);
                }
                T::Union
            }
            DataType::Dictionary(int_type, data_type, sorted) => {
                if *sorted {
                    fail!("Sorted dictionary are not supported");
                }
                let key_type = match int_type {
                    IntegerType::Int8 => DataType::Int8,
                    IntegerType::Int16 => DataType::Int16,
                    IntegerType::Int32 => DataType::Int32,
                    IntegerType::Int64 => DataType::Int64,
                    IntegerType::UInt8 => DataType::UInt8,
                    IntegerType::UInt16 => DataType::UInt16,
                    IntegerType::UInt32 => DataType::UInt32,
                    IntegerType::UInt64 => DataType::UInt64,
                };
                children.push((&Field::new("", key_type, false)).try_into()?);
                children.push((&Field::new("", data_type.as_ref().clone(), false)).try_into()?);
                T::Dictionary
            }
            dt => fail!("Cannot convert data type {dt:?}"),
        };

        let field = GenericField {
            data_type,
            name,
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
        use {GenericDataType as T, GenericTimeUnit as U};

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
            T::Time32(U::Second) => DataType::Time32(TimeUnit::Second),
            T::Time32(U::Millisecond) => DataType::Time32(TimeUnit::Millisecond),
            T::Time32(unit) => fail!("Invalid time unit {unit} for Time32"),
            T::Time64(U::Microsecond) => DataType::Time64(TimeUnit::Microsecond),
            T::Time64(U::Nanosecond) => DataType::Time64(TimeUnit::Nanosecond),
            T::Time64(unit) => fail!("Invalid time unit {unit} for Time64"),
            T::Timestamp(unit, tz) => DataType::Timestamp((*unit).into(), tz.clone()),
            T::Duration(unit) => DataType::Duration((*unit).into()),
            T::Decimal128(precision, scale) => {
                if *scale < 0 {
                    fail!("arrow2 does not support decimals with negative scale");
                }
                DataType::Decimal(*precision as usize, *scale as usize)
            }
            T::Utf8 => DataType::Utf8,
            T::LargeUtf8 => DataType::LargeUtf8,
            T::List => DataType::List(Box::new(
                value
                    .children
                    .first()
                    .ok_or_else(|| error!("List must a single child"))?
                    .try_into()?,
            )),
            T::LargeList => DataType::LargeList(Box::new(
                value
                    .children
                    .first()
                    .ok_or_else(|| error!("List must a single child"))?
                    .try_into()?,
            )),
            T::Struct => DataType::Struct(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ),
            T::Map => {
                let element_field: Field = value
                    .children
                    .first()
                    .ok_or_else(|| error!("Map must a two children"))?
                    .try_into()?;
                DataType::Map(Box::new(element_field), false)
            }
            T::Union => DataType::Union(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?,
                None,
                UnionMode::Dense,
            ),
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
                    T::U8 => IntegerType::UInt8,
                    T::U16 => IntegerType::UInt16,
                    T::U32 => IntegerType::UInt32,
                    T::U64 => IntegerType::UInt64,
                    T::I8 => IntegerType::Int8,
                    T::I16 => IntegerType::Int16,
                    T::I32 => IntegerType::Int32,
                    T::I64 => IntegerType::Int64,
                    _ => fail!("Invalid key type for dictionary"),
                };

                DataType::Dictionary(key_type, Box::new(val_field.data_type), false)
            }
        };

        let mut field = Field::new(&value.name, data_type, value.nullable);
        if let Some(strategy) = value.strategy.as_ref() {
            field.metadata = strategy.clone().into();
        }

        Ok(field)
    }
}

impl From<GenericTimeUnit> for TimeUnit {
    fn from(value: GenericTimeUnit) -> Self {
        match value {
            GenericTimeUnit::Second => Self::Second,
            GenericTimeUnit::Millisecond => Self::Millisecond,
            GenericTimeUnit::Microsecond => Self::Microsecond,
            GenericTimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}
