use super::type_support::FieldRef;
use crate::{
    _impl::arrow::datatypes::{DataType, Field, TimeUnit, UnionMode},
    internal::{
        error::{error, fail, Error, Result},
        schema::{
            GenericDataType, GenericField, GenericTimeUnit, SchemaLike, Sealed, SerdeArrowSchema,
            Strategy, STRATEGY_KEY,
        },
    },
};

/// Support for arrow types (*requires one of the `arrow-*` features*)
impl SerdeArrowSchema {
    /// Build a new Schema object from fields
    pub fn from_arrow_fields(fields: &[Field]) -> Result<Self> {
        Ok(Self {
            fields: fields
                .iter()
                .map(GenericField::try_from)
                .collect::<Result<_>>()?,
        })
    }

    /// This method is deprecated. Use
    /// [`to_arrow_fields`][SerdeArrowSchema::to_arrow_fields] instead:
    ///
    /// ```rust
    /// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
    /// # use serde_arrow::schema::{SerdeArrowSchema, SchemaLike, TracingOptions};
    /// # #[derive(serde::Deserialize)]
    /// # struct Item { a: u32 }
    /// # let schema = SerdeArrowSchema::from_type::<Item>(TracingOptions::default()).unwrap();
    /// # let fields =
    /// schema.to_arrow_fields()?
    /// # ;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated = "The method `get_arrow_fields` is deprecated. Use `to_arrow_fields` instead"]
    pub fn get_arrow_fields(&self) -> Result<Vec<Field>> {
        self.to_arrow_fields()
    }

    /// Build a vec of fields from a Schema object
    pub fn to_arrow_fields(&self) -> Result<Vec<Field>> {
        self.fields.iter().map(Field::try_from).collect()
    }
}

impl TryFrom<SerdeArrowSchema> for Vec<Field> {
    type Error = Error;

    fn try_from(value: SerdeArrowSchema) -> Result<Self> {
        value.to_arrow_fields()
    }
}

impl Sealed for Vec<Field> {}

/// Schema support for `Vec<arrow::datatype::Field>` (*requires one of the
/// `arrow-*` features*)
impl SchemaLike for Vec<Field> {
    fn from_value<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self> {
        SerdeArrowSchema::from_value(value)?.to_arrow_fields()
    }

    fn from_type<'de, T: serde::Deserialize<'de> + ?Sized>(
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_type::<T>(options)?.to_arrow_fields()
    }

    fn from_samples<T: serde::Serialize + ?Sized>(
        samples: &T,
        options: crate::schema::TracingOptions,
    ) -> Result<Self> {
        SerdeArrowSchema::from_samples(samples, options)?.to_arrow_fields()
    }
}

impl TryFrom<&DataType> for GenericDataType {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<GenericDataType> {
        match value {
            DataType::Boolean => Ok(GenericDataType::Bool),
            DataType::Null => Ok(GenericDataType::Null),
            DataType::Int8 => Ok(GenericDataType::I8),
            DataType::Int16 => Ok(GenericDataType::I16),
            DataType::Int32 => Ok(GenericDataType::I32),
            DataType::Int64 => Ok(GenericDataType::I64),
            DataType::UInt8 => Ok(GenericDataType::U8),
            DataType::UInt16 => Ok(GenericDataType::U16),
            DataType::UInt32 => Ok(GenericDataType::U32),
            DataType::UInt64 => Ok(GenericDataType::U64),
            DataType::Float16 => Ok(GenericDataType::F16),
            DataType::Float32 => Ok(GenericDataType::F32),
            DataType::Float64 => Ok(GenericDataType::F64),
            DataType::Utf8 => Ok(GenericDataType::Utf8),
            DataType::LargeUtf8 => Ok(GenericDataType::LargeUtf8),
            DataType::Date64 => Ok(GenericDataType::Date64),
            DataType::Decimal128(precision, scale) => {
                Ok(GenericDataType::Decimal128(*precision, *scale))
            }
            DataType::Timestamp(TimeUnit::Second, tz) => Ok(GenericDataType::Timestamp(
                GenericTimeUnit::Second,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => Ok(GenericDataType::Timestamp(
                GenericTimeUnit::Millisecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Ok(GenericDataType::Timestamp(
                GenericTimeUnit::Microsecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => Ok(GenericDataType::Timestamp(
                GenericTimeUnit::Nanosecond,
                tz.as_ref().map(|s| s.to_string()),
            )),
            _ => fail!("Only primitive data types can be converted to GenericDataType"),
        }
    }
}

impl TryFrom<&Field> for GenericField {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self> {
        let strategy: Option<Strategy> = match field.metadata().get(STRATEGY_KEY) {
            Some(strategy_str) => Some(strategy_str.parse::<Strategy>()?),
            None => None,
        };
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
            DataType::Struct(fields) => {
                for field in fields {
                    children.push(field.as_field_ref().try_into()?);
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
                        fail!("Union types with explicit field indices are not supported");
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
        let data_type = match &value.data_type {
            GenericDataType::Null => DataType::Null,
            GenericDataType::Bool => DataType::Boolean,
            GenericDataType::I8 => DataType::Int8,
            GenericDataType::I16 => DataType::Int16,
            GenericDataType::I32 => DataType::Int32,
            GenericDataType::I64 => DataType::Int64,
            GenericDataType::U8 => DataType::UInt8,
            GenericDataType::U16 => DataType::UInt16,
            GenericDataType::U32 => DataType::UInt32,
            GenericDataType::U64 => DataType::UInt64,
            GenericDataType::F16 => DataType::Float16,
            GenericDataType::F32 => DataType::Float32,
            GenericDataType::F64 => DataType::Float64,
            GenericDataType::Date64 => DataType::Date64,
            GenericDataType::Decimal128(precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            GenericDataType::Utf8 => DataType::Utf8,
            GenericDataType::LargeUtf8 => DataType::LargeUtf8,
            GenericDataType::List => DataType::List(
                Box::<Field>::new(
                    value
                        .children
                        .first()
                        .ok_or_else(|| error!("List must a single child"))?
                        .try_into()?,
                )
                .into(),
            ),
            GenericDataType::LargeList => DataType::LargeList(
                Box::<Field>::new(
                    value
                        .children
                        .first()
                        .ok_or_else(|| error!("List must a single child"))?
                        .try_into()?,
                )
                .into(),
            ),
            GenericDataType::Struct => DataType::Struct(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<_>>()?,
            ),
            GenericDataType::Map => {
                let element_field: Field = value
                    .children
                    .first()
                    .ok_or_else(|| error!("Map must a single child"))?
                    .try_into()?;
                DataType::Map(Box::new(element_field).into(), false)
            }
            #[cfg(not(feature = "arrow-36"))]
            GenericDataType::Union => {
                let mut fields = Vec::new();
                for (idx, field) in value.children.iter().enumerate() {
                    fields.push((idx as i8, std::sync::Arc::new(Field::try_from(field)?)));
                }
                DataType::Union(fields.into_iter().collect(), UnionMode::Dense)
            }
            #[cfg(feature = "arrow-36")]
            GenericDataType::Union => DataType::Union(
                value
                    .children
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<Vec<_>>>()?,
                (0..value.children.len())
                    .into_iter()
                    .map(|v| v as i8)
                    .collect(),
                UnionMode::Dense,
            ),
            GenericDataType::Dictionary => {
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
            GenericDataType::Timestamp(GenericTimeUnit::Second, tz) => {
                DataType::Timestamp(TimeUnit::Second, tz.clone().map(|s| s.into()))
            }
            GenericDataType::Timestamp(GenericTimeUnit::Millisecond, tz) => {
                DataType::Timestamp(TimeUnit::Millisecond, tz.clone().map(|s| s.into()))
            }
            GenericDataType::Timestamp(GenericTimeUnit::Microsecond, tz) => {
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone().map(|s| s.into()))
            }
            GenericDataType::Timestamp(GenericTimeUnit::Nanosecond, tz) => {
                DataType::Timestamp(TimeUnit::Nanosecond, tz.clone().map(|s| s.into()))
            }
        };

        let mut field = Field::new(&value.name, data_type, value.nullable);
        if let Some(strategy) = value.strategy.as_ref() {
            field.set_metadata(strategy.clone().into());
        }

        Ok(field)
    }
}
