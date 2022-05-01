use arrow2::datatypes::{DataType as Arrow2DataType, Field, Schema as Arrow2Schema};

use super::{DataType, Schema};
use crate::{fail, Error, Result};

impl std::convert::TryFrom<&DataType> for Arrow2DataType {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        use Arrow2DataType::*;
        match value {
            DataType::Bool => Ok(Boolean),
            DataType::I8 => Ok(Int8),
            DataType::I16 => Ok(Int16),
            DataType::I32 => Ok(Int32),
            DataType::I64 => Ok(Int64),
            DataType::U8 => Ok(UInt8),
            DataType::U16 => Ok(UInt16),
            DataType::U32 => Ok(UInt32),
            DataType::U64 => Ok(UInt64),
            DataType::F32 => Ok(Float32),
            DataType::F64 => Ok(Float64),
            DataType::DateTimeStr | DataType::NaiveDateTimeStr | DataType::DateTimeMilliseconds => {
                Ok(Date64)
            }
            DataType::Str => Ok(Utf8),
            DataType::Arrow2(res) => Ok(res.clone()),
            #[allow(unreachable_patterns)]
            dt => fail!("Cannot convert {dt} to an arrow data type"),
        }
    }
}

impl From<Arrow2DataType> for DataType {
    fn from(dt: Arrow2DataType) -> Self {
        use Arrow2DataType::*;
        match dt {
            Boolean => Self::Bool,
            UInt8 => Self::U8,
            UInt16 => Self::U16,
            UInt32 => Self::U32,
            UInt64 => Self::U64,
            Int8 => Self::I8,
            Int16 => Self::I16,
            Int32 => Self::I32,
            Int64 => Self::I64,
            Float32 => Self::F32,
            Float64 => Self::F64,
            Utf8 => Self::Str,
            dt => Self::Arrow2(dt),
        }
    }
}

impl From<&Arrow2DataType> for DataType {
    fn from(value: &Arrow2DataType) -> Self {
        value.clone().into()
    }
}

impl std::convert::TryFrom<&Schema> for Arrow2Schema {
    type Error = Error;

    fn try_from(value: &Schema) -> Result<Self, Self::Error> {
        let mut fields = Vec::new();

        for field in &value.fields {
            let data_type = value
                .data_type
                .get(field)
                .ok_or_else(|| Error::Custom(format!("No data type detected for {}", field)))?;
            let nullable = value.nullable.contains(field);

            let field = Field::new(field, Arrow2DataType::try_from(data_type)?, nullable);
            fields.push(field);
        }

        let schema = Arrow2Schema::from(fields);
        Ok(schema)
    }
}

impl std::convert::TryFrom<&Arrow2Schema> for Schema {
    type Error = Error;

    fn try_from(value: &Arrow2Schema) -> Result<Self> {
        let mut res = Schema::new();

        for field in &value.fields {
            res.add_field(
                &field.name,
                Some(DataType::from(field.data_type())),
                Some(field.is_nullable),
            );
        }

        Ok(res)
    }
}
