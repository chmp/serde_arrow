/// Arrow support for schema operations
use std::convert::TryFrom;

use arrow::{
    datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

use super::{DataType, Schema};
use crate::{Error, Result, fail};

impl Schema {
    pub fn from_record_batch(record_batch: &RecordBatch) -> Result<Self> {
        record_batch.schema().as_ref().try_into()
    }

    pub fn build_arrow_schema(&self) -> Result<ArrowSchema> {
        ArrowSchema::try_from(self)
    }
}

impl std::convert::TryFrom<&DataType> for ArrowType {
    type Error = Error;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Bool => Ok(ArrowType::Boolean),
            DataType::I8 => Ok(ArrowType::Int8),
            DataType::I16 => Ok(ArrowType::Int16),
            DataType::I32 => Ok(ArrowType::Int32),
            DataType::I64 => Ok(ArrowType::Int64),
            DataType::U8 => Ok(ArrowType::UInt8),
            DataType::U16 => Ok(ArrowType::UInt16),
            DataType::U32 => Ok(ArrowType::UInt32),
            DataType::U64 => Ok(ArrowType::UInt64),
            DataType::F32 => Ok(ArrowType::Float32),
            DataType::F64 => Ok(ArrowType::Float64),
            DataType::DateTimeStr | DataType::NaiveDateTimeStr | DataType::DateTimeMilliseconds => {
                Ok(ArrowType::Date64)
            }
            DataType::Str => Ok(ArrowType::Utf8),
            DataType::Arrow(res) => Ok(res.clone()),
            #[allow(unreachable_patterns)]
            dt => fail!("Cannot convert {dt:?} to an arrow data type"),
        }
    }
}

impl From<ArrowType> for DataType {
    fn from(value: ArrowType) -> Self {
        use ArrowType::*;
        match value {
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
            dt => Self::Arrow(dt),
        }
    }
}

impl From<&ArrowType> for DataType {
    fn from(value: &ArrowType) -> Self {
        value.clone().into()
    }
}

impl std::convert::TryFrom<&Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(value: &Schema) -> Result<Self, Self::Error> {
        let mut fields = Vec::new();

        for field in &value.fields {
            let data_type = value
                .data_type
                .get(field)
                .ok_or_else(|| Error::Custom(format!("No data type detected for {}", field)))?;
            let nullable = value.nullable.contains(field);

            let field = Field::new(field, ArrowType::try_from(data_type)?, nullable);
            fields.push(field);
        }

        let schema = ArrowSchema::new(fields);
        Ok(schema)
    }
}

impl std::convert::TryFrom<&ArrowSchema> for Schema {
    type Error = Error;

    fn try_from(value: &ArrowSchema) -> Result<Self> {
        let mut res = Schema::new();

        for field in value.fields() {
            res.add_field(
                field.name(),
                Some(DataType::from(field.data_type())),
                Some(field.is_nullable()),
            );
        }

        Ok(res)
    }
}
