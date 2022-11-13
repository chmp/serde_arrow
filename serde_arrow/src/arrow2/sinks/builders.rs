use arrow2::datatypes::{DataType, Field};

use crate::{
    arrow2::{schema::STRATEGY_KEY, Strategy},
    fail, Result,
};

use super::{
    base::DynamicArrayBuilder,
    primitive_sinks::{
        BooleanArrayBuilder, DateTimeStrBuilder, NaiveDateTimeStrBuilder, PrimitiveArrayBuilder,
    },
    struct_sink::StructArrayBuilder,
};

pub fn build_dynamic_array_builder(field: &Field) -> Result<DynamicArrayBuilder> {
    match field.data_type() {
        DataType::Boolean => Ok(DynamicArrayBuilder::new(BooleanArrayBuilder::new())),
        DataType::Int8 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i8>::new())),
        DataType::Int16 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i16>::new())),
        DataType::Int32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i32>::new())),
        DataType::Int64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i64>::new())),
        DataType::UInt8 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u8>::new())),
        DataType::UInt16 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u16>::new())),
        DataType::UInt32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u32>::new())),
        DataType::UInt64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<u64>::new())),
        DataType::Float32 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<f32>::new())),
        DataType::Float64 => Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<f64>::new())),
        DataType::Date64 => {
            if let Some(strategy) = field.metadata.get(STRATEGY_KEY) {
                let strategy: Strategy = strategy.parse()?;
                match strategy {
                    Strategy::NaiveDateTimeStr => {
                        Ok(DynamicArrayBuilder::new(NaiveDateTimeStrBuilder::default()))
                    }
                    Strategy::DateTimeStr => {
                        Ok(DynamicArrayBuilder::new(DateTimeStrBuilder::default()))
                    }
                }
            } else {
                // TODO: is this correct?
                Ok(DynamicArrayBuilder::new(PrimitiveArrayBuilder::<i64>::new()))
            }
        }
        DataType::Struct(fields) => {
            let mut columns = Vec::new();
            let mut builders = Vec::new();
            let mut nullable = Vec::new();

            for field in fields {
                columns.push(field.name.to_owned());
                builders.push(build_dynamic_array_builder(field)?);
                nullable.push(field.is_nullable);
            }

            let builder = StructArrayBuilder::new(columns, builders, nullable);
            Ok(DynamicArrayBuilder::new(builder))
        }
        _ => fail!(
            "Cannot build sink for {} with type {:?}",
            field.name,
            field.data_type
        ),
    }
}
