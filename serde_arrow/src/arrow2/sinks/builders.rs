use arrow2::datatypes::DataType;

use crate::{fail, Result};

use super::{base::DynamicArrayBuilder, primitive_sinks::{BooleanArrayBuilder, PrimitiveArrayBuilder}, struct_sink::StructArrayBuilder};


pub fn build_dynamic_array_builder(dt: &DataType) -> Result<DynamicArrayBuilder> {
    match dt {
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
        DataType::Struct(fields) => {
            let mut columns = Vec::new();
            let mut builders = Vec::new();
            let mut nullable = Vec::new();

            for field in fields {
                columns.push(field.name.to_owned());
                builders.push(build_dynamic_array_builder(field.data_type())?);
                nullable.push(field.is_nullable);
            }

            let builder = StructArrayBuilder::new(columns, builders, nullable);
            Ok(DynamicArrayBuilder::new(builder))
        }
        _ => fail!("Cannot build sink for {dt:?}"),
    }
}