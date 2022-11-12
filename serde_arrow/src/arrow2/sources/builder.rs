use arrow2::{
    array::{Array, StructArray},
    datatypes::DataType,
    types::NativeType,
};

use crate::{
    error,
    event::{DynamicSource, Event},
    fail, Result,
};

use super::{
    primitive_sources::{BooleanEventSource, PrimitiveEventSource},
    struct_source::StructSource,
};

pub fn build_dynamic_source<'a>(array: &'a dyn Array) -> Result<DynamicSource<'a>> {
    let source = match array.data_type() {
        DataType::Int8 => build_dynamic_primitive_source::<i8>(array)?,
        DataType::Int16 => build_dynamic_primitive_source::<i16>(array)?,
        DataType::Int32 => build_dynamic_primitive_source::<i32>(array)?,
        DataType::Int64 => build_dynamic_primitive_source::<i64>(array)?,
        DataType::UInt8 => build_dynamic_primitive_source::<u8>(array)?,
        DataType::UInt16 => build_dynamic_primitive_source::<u16>(array)?,
        DataType::UInt32 => build_dynamic_primitive_source::<u32>(array)?,
        DataType::UInt64 => build_dynamic_primitive_source::<u64>(array)?,
        DataType::Float32 => build_dynamic_primitive_source::<f32>(array)?,
        DataType::Float64 => build_dynamic_primitive_source::<f64>(array)?,
        DataType::Boolean => DynamicSource::new(BooleanEventSource::new(
            array
                .as_any()
                .downcast_ref()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::Struct(_) => build_dynamic_struct_source(array)?,
        dt => fail!("{dt:?} not yet supported"),
    };
    Ok(source)
}

pub fn build_dynamic_primitive_source<'a, T: Into<Event<'static>> + NativeType>(
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let source = PrimitiveEventSource::<'a, T>::new(
        array
            .as_any()
            .downcast_ref()
            .ok_or_else(|| error!("Mismatched type"))?,
    );
    Ok(DynamicSource::new(source))
}

pub fn build_dynamic_struct_source<'a>(array: &'a dyn Array) -> Result<DynamicSource<'a>> {
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| error!("mismatched type"))?;

    let fields = array.fields();
    let values: Result<Vec<DynamicSource<'_>>> = array
        .values()
        .iter()
        .map(|array| build_dynamic_source(array.as_ref()))
        .collect();
    let values = values?;

    let source = StructSource::new(fields, values);

    Ok(DynamicSource::new(source))
}
