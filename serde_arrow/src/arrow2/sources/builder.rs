use arrow2::{
    array::{Array, StructArray},
    datatypes::{DataType, Field},
    types::NativeType,
};

use crate::{
    arrow2::RecordSource,
    error,
    event::{DynamicSource, Event},
    fail, Result,
};

use super::{
    primitive_sources::{BooleanEventSource, PrimitiveEventSource},
    struct_source::StructSource,
};

pub fn build_record_source<'a>(
    fields: &'a [Field],
    arrays: &[&'a dyn Array],
) -> Result<RecordSource<'a, DynamicSource<'a>>> {
    let mut columns = Vec::new();
    let mut sources = Vec::new();

    for i in 0..fields.len() {
        columns.push(fields[i].name.as_str());
        sources.push(build_dynamic_source(fields[i].data_type(), arrays[i])?);
    }

    Ok(RecordSource::new(columns, sources))
}

pub fn build_dynamic_source<'a>(
    data_type: &'a DataType,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let source = match data_type {
        DataType::Int8 => build_dynamic_primitive_source::<i8>(data_type, array)?,
        DataType::Int16 => build_dynamic_primitive_source::<i16>(data_type, array)?,
        DataType::Int32 => build_dynamic_primitive_source::<i32>(data_type, array)?,
        DataType::Int64 => build_dynamic_primitive_source::<i64>(data_type, array)?,
        DataType::UInt8 => build_dynamic_primitive_source::<u8>(data_type, array)?,
        DataType::UInt16 => build_dynamic_primitive_source::<u16>(data_type, array)?,
        DataType::UInt32 => build_dynamic_primitive_source::<u32>(data_type, array)?,
        DataType::UInt64 => build_dynamic_primitive_source::<u64>(data_type, array)?,
        DataType::Float32 => build_dynamic_primitive_source::<f32>(data_type, array)?,
        DataType::Float64 => build_dynamic_primitive_source::<f64>(data_type, array)?,
        DataType::Boolean => DynamicSource::new(BooleanEventSource::new(
            array
                .as_any()
                .downcast_ref()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::Struct(fields) => build_dynamic_struct_source(fields, array)?,
        dt => fail!("{dt:?} not yet supported"),
    };
    Ok(source)
}

pub fn build_dynamic_primitive_source<'a, T: Into<Event<'static>> + NativeType>(
    data_type: &'a DataType,
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

pub fn build_dynamic_struct_source<'a>(
    fields: &'a [Field],
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| error!("mismatched type"))?;

    let fields = fields.iter().map(|field| field.name.as_str()).collect();
    let values: Result<Vec<DynamicSource<'_>>> = array
        .values()
        .iter()
        .map(|array| build_dynamic_source(array.data_type(), array.as_ref()))
        .collect();
    let values = values?;

    let source = StructSource::new(fields, values);

    Ok(DynamicSource::new(source))
}
