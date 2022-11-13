use arrow2::{
    array::{Array, StructArray},
    datatypes::{DataType, Field},
    types::NativeType,
};

use crate::{
    arrow2::{schema::STRATEGY_KEY, Strategy},
    error,
    event::{DynamicSource, Event},
    fail, Result,
};

use super::{
    primitive_sources::{
        BooleanEventSource, DateTimeStrSource, NaiveDateTimeStrSource, PrimitiveEventSource,
    },
    record_source::RecordSource,
    struct_source::StructSource,
};

pub fn build_record_source<'a, A: AsRef<dyn Array> + 'a>(
    fields: &'a [Field],
    arrays: &'a [A],
) -> Result<RecordSource<'a, DynamicSource<'a>>> {
    let mut columns = Vec::new();
    let mut sources = Vec::new();

    for i in 0..fields.len() {
        columns.push(fields[i].name.as_str());
        sources.push(build_dynamic_source(&fields[i], arrays[i].as_ref())?);
    }

    Ok(RecordSource::new(columns, sources))
}

pub fn build_dynamic_source<'a>(
    field: &'a Field,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let source = match field.data_type() {
        DataType::Int8 => build_dynamic_primitive_source::<i8>(field, array)?,
        DataType::Int16 => build_dynamic_primitive_source::<i16>(field, array)?,
        DataType::Int32 => build_dynamic_primitive_source::<i32>(field, array)?,
        DataType::Int64 => build_dynamic_primitive_source::<i64>(field, array)?,
        DataType::UInt8 => build_dynamic_primitive_source::<u8>(field, array)?,
        DataType::UInt16 => build_dynamic_primitive_source::<u16>(field, array)?,
        DataType::UInt32 => build_dynamic_primitive_source::<u32>(field, array)?,
        DataType::UInt64 => build_dynamic_primitive_source::<u64>(field, array)?,
        DataType::Float32 => build_dynamic_primitive_source::<f32>(field, array)?,
        DataType::Float64 => build_dynamic_primitive_source::<f64>(field, array)?,
        DataType::Boolean => DynamicSource::new(BooleanEventSource::new(
            array
                .as_any()
                .downcast_ref()
                .ok_or_else(|| error!("mismatched types"))?,
        )),
        DataType::Date64 => {
            if let Some(strategy) = field.metadata.get(STRATEGY_KEY) {
                let strategy: Strategy = strategy.parse()?;
                match strategy {
                    Strategy::NaiveDateTimeStr => DynamicSource::new(NaiveDateTimeStrSource(
                        PrimitiveEventSource::<i64>::from_array(array)?,
                    )),
                    Strategy::DateTimeStr => DynamicSource::new(DateTimeStrSource(
                        PrimitiveEventSource::<i64>::from_array(array)?,
                    )),
                }
            } else {
                build_dynamic_primitive_source::<i64>(field, array)?
            }
        }
        DataType::Struct(fields) => build_dynamic_struct_source(fields, array)?,
        dt => fail!("{dt:?} not yet supported"),
    };
    Ok(source)
}

pub fn build_dynamic_primitive_source<'a, T: Into<Event<'static>> + NativeType>(
    field: &'a Field,
    array: &'a dyn Array,
) -> Result<DynamicSource<'a>> {
    let source =
        PrimitiveEventSource::<'a, T>::new(array.as_any().downcast_ref().ok_or_else(|| {
            error!(
                "Mismatched type. Expected {:?}, found: {:?}",
                field.data_type,
                array.data_type()
            )
        })?);
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
    let children = array.values();

    let mut names: Vec<&'a str> = Vec::new();
    let mut values: Vec<DynamicSource<'a>> = Vec::new();

    for i in 0..fields.len() {
        names.push(fields[i].name.as_str());
        values.push(build_dynamic_source(&fields[i], children[i].as_ref())?);
    }

    let source = StructSource::new(names, values);

    Ok(DynamicSource::new(source))
}
