use crate::{
    internal::{
        common::BitBuffer,
        deserialization::date64_deserializer::Date64Deserializer,
        error::{fail, Result},
        schema::{GenericDataType, GenericField},
    },
    schema::Strategy,
};

use super::{array_deserializer::ArrayDeserializer, time_deserializer::TimeDeserializer};

pub fn build_timestamp_deserializer<'a>(
    field: &GenericField,
    values: &'a [i64],
    validity: Option<BitBuffer<'a>>,
) -> Result<ArrayDeserializer<'a>> {
    let strategy = field.strategy.as_ref();
    let GenericDataType::Timestamp(unit, _) = &field.data_type else {
        fail!(
            "invalid data type for timestamp deserializer: {dt}",
            dt = field.data_type
        );
    };

    if matches!(
        strategy,
        Some(Strategy::NaiveStrAsDate64 | Strategy::UtcStrAsDate64)
    ) {
        return Ok(Date64Deserializer::new(values, validity, *unit, field.is_utc()?).into());
    }

    if let Some(strategy) = strategy {
        fail!("invalid strategy {strategy} for timestamp field");
    }

    Ok(Date64Deserializer::new(values, validity, *unit, field.is_utc()?).into())
}

pub fn build_time32_deserializer<'a>(
    field: &GenericField,
    values: &'a [i32],
    validity: Option<BitBuffer<'a>>,
) -> Result<ArrayDeserializer<'a>> {
    let GenericDataType::Time32(unit) = &field.data_type else {
        fail!("invalid data type for time64");
    };

    Ok(TimeDeserializer::<i32>::new(values, validity, *unit).into())
}

pub fn build_time64_deserializer<'a>(
    field: &GenericField,
    values: &'a [i64],
    validity: Option<BitBuffer<'a>>,
) -> Result<ArrayDeserializer<'a>> {
    let GenericDataType::Time64(unit) = &field.data_type else {
        fail!("invalid data type for time64");
    };

    Ok(TimeDeserializer::<i64>::new(values, validity, *unit).into())
}
