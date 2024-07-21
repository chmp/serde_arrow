use crate::internal::{
    deserialization::date64_deserializer::Date64Deserializer,
    error::{fail, Result},
    schema::{GenericDataType, GenericField, Strategy},
};

use super::{
    array_deserializer::ArrayDeserializer, utils::BitBuffer,
};

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

