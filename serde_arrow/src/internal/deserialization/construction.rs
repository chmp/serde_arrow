use crate::{
    internal::{
        common::BitBuffer,
        deserialization::{
            date64_deserializer::Date64Deserializer, integer_deserializer::IntegerDeserializer,
        },
        error::{fail, Result},
        schema::{GenericDataType, GenericField, GenericTimeUnit},
    },
    schema::Strategy,
};

use super::{array_deserializer::ArrayDeserializer, time64_deserializer::Time64Deserializer};

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
        if !matches!(unit, GenericTimeUnit::Millisecond) {
            let strategy = strategy.unwrap();
            fail!(
                "invalid unit {unit} for timestamp with strategy {strategy}, must be Millisecond"
            );
        }

        return Ok(Date64Deserializer::new(values, validity, field.is_utc()?).into());
    }

    if let Some(strategy) = strategy {
        fail!("invalid strategy {strategy} for timestamp field");
    }

    match unit {
        GenericTimeUnit::Millisecond => {
            Ok(Date64Deserializer::new(values, validity, field.is_utc()?).into())
        }
        _ => Ok(IntegerDeserializer::new(values, validity).into()),
    }
}

pub fn build_time64_deserializer<'a>(
    field: &GenericField,
    values: &'a [i64],
    validity: Option<BitBuffer<'a>>,
) -> Result<ArrayDeserializer<'a>> {
    let GenericDataType::Time64(unit) = &field.data_type else {
        fail!("invalid data type for time64");
    };

    Ok(Time64Deserializer::new(values, validity, unit.clone()).into())
}
