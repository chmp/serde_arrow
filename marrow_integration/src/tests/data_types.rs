use arrow_schema::{DataType as AD, TimeUnit as AU};
use marrow::datatypes::{DataType, TimeUnit};

use super::utils::PanicOnError;

fn assert_symmetric_conversion(arrow: AD, marrow: DataType) -> PanicOnError<()> {
    // conversion via try-from
    assert_eq!(DataType::try_from(&arrow)?, marrow);
    assert_eq!(AD::try_from(&marrow)?, arrow);

    // conversion via serde
    assert_eq!(
        serde_json::from_value::<DataType>(serde_json::to_value(&arrow)?)?,
        marrow,
    );
    assert_eq!(
        serde_json::from_value::<AD>(serde_json::to_value(&marrow)?)?,
        arrow,
    );
    Ok(())
}

#[test]
fn primitives() -> PanicOnError<()> {
    assert_symmetric_conversion(AD::Null, DataType::Null)?;
    assert_symmetric_conversion(AD::Boolean, DataType::Boolean)?;
    assert_symmetric_conversion(AD::Int8, DataType::Int8)?;
    assert_symmetric_conversion(AD::Int16, DataType::Int16)?;
    assert_symmetric_conversion(AD::Int32, DataType::Int32)?;
    assert_symmetric_conversion(AD::Int64, DataType::Int64)?;
    assert_symmetric_conversion(AD::UInt8, DataType::UInt8)?;
    assert_symmetric_conversion(AD::UInt16, DataType::UInt16)?;
    assert_symmetric_conversion(AD::UInt32, DataType::UInt32)?;
    assert_symmetric_conversion(AD::UInt64, DataType::UInt64)?;
    assert_symmetric_conversion(AD::Float16, DataType::Float16)?;
    assert_symmetric_conversion(AD::Float32, DataType::Float32)?;
    assert_symmetric_conversion(AD::Float64, DataType::Float64)?;
    assert_symmetric_conversion(AD::Date32, DataType::Date32)?;
    assert_symmetric_conversion(AD::Date64, DataType::Date64)?;
    assert_symmetric_conversion(AD::Time32(AU::Second), DataType::Time32(TimeUnit::Second))?;
    assert_symmetric_conversion(
        AD::Time32(AU::Millisecond),
        DataType::Time32(TimeUnit::Millisecond),
    )?;
    assert_symmetric_conversion(
        AD::Time64(AU::Microsecond),
        DataType::Time64(TimeUnit::Microsecond),
    )?;
    assert_symmetric_conversion(
        AD::Time64(AU::Nanosecond),
        DataType::Time64(TimeUnit::Nanosecond),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Second, None),
        DataType::Timestamp(TimeUnit::Second, None),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Millisecond, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Second, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Millisecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Microsecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )?;
    assert_symmetric_conversion(
        AD::Timestamp(AU::Nanosecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )?;
    assert_symmetric_conversion(
        AD::Duration(AU::Second),
        DataType::Duration(TimeUnit::Second),
    )?;
    assert_symmetric_conversion(
        AD::Duration(AU::Millisecond),
        DataType::Duration(TimeUnit::Millisecond),
    )?;
    assert_symmetric_conversion(
        AD::Duration(AU::Microsecond),
        DataType::Duration(TimeUnit::Microsecond),
    )?;
    assert_symmetric_conversion(
        AD::Duration(AU::Nanosecond),
        DataType::Duration(TimeUnit::Nanosecond),
    )?;
    assert_symmetric_conversion(AD::Decimal128(2, -2), DataType::Decimal128(2, -2))?;
    assert_symmetric_conversion(AD::Decimal128(5, 3), DataType::Decimal128(5, 3))?;
    Ok(())
}

#[test]
fn binary_like() -> PanicOnError<()> {
    assert_symmetric_conversion(AD::Binary, DataType::Binary)?;
    assert_symmetric_conversion(AD::LargeBinary, DataType::LargeBinary)?;
    assert_symmetric_conversion(AD::FixedSizeBinary(5), DataType::FixedSizeBinary(5))?;
    assert_symmetric_conversion(AD::Utf8, DataType::Utf8)?;
    assert_symmetric_conversion(AD::LargeUtf8, DataType::LargeUtf8)?;
    Ok(())
}
