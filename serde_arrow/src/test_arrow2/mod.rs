use crate::DataType;

use arrow2::datatypes::DataType as Arrow2DataType;

#[test]
fn from_data_type() {
    use Arrow2DataType::*;

    assert_eq!(DataType::from(Boolean), DataType::Bool);
    assert_eq!(DataType::from(Int8), DataType::I8);
    assert_eq!(DataType::from(Int16), DataType::I16);
    assert_eq!(DataType::from(Int32), DataType::I32);
    assert_eq!(DataType::from(Int64), DataType::I64);
    assert_eq!(DataType::from(UInt8), DataType::U8);
    assert_eq!(DataType::from(UInt16), DataType::U16);
    assert_eq!(DataType::from(UInt32), DataType::U32);
    assert_eq!(DataType::from(UInt64), DataType::U64);
    assert_eq!(DataType::from(Utf8), DataType::Str);

    assert_eq!(DataType::from(LargeUtf8), DataType::Arrow2(LargeUtf8));
}
