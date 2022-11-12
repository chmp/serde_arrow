mod sinks;
mod sources;

use crate::DataType;

use arrow2::{
    array::{Array, MutableArray, MutablePrimitiveArray, MutableStructArray, StructArray},
    datatypes::{DataType as Arrow2DataType, Field},
};

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

#[test]
fn test_struct_arrow_builder() {
    let data_type = Arrow2DataType::Struct(vec![
        Field::new("a", Arrow2DataType::Int8, false),
        Field::new("b", Arrow2DataType::Int32, false),
    ]);
    let values: Vec<Box<dyn MutableArray>> = vec![
        Box::new(MutablePrimitiveArray::<i8>::new()),
        Box::new(MutablePrimitiveArray::<i32>::new()),
    ];
    let mut builder = MutableStructArray::new(data_type, values);

    builder
        .value::<MutablePrimitiveArray<i8>>(0)
        .unwrap()
        .push(Some(0));
    builder
        .value::<MutablePrimitiveArray<i32>>(1)
        .unwrap()
        .push(Some(1));
    builder.push(true);

    builder
        .value::<MutablePrimitiveArray<i8>>(0)
        .unwrap()
        .push(Some(2));
    builder
        .value::<MutablePrimitiveArray<i32>>(1)
        .unwrap()
        .push(Some(4));
    builder.push(true);

    let array: StructArray = builder.into();

    println!("{array:?}");

    assert_eq!(array.len(), 2);
}

#[test]
fn test_nested_struct_arrow_builder() {
    let inner_type = Arrow2DataType::Struct(vec![
        Field::new("c", Arrow2DataType::Int32, false),
        Field::new("d", Arrow2DataType::Int32, false),
    ]);
    let data_type = Arrow2DataType::Struct(vec![
        Field::new("a", Arrow2DataType::Int8, false),
        Field::new("b", inner_type.clone(), false),
    ]);
    let mut builder = MutableStructArray::new(
        data_type,
        vec![
            Box::new(MutablePrimitiveArray::<i8>::new()) as _,
            Box::new(MutableStructArray::new(
                inner_type,
                vec![
                    Box::new(MutablePrimitiveArray::<i32>::new()) as _,
                    Box::new(MutablePrimitiveArray::<i32>::new()) as _,
                ],
            )) as _,
        ],
    );

    builder
        .value::<MutablePrimitiveArray<i8>>(0)
        .unwrap()
        .push(Some(0));
    builder
        .value::<MutableStructArray>(1)
        .unwrap()
        .value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(1));
    builder
        .value::<MutableStructArray>(1)
        .unwrap()
        .value::<MutablePrimitiveArray<i32>>(1)
        .unwrap()
        .push(Some(1));
    builder.value::<MutableStructArray>(1).unwrap().push(true);
    builder.push(true);

    builder
        .value::<MutablePrimitiveArray<i8>>(0)
        .unwrap()
        .push(Some(2));
    builder
        .value::<MutableStructArray>(1)
        .unwrap()
        .value::<MutablePrimitiveArray<i32>>(0)
        .unwrap()
        .push(Some(3));
    builder
        .value::<MutableStructArray>(1)
        .unwrap()
        .value::<MutablePrimitiveArray<i32>>(1)
        .unwrap()
        .push(Some(4));
    builder.value::<MutableStructArray>(1).unwrap().push(true);
    builder.push(true);

    let array: StructArray = builder.into();

    println!("{array:?}");

    assert_eq!(array.len(), 2);
}
