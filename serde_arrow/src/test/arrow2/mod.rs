mod generic_sources;
mod round_trip;
mod round_trip_array;
mod schema;
mod schema_events;
mod schema_mod;
mod sinks;
mod sources;
pub(crate) mod utils;

use arrow2::{
    array::{Array, MutableArray, MutablePrimitiveArray, MutableStructArray, StructArray},
    datatypes::{DataType as Arrow2DataType, Field},
};

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
