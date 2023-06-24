use serde::{Deserialize, Serialize};

use crate::{
    arrow, arrow2,
    internal::{
        deserialize_from_arrays,
        schema::{GenericDataType, GenericField},
    },
    Result,
};

#[test]
fn example_arrow2() {
    use crate::_impl::arrow2::datatypes::Field;

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct S {
        a: i32,
        b: f32,
    }

    let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

    let fields = vec![
        GenericField::new("a", GenericDataType::I32, false),
        GenericField::new("b", GenericDataType::F16, false),
    ];

    let arrays;
    {
        let fields = fields
            .iter()
            .map(|f| Field::try_from(f))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        arrays = arrow2::serialize_into_arrays(&fields, items).unwrap();
    }

    let rountripped: Vec<S> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(rountripped, items);
}

#[test]
fn example_arrow() {
    use crate::_impl::arrow::{
        array::PrimitiveArray,
        datatypes::{Field, Float16Type, Int32Type},
    };

    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    struct S {
        a: i32,
        b: f32,
    }

    let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

    let fields = vec![
        GenericField::new("a", GenericDataType::I32, false),
        GenericField::new("b", GenericDataType::F16, false),
    ];
    let fields = fields
        .iter()
        .map(|f| Field::try_from(f))
        .collect::<Result<Vec<_>>>()
        .unwrap();

    let arrays = arrow::serialize_into_arrays(&fields, items).unwrap();

    let a = &arrays[0];

    let a_data = a
        .as_any()
        .downcast_ref::<PrimitiveArray<Int32Type>>()
        .unwrap()
        .values();
    let a_data: &[u32] = bytemuck::try_cast_slice(a_data).unwrap();

    assert_eq!(a_data, &[0, 1, 2]);

    let b = &arrays[1];

    let b_data = b
        .as_any()
        .downcast_ref::<PrimitiveArray<Float16Type>>()
        .unwrap()
        .values();
    let b_data: &[u16] = bytemuck::try_cast_slice(b_data).unwrap();

    assert_eq!(b_data.len(), 3);
}
