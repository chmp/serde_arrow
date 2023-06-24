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
    use crate::_impl::arrow::datatypes::Field;

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

        arrays = arrow::serialize_into_arrays(&fields, items).unwrap();
    }

    let rountripped: Vec<S> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(rountripped, items);
}
