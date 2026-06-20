use std::sync::Arc;

use marrow::{
    array::{Array, BooleanArray, PrimitiveArray, StructArray},
    datatypes::FieldMeta,
};

use super::utils::{assert_arrays_eq, PanicOnError};

mod struct_ {
    use super::*;

    // Copied from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/array/struct_array.rs#L52
    // License: ../../LICENSE.arrow.txt
    // Notice: ../../NOTICE.arrow.txt
    //
    // Original notice:
    //
    // Licensed to the Apache Software Foundation (ASF) under one
    // or more contributor license agreements.  See the NOTICE file
    // distributed with this work for additional information
    // regarding copyright ownership.  The ASF licenses this file
    // to you under the Apache License, Version 2.0 (the
    // "License"); you may not use this file except in compliance
    // with the License.  You may obtain a copy of the License at
    //
    //   http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing,
    // software distributed under the License is distributed on an
    // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    // KIND, either express or implied.  See the License for the
    // specific language governing permissions and limitations
    // under the License.
    fn example_array() -> PanicOnError<arrow_array::ArrayRef> {
        let boolean = Arc::new(arrow_array::array::BooleanArray::from(vec![
            false, false, true, true,
        ]));
        let int = Arc::new(arrow_array::array::Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = arrow_array::array::StructArray::from(vec![
            (
                Arc::new(arrow_schema::Field::new(
                    "b",
                    arrow_schema::DataType::Boolean,
                    false,
                )),
                boolean as arrow_array::ArrayRef,
            ),
            (
                Arc::new(arrow_schema::Field::new(
                    "c",
                    arrow_schema::DataType::Int32,
                    false,
                )),
                int as arrow_array::ArrayRef,
            ),
        ]);

        Ok(Arc::new(struct_array) as arrow_array::ArrayRef)
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::Struct(StructArray {
                len: 4,
                validity: None,
                fields: vec![
                    (
                        FieldMeta {
                            name: String::from("b"),
                            ..FieldMeta::default()
                        },
                        Array::Boolean(BooleanArray {
                            len: 4,
                            validity: None,
                            values: vec![0b_1100],
                        }),
                    ),
                    (
                        FieldMeta {
                            name: String::from("c"),
                            ..FieldMeta::default()
                        },
                        Array::Int32(PrimitiveArray {
                            validity: None,
                            values: vec![42, 28, 19, 31],
                        }),
                    ),
                ],
            }),
        )
    }
}
