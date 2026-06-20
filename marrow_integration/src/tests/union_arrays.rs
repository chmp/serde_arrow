use std::sync::Arc;

use marrow::{
    array::{Array, PrimitiveArray, UnionArray},
    datatypes::FieldMeta,
};

use super::utils::{assert_arrays_eq, PanicOnError};

mod dense_union_array {
    use super::*;

    use arrow_array::{ArrayRef, Float64Array, Int32Array};

    // Adapted from the arrow docs
    //
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/array/union_array.rs#L48
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
    fn example_array() -> PanicOnError<ArrayRef> {
        let int_array = Int32Array::from(vec![1, 34]);
        let float_array = Float64Array::from(vec![3.2]);
        let type_ids = vec![0_i8, 1, 0];
        let offsets = vec![0, 0, 1];

        let union_fields = vec![
            (
                0_i8,
                Arc::new(arrow_schema::Field::new(
                    "A",
                    arrow_schema::DataType::Int32,
                    false,
                )),
            ),
            (
                1_i8,
                Arc::new(arrow_schema::Field::new(
                    "B",
                    arrow_schema::DataType::Float64,
                    false,
                )),
            ),
        ];

        let children = vec![Arc::new(int_array) as ArrayRef, Arc::new(float_array)];

        let array = arrow_array::UnionArray::try_new(
            union_fields.into_iter().collect(),
            type_ids.into(),
            Some(offsets.into()),
            children,
        )?;

        Ok(Arc::new(array) as ArrayRef)
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::Union(UnionArray {
                types: vec![0, 1, 0],
                offsets: Some(vec![0, 0, 1]),
                fields: vec![
                    (
                        0,
                        FieldMeta {
                            name: String::from("A"),
                            ..Default::default()
                        },
                        Array::Int32(PrimitiveArray {
                            validity: None,
                            values: vec![1, 34],
                        }),
                    ),
                    (
                        1,
                        FieldMeta {
                            name: String::from("B"),
                            ..Default::default()
                        },
                        Array::Float64(PrimitiveArray {
                            validity: None,
                            values: vec![3.2],
                        }),
                    ),
                ],
            }),
        )
    }
}

mod sparse_union_array {
    use super::*;

    use arrow_array::{ArrayRef, Float64Array, Int32Array};

    // Adapted from the arrow docs
    //
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/array/union_array.rs#L87
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
    fn example_array() -> PanicOnError<ArrayRef> {
        use arrow_schema::{DataType, Field};

        let int_array = Int32Array::from(vec![Some(1), None, Some(34)]);
        let float_array = Float64Array::from(vec![None, Some(3.2), None]);
        let type_ids = vec![0_i8, 1, 0];
        let union_fields = vec![
            (0, Arc::new(Field::new("A", DataType::Int32, false))),
            (1, Arc::new(Field::new("B", DataType::Float64, false))),
        ];

        let children = vec![Arc::new(int_array) as ArrayRef, Arc::new(float_array)];

        let array = arrow_array::UnionArray::try_new(
            union_fields.into_iter().collect(),
            type_ids.into_iter().collect(),
            None,
            children,
        )?;
        Ok(Arc::new(array) as ArrayRef)
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::Union(UnionArray {
                types: vec![0, 1, 0],
                offsets: None,
                fields: vec![
                    (
                        0,
                        // NOTE: the fields are explicitly set as non-nullable
                        FieldMeta {
                            name: String::from("A"),
                            ..Default::default()
                        },
                        Array::Int32(PrimitiveArray {
                            validity: Some(marrow::bit_vec![true, false, true]),
                            values: vec![1, 0, 34],
                        }),
                    ),
                    (
                        1,
                        // NOTE: the fields are explicitly set as non-nullable
                        FieldMeta {
                            name: String::from("B"),
                            ..Default::default()
                        },
                        Array::Float64(PrimitiveArray {
                            validity: Some(marrow::bit_vec![false, true, false]),
                            values: vec![0.0, 3.2, 0.0],
                        }),
                    ),
                ],
            }),
        )
    }
}
