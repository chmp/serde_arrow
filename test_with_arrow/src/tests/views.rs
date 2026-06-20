use std::sync::Arc;

use marrow::array::{Array, BytesViewArray};

use super::utils::{assert_arrays_eq, PanicOnError};

use marrow::datatypes::DataType;

#[test]
fn view_types() {
    assert_eq!(
        DataType::try_from(&arrow_schema::DataType::Utf8View).unwrap(),
        DataType::Utf8View
    );
    assert_eq!(
        DataType::try_from(&arrow_schema::DataType::BinaryView).unwrap(),
        DataType::BinaryView
    );

    assert_eq!(
        arrow_schema::DataType::try_from(&DataType::Utf8View).unwrap(),
        arrow_schema::DataType::Utf8View
    );
    assert_eq!(
        arrow_schema::DataType::try_from(&DataType::BinaryView).unwrap(),
        arrow_schema::DataType::BinaryView
    );
}

fn pack_inline(data: &[u8]) -> u128 {
    assert!(data.len() <= 12);
    let mut result = data.len() as u128;
    for (i, b) in data.iter().enumerate() {
        result |= (*b as u128) << 8 * (4 + i);
    }

    result
}

fn pack_extern(data: &[u8], buffer: usize, offset: usize) -> u128 {
    assert!(data.len() > 12);
    assert!(data.len() <= i32::MAX as usize);
    assert!(buffer <= i32::MAX as usize);
    assert!(offset <= i32::MAX as usize);

    let len_bytes = data.len() as u128;
    let prefix = (data[0] as u128)
        | ((data[1] as u128) << 8)
        | ((data[2] as u128) << 16)
        | ((data[3] as u128) << 24);
    let buffer_bytes = buffer as u128;
    let offset_bytes = offset as u128;

    len_bytes | (prefix << 32) | (buffer_bytes << 64) | (offset_bytes << 96)
}

mod string_view {

    use super::*;

    use arrow_array::{ArrayRef, StringViewArray};

    // Adapted from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/4a0bdde24f48d0fc1222d936493f798d9ea4789d/arrow-array/src/array/byte_view_array.rs#L784
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
        Ok(Arc::new(StringViewArray::from_iter_values(vec![
            "hello",
            "world",
            "large payload over 12 bytes",
            "lulu",
            "second large payload",
        ])))
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::Utf8View(BytesViewArray {
                validity: None,
                data: vec![
                    pack_inline(b"hello"),
                    pack_inline(b"world"),
                    pack_extern(b"large payload over 12 bytes", 0, 0),
                    pack_inline(b"lulu"),
                    pack_extern(b"second large payload", 0, 27),
                ],
                buffers: vec![b"large payload over 12 bytessecond large payload".to_vec()],
            }),
        )
    }
}

mod binary_view {

    use super::*;

    use arrow_array::{ArrayRef, BinaryViewArray};

    // Adapted from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/4a0bdde24f48d0fc1222d936493f798d9ea4789d/arrow-array/src/array/byte_view_array.rs#L743
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
        Ok(Arc::new(BinaryViewArray::from_iter_values(vec![
            b"hello" as &[u8],
            b"world",
            b"large payload over 12 bytes",
            b"lulu",
            b"second large payload",
        ])))
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::BinaryView(BytesViewArray {
                validity: None,
                data: vec![
                    pack_inline(b"hello"),
                    pack_inline(b"world"),
                    pack_extern(b"large payload over 12 bytes", 0, 0),
                    pack_inline(b"lulu"),
                    pack_extern(b"second large payload", 0, 27),
                ],
                buffers: vec![b"large payload over 12 bytessecond large payload".to_vec()],
            }),
        )
    }
}
