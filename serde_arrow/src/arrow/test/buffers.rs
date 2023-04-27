use crate::internal::{
    bytecode::buffers::{BoolBuffer, PrimitiveBuffer, StringBuffer},
    error::Result,
};

use crate::_impl::arrow::{
    array::ArrayData,
    buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer},
    datatypes::DataType,
};

impl BoolBuffer {
    pub fn into_arrow_null_buffer(self) -> NullBuffer {
        let buffer = Buffer::from(self.buffer);
        let buffer = BooleanBuffer::new(buffer, 0, self.len);
        let buffer = NullBuffer::new(buffer);
        buffer
    }
}

macro_rules! impl_primitive_buffer_to_arrow {
    () => {
        impl_primitive_buffer_to_arrow!(u8, UInt8);
        impl_primitive_buffer_to_arrow!(u16, UInt16);
        impl_primitive_buffer_to_arrow!(u32, UInt32);
        impl_primitive_buffer_to_arrow!(u64, UInt64);

        impl_primitive_buffer_to_arrow!(i8, Int8);
        impl_primitive_buffer_to_arrow!(i16, Int16);
        impl_primitive_buffer_to_arrow!(i32, Int32);
        impl_primitive_buffer_to_arrow!(i64, Int64);

        impl_primitive_buffer_to_arrow!(f32, Float32);
        impl_primitive_buffer_to_arrow!(f64, Float64);
    };
    ($rust_ty:ty, $arrow_datatype:ident) => {
        impl PrimitiveBuffer<$rust_ty> {
            pub fn into_arrow_array_data(self) -> Result<ArrayData> {
                let len = self.validity.len;
                let null_buffer = Buffer::from(self.validity.buffer);
                let data = ScalarBuffer::from(self.data).into_inner();

                Ok(ArrayData::try_new(
                    DataType::$arrow_datatype,
                    len,
                    Some(null_buffer),
                    0,
                    vec![data],
                    vec![],
                )?)
            }
        }
    };
}

impl_primitive_buffer_to_arrow!();

macro_rules! impl_string_buffer_to_arrow {
    () => {
        impl_string_buffer_to_arrow!(i32, Utf8);
        impl_string_buffer_to_arrow!(i64, LargeUtf8);
    };
    ($ty:ty, $dtype:ident) => {
        impl StringBuffer<$ty> {
            pub fn into_arrow_array_data(self) -> Result<ArrayData> {
                let len = self.validity.len;
                let null_buffer = Buffer::from(self.validity.buffer);
                let offsets = ScalarBuffer::from(self.offsets.offsets).into_inner();
                let data = ScalarBuffer::from(self.data).into_inner();

                Ok(ArrayData::try_new(
                    DataType::$dtype,
                    len,
                    Some(null_buffer),
                    0,
                    vec![offsets, data],
                    vec![],
                )?)
            }
        }
    };
}

impl_string_buffer_to_arrow!();

mod validity_bitmap {
    use crate::internal::bytecode::buffers::BoolBuffer;

    #[test]
    fn example() {
        let bitmap = BoolBuffer::from([
            true, false, false, true, true, true, false, false, true, true,
        ]);
        let null_buffer = bitmap.into_arrow_null_buffer();

        assert_eq!(null_buffer.len(), 10);
        assert_eq!(null_buffer.is_null(0), false);
        assert_eq!(null_buffer.is_null(1), true);
        assert_eq!(null_buffer.is_null(2), true);
        assert_eq!(null_buffer.is_null(3), false);
        assert_eq!(null_buffer.is_null(4), false);
        assert_eq!(null_buffer.is_null(5), false);
        assert_eq!(null_buffer.is_null(6), true);
        assert_eq!(null_buffer.is_null(7), true);
        assert_eq!(null_buffer.is_null(8), false);
        assert_eq!(null_buffer.is_null(9), false);

        assert_eq!(null_buffer.null_count(), 4);
    }
}

mod u16_array {
    use crate::internal::bytecode::buffers::PrimitiveBuffer;

    use crate::_impl::arrow::{
        array::{make_array, Array, AsArray},
        datatypes::UInt16Type,
    };

    #[test]
    fn example() {
        let mut buffer = PrimitiveBuffer::<u16>::new();

        buffer.push(0).unwrap();
        buffer.push_null().unwrap();
        buffer.push_null().unwrap();
        buffer.push(6).unwrap();
        buffer.push(8).unwrap();
        buffer.push_null().unwrap();
        buffer.push(12).unwrap();

        let data = buffer.into_arrow_array_data().unwrap();

        let array = make_array(data);
        let array = array.as_primitive_opt::<UInt16Type>().unwrap();

        assert_eq!(array.len(), 7);

        assert_eq!(array.is_null(0), false);
        assert_eq!(array.value(0), 0);

        assert_eq!(array.is_null(1), true);
        assert_eq!(array.value(1), 0);

        assert_eq!(array.is_null(2), true);
        assert_eq!(array.value(2), 0);

        assert_eq!(array.is_null(3), false);
        assert_eq!(array.value(3), 6);

        assert_eq!(array.is_null(4), false);
        assert_eq!(array.value(4), 8);

        assert_eq!(array.is_null(5), true);
        assert_eq!(array.value(5), 0);

        assert_eq!(array.is_null(6), false);
        assert_eq!(array.value(6), 12);
    }
}

mod utf8_array {
    use crate::internal::bytecode::buffers::StringBuffer;

    use crate::_impl::arrow::array::{make_array, Array, AsArray};

    #[test]
    fn example() {
        let mut buffer = StringBuffer::<i64>::new();

        buffer.push("foo").unwrap();
        buffer.push_null().unwrap();
        buffer.push_null().unwrap();
        buffer.push("bar").unwrap();
        buffer.push("baz").unwrap();

        let data = buffer.into_arrow_array_data().unwrap();

        let array = make_array(data);
        let array = array.as_string_opt::<i64>().unwrap();

        assert_eq!(array.len(), 5);

        assert_eq!(array.is_null(0), false);
        assert_eq!(array.value(0), "foo");

        assert_eq!(array.is_null(1), true);
        assert_eq!(array.value(1), "");

        assert_eq!(array.is_null(2), true);
        assert_eq!(array.value(2), "");

        assert_eq!(array.is_null(3), false);
        assert_eq!(array.value(3), "bar");

        assert_eq!(array.is_null(4), false);
        assert_eq!(array.value(4), "baz");
    }
}
