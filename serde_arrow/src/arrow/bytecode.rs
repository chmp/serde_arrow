#![allow(missing_docs)]
use crate::internal::{
    bytecode::{
        buffers::{BitBuffer, BoolBuffer, PrimitiveBuffer, StringBuffer},
        compiler::ArrayMapping,
        interpreter::Buffers,
        Interpreter,
    },
    error::{fail, Result},
};

use crate::_impl::arrow::{
    array::{make_array, ArrayData, ArrayRef},
    buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};

pub trait IntoArrowArrayData {
    fn into_arrow_array_data(self) -> Result<ArrayData>;

    fn take_as_arrow_array_data(&mut self) -> Result<ArrayData>
    where
        Self: Default,
    {
        std::mem::take(self).into_arrow_array_data()
    }
}

impl BitBuffer {
    pub fn into_arrow_null_buffer(self) -> NullBuffer {
        let buffer = Buffer::from(self.buffer);
        let buffer = BooleanBuffer::new(buffer, 0, self.len);
        NullBuffer::new(buffer)
    }
}

impl IntoArrowArrayData for BoolBuffer {
    fn into_arrow_array_data(self) -> Result<ArrayData> {
        let len = self.validity.len;
        let null_buffer = Buffer::from(self.validity.buffer);
        let data = Buffer::from(self.data.buffer);

        Ok(ArrayData::try_new(
            DataType::Boolean,
            len,
            Some(null_buffer),
            0,
            vec![data],
            vec![],
        )?)
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
        impl IntoArrowArrayData for PrimitiveBuffer<$rust_ty> {
            fn into_arrow_array_data(self) -> Result<ArrayData> {
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
        impl IntoArrowArrayData for StringBuffer<$ty> {
            fn into_arrow_array_data(self) -> Result<ArrayData> {
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

#[cfg(test)]
mod validity_bitmap {
    use crate::internal::bytecode::buffers::BitBuffer;

    #[test]
    fn example() {
        let bitmap = BitBuffer::from([
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

#[cfg(test)]
mod u16_array {
    use super::IntoArrowArrayData;
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

#[cfg(test)]
mod utf8_array {
    use super::IntoArrowArrayData;
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

impl Interpreter {
    /// Build the arrow arrays
    pub fn build_arrow_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        let mut res = Vec::new();
        for mapping in &self.array_mapping {
            let data = build_array_data(&mut self.buffers, mapping)?;
            let array = make_array(data);
            res.push(array);
        }
        Ok(res)
    }
}

macro_rules! build_primitive_array_data {
    ($buffers:expr, $dtype:ident, $ty:ident, $buffer:expr, $validity:expr) => {{
        let data = std::mem::take(&mut $buffers.bool[$buffer]);

        let len = data.len();
        let data = ScalarBuffer::from(data.buffer).into_inner();
        if $validity.is_some() {
            fail!("Nullable primitives are not yet supported");
        }
        // let null_buffer = Buffer::from(self.validity.buffer);

        Ok(ArrayData::try_new(
            DataType::$dtype,
            len,
            None, // Some(null_buffer),
            0,
            vec![data],
            vec![],
        )?)
    }};
}

pub fn build_array_data(buffers: &mut Buffers, mapping: &ArrayMapping) -> Result<ArrayData> {
    use ArrayMapping as M;
    match mapping {
        &M::Bool {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Boolean, bool, buffer, validity),
        &M::U8(_, idx) => buffers.u8[idx].take_as_arrow_array_data(),
        &M::U16(_, idx) => buffers.u16[idx].take_as_arrow_array_data(),
        &M::U32(_, idx) => buffers.u32[idx].take_as_arrow_array_data(),
        &M::U64(_, idx) => buffers.u64[idx].take_as_arrow_array_data(),
        &M::I8(_, idx) => buffers.i8[idx].take_as_arrow_array_data(),
        &M::I16(_, idx) => buffers.i16[idx].take_as_arrow_array_data(),
        &M::I32(_, idx) => buffers.i32[idx].take_as_arrow_array_data(),
        &M::I64(_, idx) => buffers.i64[idx].take_as_arrow_array_data(),
        &M::F32(_, idx) => buffers.f32[idx].take_as_arrow_array_data(),
        &M::F64(_, idx) => buffers.f64[idx].take_as_arrow_array_data(),
        &M::Utf8(_, idx) => buffers.utf8[idx].take_as_arrow_array_data(),
        &M::LargeUtf8(_, idx) => buffers.large_utf8[idx].take_as_arrow_array_data(),
        M::Struct {
            field,
            fields,
            validity,
        } => {
            let mut data = Vec::new();
            for field in fields {
                data.push(build_array_data(buffers, field)?);
            }

            let field: Field = field.try_into()?;

            let (validity, len) = if let Some(validity) = validity {
                let validity =
                    std::mem::take(&mut buffers.validity[*validity]).into_arrow_null_buffer();
                let len = validity.len();
                let validity = validity.into_inner().into_inner();

                (Some(validity), len)
            } else {
                // TODO: avoid the panic here
                (None, data[0].len())
            };

            Ok(ArrayData::builder(field.data_type().clone())
                .len(len)
                .null_bit_buffer(validity)
                .child_data(data)
                .build()?)
        }
        M::List {
            field,
            item,
            offsets,
            validity,
        } => {
            let values = build_array_data(buffers, item)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.offset[*offsets]);
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let (validity, len) = if let Some(validity) = validity {
                let validity =
                    std::mem::take(&mut buffers.validity[*validity]).into_arrow_null_buffer();
                let len = validity.len();
                let validity = validity.into_inner().into_inner();
                (Some(validity), len)
            } else {
                (None, offset_buffer.len() - 1)
            };

            let array_data_builder = ArrayData::builder(field.data_type().clone())
                .len(len)
                .add_buffer(offset_buffer)
                .add_child_data(values)
                .null_bit_buffer(validity);

            Ok(array_data_builder.build()?)
        }
        M::LargeList {
            field,
            item,
            offsets,
            validity,
        } => {
            let values = build_array_data(buffers, item)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.large_offset[*offsets]);
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let (validity, len) = if let Some(validity) = validity {
                let validity =
                    std::mem::take(&mut buffers.validity[*validity]).into_arrow_null_buffer();
                let len = validity.len();
                let validity = validity.into_inner().into_inner();
                (Some(validity), len)
            } else {
                (None, offset_buffer.len() - 1)
            };

            let array_data_builder = ArrayData::builder(field.data_type().clone())
                .len(len)
                .add_buffer(offset_buffer)
                .add_child_data(values)
                .null_bit_buffer(validity);

            Ok(array_data_builder.build()?)
        }
    }
}
