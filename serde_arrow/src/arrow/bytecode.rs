#![allow(missing_docs)]
use crate::internal::{
    bytecode::{
        buffers::{BitBuffer, BoolBuffer},
        compiler::ArrayMapping,
        interpreter::Buffers,
        Interpreter,
    },
    error::Result,
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
        let data = std::mem::take(&mut $buffers.$ty[$buffer]);

        let len = data.len();
        let data = ScalarBuffer::from(data.buffer).into_inner();

        let validity = if let Some(validity) = $validity {
            let validity = std::mem::take(&mut $buffers.validity[validity]);
            Some(Buffer::from(validity.buffer))
        } else {
            None
        };

        Ok(ArrayData::try_new(
            DataType::$dtype,
            len,
            validity,
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
        &M::U8 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt8, u8, buffer, validity),
        &M::U16 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt16, u16, buffer, validity),
        &M::U32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt32, u32, buffer, validity),
        &M::U64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt64, u64, buffer, validity),
        &M::I8 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int8, i8, buffer, validity),
        &M::I16 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int16, i16, buffer, validity),
        &M::I32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int32, i32, buffer, validity),
        &M::I64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int64, i64, buffer, validity),
        &M::F32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Float32, f32, buffer, validity),
        &M::F64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Float64, f64, buffer, validity),
        &M::Utf8 {
            buffer, validity, ..
        } => {
            let buffer = std::mem::take(&mut buffers.utf8[buffer]);
            let len = buffer.len();

            let offsets = ScalarBuffer::from(buffer.offsets.offsets).into_inner();
            let data = ScalarBuffer::from(buffer.data).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.validity[validity]);
                Some(Buffer::from(validity.buffer))
            } else {
                None
            };

            Ok(ArrayData::try_new(
                DataType::LargeUtf8,
                len,
                validity,
                0,
                vec![offsets, data],
                vec![],
            )?)
        }
        &M::LargeUtf8 {
            buffer, validity, ..
        } => {
            let buffer = std::mem::take(&mut buffers.large_utf8[buffer]);
            let len = buffer.len();

            let offsets = ScalarBuffer::from(buffer.offsets.offsets).into_inner();
            let data = ScalarBuffer::from(buffer.data).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.validity[validity]);
                Some(Buffer::from(validity.buffer))
            } else {
                None
            };

            Ok(ArrayData::try_new(
                DataType::LargeUtf8,
                len,
                validity,
                0,
                vec![offsets, data],
                vec![],
            )?)
        }
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

            let offset = std::mem::take(&mut buffers.large_offset[*offsets]);
            let len = offset.len();
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let validity = if let Some(validity) = validity {
                let validity =
                    std::mem::take(&mut buffers.validity[*validity]).into_arrow_null_buffer();
                let validity = validity.into_inner().into_inner();
                Some(validity)
            } else {
                None
            };

            let field: Field = field.try_into()?;
            let array_data_builder = ArrayData::builder(field.data_type().clone())
                .len(len)
                .add_buffer(offset_buffer)
                .add_child_data(values)
                .null_bit_buffer(validity);

            Ok(array_data_builder.build()?)
        }
    }
}
