#![allow(missing_docs)]

use crate::internal::{
    bytecode::{compiler::ArrayMapping, interpreter::Buffers, Interpreter},
    error::Result,
};

use crate::_impl::arrow::{
    array::{make_array, Array, ArrayData, ArrayRef, NullArray},
    buffer::{Buffer, ScalarBuffer},
    datatypes::{DataType, Field},
};

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
        &M::Null { buffer, .. } => Ok(NullArray::new(buffers.null[buffer].len()).into_data()),
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
        &M::Date64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Date64, i64, buffer, validity),
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
                let validity = std::mem::take(&mut buffers.validity[*validity]);
                let len = validity.len();
                let validity = Buffer::from(validity.buffer);
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
            let len = offset.len();
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.validity[*validity]);
                Some(Buffer::from(validity.buffer))
            } else {
                None
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
                let validity = std::mem::take(&mut buffers.validity[*validity]);
                Some(Buffer::from(validity.buffer))
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
        M::Union {
            field,
            fields,
            types,
        } => {
            let types = std::mem::take(&mut buffers.i8[*types]);
            let mut current_offset = vec![0; fields.len()];
            let mut offsets = Vec::new();

            for &t in &types.buffer {
                offsets.push(current_offset[t as usize]);
                current_offset[t as usize] += 1;
            }

            let mut children = Vec::new();
            for child in fields {
                children.push(build_array_data(buffers, child)?);
            }

            let len = types.len();

            let field: Field = field.try_into()?;
            let array_data_builder = ArrayData::builder(field.data_type().clone())
                .len(len)
                .add_buffer(Buffer::from_vec(types.buffer))
                .add_buffer(Buffer::from_vec(offsets))
                .child_data(children);

            Ok(array_data_builder.build()?)
        }
        M::DictionaryU32LargeUtf8 {
            field,
            dictionary,
            indices,
            validity,
        } => {
            let indices = std::mem::take(&mut buffers.u32[*indices]);

            let len = indices.len();
            let indices = ScalarBuffer::from(indices.buffer).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.validity[*validity]);
                Some(Buffer::from(validity.buffer))
            } else {
                None
            };

            let indices =
                ArrayData::try_new(DataType::UInt32, len, validity, 0, vec![indices], vec![])?;

            let dictionary = std::mem::take(&mut buffers.large_dictionaries[*dictionary]);
            let values_len = dictionary.values.len();

            let offsets = ScalarBuffer::from(dictionary.values.offsets.offsets).into_inner();
            let data = ScalarBuffer::from(dictionary.values.data).into_inner();

            let values = ArrayData::try_new(
                DataType::LargeUtf8,
                values_len,
                None,
                0,
                vec![offsets, data],
                vec![],
            )?;

            let field: Field = field.try_into()?;
            let data_type = field.data_type().clone();

            Ok(indices
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values])
                .build()?)
        }
    }
}
