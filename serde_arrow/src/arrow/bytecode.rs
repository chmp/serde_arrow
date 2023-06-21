#![allow(missing_docs)]

use crate::internal::{
    bytecode::{
        buffers::{BitBuffer, StringBuffer},
        compiler::{ArrayMapping, DictionaryIndex, DictionaryValue},
        interpreter::Buffers,
        Interpreter,
    },
    conversions::ToBytes,
    error::{fail, Result},
};

use crate::_impl::arrow::{
    array::{make_array, Array, ArrayData, ArrayRef, NullArray},
    buffer::{Buffer, ScalarBuffer},
    datatypes::{ArrowNativeType, DataType, Field},
};

impl Interpreter {
    /// Build the arrow arrays
    pub fn build_arrow_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        let mut res = Vec::new();
        for mapping in &self.structure.array_mapping {
            let data = build_array_data(&mut self.buffers, mapping)?;
            let array = make_array(data);
            res.push(array);
        }
        Ok(res)
    }
}

macro_rules! build_primitive_array_data {
    ($buffers:expr, $dtype:ident, $ty:ty, $bytes_ty:ident, $buffer:expr, $validity:expr) => {{
        let data = std::mem::take(&mut $buffers.$bytes_ty[$buffer]);
        let data: Vec<$ty> = ToBytes::from_bytes_vec(data.buffer);
        let validity = $validity.map(|validity| std::mem::take(&mut $buffers.validity[validity]));
        build_array_data_primitive(DataType::$dtype, data.len(), data, validity)
    }};
}

pub fn build_array_data(buffers: &mut Buffers, mapping: &ArrayMapping) -> Result<ArrayData> {
    use ArrayMapping as M;
    match mapping {
        &M::Null { buffer, .. } => Ok(NullArray::new(buffers.u0[buffer].len()).into_data()),
        &M::Bool {
            buffer, validity, ..
        } => {
            let data = std::mem::take(&mut buffers.u1[buffer]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.validity[validity]));
            build_array_data_primitive(DataType::Boolean, data.len(), data.buffer, validity)
        }
        &M::U8 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt8, u8, u8, buffer, validity),
        &M::U16 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt16, u16, u16, buffer, validity),
        &M::U32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt32, u32, u32, buffer, validity),
        &M::U64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, UInt64, u64, u64, buffer, validity),
        &M::I8 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int8, i8, u8, buffer, validity),
        &M::I16 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int16, i16, u16, buffer, validity),
        &M::I32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int32, i32, u32, buffer, validity),
        &M::I64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Int64, i64, u64, buffer, validity),
        &M::F32 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Float32, f32, u32, buffer, validity),
        &M::F64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Float64, f64, u64, buffer, validity),
        &M::Date64 {
            buffer, validity, ..
        } => build_primitive_array_data!(buffers, Date64, i64, u64, buffer, validity),
        &M::Utf8 {
            buffer, validity, ..
        } => {
            let values = std::mem::take(&mut buffers.utf8[buffer]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.validity[validity]));
            build_array_data_utf8(values, validity)
        }
        &M::LargeUtf8 {
            buffer, validity, ..
        } => {
            let values = std::mem::take(&mut buffers.large_utf8[buffer]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.validity[validity]));
            build_array_data_large_utf8(values, validity)
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
                if data.is_empty() {
                    fail!("cannot built non-nullable structs without fields");
                }
                (None, data[0].len())
            };

            Ok(ArrayData::builder(field.data_type().clone())
                .len(len)
                .null_bit_buffer(validity)
                .child_data(data)
                .build()?)
        }
        M::Map {
            field,
            entries,
            offsets,
            validity,
        } => {
            let entries = build_array_data(buffers, entries)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.u32_offsets[*offsets]);
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
                .add_child_data(entries)
                .null_bit_buffer(validity);

            Ok(array_data_builder.build()?)
        }
        M::List {
            field,
            item,
            offsets,
            validity,
        } => {
            let values = build_array_data(buffers, item)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.u32_offsets[*offsets]);
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

            let offset = std::mem::take(&mut buffers.u64_offsets[*offsets]);
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
            let types = std::mem::take(&mut buffers.u8[*types]);
            let types: Vec<i8> = ToBytes::from_bytes_vec(types.buffer);
            let mut current_offset = vec![0; fields.len()];
            let mut offsets = Vec::new();

            for &t in &types {
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
                .add_buffer(Buffer::from_vec(types))
                .add_buffer(Buffer::from_vec(offsets))
                .child_data(children);

            Ok(array_data_builder.build()?)
        }
        M::Dictionary {
            field,
            dictionary,
            indices,
            validity,
        } => {
            use {DictionaryIndex as I, DictionaryValue as V};
            let validity = validity.map(|val| std::mem::take(&mut buffers.validity[val]));

            let indices = match indices {
                I::U8(indices) => {
                    let indices = std::mem::take(&mut buffers.u8[*indices]);
                    build_array_data_primitive(
                        DataType::UInt8,
                        indices.len(),
                        indices.buffer,
                        validity,
                    )?
                }
                I::U16(indices) => {
                    let indices = std::mem::take(&mut buffers.u16[*indices]);
                    build_array_data_primitive(
                        DataType::UInt16,
                        indices.len(),
                        indices.buffer,
                        validity,
                    )?
                }
                I::U32(indices) => {
                    let indices = std::mem::take(&mut buffers.u32[*indices]);
                    build_array_data_primitive(
                        DataType::UInt32,
                        indices.len(),
                        indices.buffer,
                        validity,
                    )?
                }
                I::U64(indices) => {
                    let indices = std::mem::take(&mut buffers.u64[*indices]);
                    build_array_data_primitive(
                        DataType::UInt64,
                        indices.len(),
                        indices.buffer,
                        validity,
                    )?
                }
                I::I8(indices) => {
                    let indices = std::mem::take(&mut buffers.u8[*indices]);
                    let indices: Vec<i8> = ToBytes::from_bytes_vec(indices.buffer);
                    build_array_data_primitive(DataType::Int8, indices.len(), indices, validity)?
                }
                I::I16(indices) => {
                    let indices = std::mem::take(&mut buffers.u16[*indices]);
                    let indices: Vec<i16> = ToBytes::from_bytes_vec(indices.buffer);
                    build_array_data_primitive(DataType::Int16, indices.len(), indices, validity)?
                }
                I::I32(indices) => {
                    let indices = std::mem::take(&mut buffers.u32[*indices]);
                    let indices: Vec<i32> = ToBytes::from_bytes_vec(indices.buffer);
                    build_array_data_primitive(DataType::Int32, indices.len(), indices, validity)?
                }
                I::I64(indices) => {
                    let indices = std::mem::take(&mut buffers.u64[*indices]);
                    let indices: Vec<i64> = ToBytes::from_bytes_vec(indices.buffer);
                    build_array_data_primitive(DataType::Int64, indices.len(), indices, validity)?
                }
            };

            let values = match dictionary {
                V::Utf8(dict) => {
                    let dictionary = std::mem::take(&mut buffers.dictionaries[*dict]);
                    build_array_data_utf8(dictionary.values, None)?
                }
                V::LargeUtf8(dict) => {
                    let dictionary = std::mem::take(&mut buffers.large_dictionaries[*dict]);
                    build_array_data_large_utf8(dictionary.values, None)?
                }
            };

            let data_type = Field::try_from(field)?.data_type().clone();

            Ok(indices
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values])
                .build()?)
        }
    }
}

fn build_array_data_utf8(
    values: StringBuffer<i32>,
    validity: Option<BitBuffer>,
) -> Result<ArrayData> {
    build_array_data_utf8_impl(
        DataType::Utf8,
        values.data,
        values.offsets.offsets,
        validity,
    )
}

fn build_array_data_large_utf8(
    values: StringBuffer<i64>,
    validity: Option<BitBuffer>,
) -> Result<ArrayData> {
    build_array_data_utf8_impl(
        DataType::LargeUtf8,
        values.data,
        values.offsets.offsets,
        validity,
    )
}

fn build_array_data_utf8_impl<O: ArrowNativeType>(
    data_type: DataType,
    data: Vec<u8>,
    offsets: Vec<O>,
    validity: Option<BitBuffer>,
) -> Result<ArrayData> {
    let values_len = offsets.len() - 1;

    let offsets = ScalarBuffer::from(offsets).into_inner();
    let data = ScalarBuffer::from(data).into_inner();
    let validity = validity.map(|b| Buffer::from(b.buffer));

    Ok(ArrayData::try_new(
        data_type,
        values_len,
        validity,
        0,
        vec![offsets, data],
        vec![],
    )?)
}

fn build_array_data_primitive<T: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    data: Vec<T>,
    validity: Option<BitBuffer>,
) -> Result<ArrayData> {
    Ok(ArrayData::try_new(
        data_type,
        len,
        validity.map(|b| Buffer::from(b.buffer)),
        0,
        vec![ScalarBuffer::from(data).into_inner()],
        vec![],
    )?)
}
