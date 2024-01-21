#![allow(missing_docs)]

use crate::internal::{
    common::{ArrayMapping, DictionaryIndex, DictionaryValue, MutableBitBuffer, MutableBuffers},
    conversions::ToBytes,
    error::{fail, Result},
    serialization_ng::ArrayBuilder,
};

use crate::_impl::arrow::{
    array::{make_array, Array, ArrayData, ArrayRef, NullArray},
    buffer::{Buffer, ScalarBuffer},
    datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType, Field, Float16Type},
};

impl ArrayBuilder {
    pub fn build_arrow_arrays(&mut self) -> Result<Vec<ArrayRef>> {
        let fields = self.take_records()?;
        let arrays = fields
            .into_iter()
            .map(build_array)
            .collect::<Result<Vec<_>>>()?;
        Ok(arrays)
    }
}

fn build_array(builder: ArrayBuilder) -> Result<ArrayRef> {
    let data = build_array_data(builder)?;
    Ok(make_array(data))
}

fn build_array_data(builder: ArrayBuilder) -> Result<ArrayData> {
    match builder {
        ArrayBuilder::Bool(builder) => build_array_data_primitive_with_len(
            DataType::Boolean,
            builder.buffer.len(),
            builder.buffer.buffer,
            builder.validity,
        ),
        ArrayBuilder::I8(builder) => {
            build_array_data_primitive(DataType::Int8, builder.buffer, builder.validity)
        }
        ArrayBuilder::I16(builder) => {
            build_array_data_primitive(DataType::Int16, builder.buffer, builder.validity)
        }
        ArrayBuilder::I32(builder) => {
            build_array_data_primitive(DataType::Int32, builder.buffer, builder.validity)
        }
        ArrayBuilder::I64(builder) => {
            build_array_data_primitive(DataType::Int64, builder.buffer, builder.validity)
        }
        ArrayBuilder::U8(builder) => {
            build_array_data_primitive(DataType::UInt8, builder.buffer, builder.validity)
        }
        ArrayBuilder::U16(builder) => {
            build_array_data_primitive(DataType::UInt16, builder.buffer, builder.validity)
        }
        ArrayBuilder::U32(builder) => {
            build_array_data_primitive(DataType::UInt32, builder.buffer, builder.validity)
        }
        ArrayBuilder::U64(builder) => {
            build_array_data_primitive(DataType::UInt64, builder.buffer, builder.validity)
        }
        ArrayBuilder::F32(builder) => {
            build_array_data_primitive(DataType::Float32, builder.buffer, builder.validity)
        }
        ArrayBuilder::F64(builder) => {
            build_array_data_primitive(DataType::Float64, builder.buffer, builder.validity)
        }
        ArrayBuilder::Utf8(builder) => build_array_data_utf8(
            DataType::Utf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        ArrayBuilder::LargeUtf8(builder) => build_array_data_utf8(
            DataType::LargeUtf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        builder => fail!("cannot build arrow array for {}", builder.name()),
    }
}

fn build_array_data_primitive<T: ArrowNativeType>(
    data_type: DataType,
    data: Vec<T>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let len = data.len();
    build_array_data_primitive_with_len(data_type, len, data, validity)
}

fn build_array_data_primitive_with_len<T: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    data: Vec<T>,
    validity: Option<MutableBitBuffer>,
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

fn build_array_data_utf8<O: ArrowNativeType>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<MutableBitBuffer>,
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

// Old Code. Delete once reimplemented
macro_rules! build_primitive_array_data {
    ($buffers:expr, $field:expr, $ty:ty, $bytes_ty:ident, $buffer:expr, $validity:expr) => {{
        let data = std::mem::take(&mut $buffers.$bytes_ty[$buffer]);
        let data: Vec<$ty> = ToBytes::from_bytes_vec(data);
        let validity = $validity.map(|validity| std::mem::take(&mut $buffers.u1[validity]));
        let data_type = Field::try_from($field)?.data_type().clone();
        build_array_data_primitive_with_len(data_type, data.len(), data, validity)
    }};
}

pub fn build_array_data_old(
    buffers: &mut MutableBuffers,
    mapping: &ArrayMapping,
) -> Result<ArrayData> {
    use ArrayMapping as M;
    match mapping {
        &M::Null { buffer, .. } => Ok(NullArray::new(buffers.u0[buffer].len()).into_data()),
        &M::Bool {
            buffer, validity, ..
        } => {
            let data = std::mem::take(&mut buffers.u1[buffer]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.u1[validity]));
            build_array_data_primitive_with_len(
                DataType::Boolean,
                data.len(),
                data.buffer,
                validity,
            )
        }
        M::U8 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, u8, u8, *buffer, *validity),
        M::U16 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, u16, u16, *buffer, *validity),
        M::U32 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, u32, u32, *buffer, *validity),
        M::U64 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, u64, u64, *buffer, *validity),
        M::I8 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, i8, u8, *buffer, *validity),
        M::I16 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, i16, u16, *buffer, *validity),
        M::I32 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, i32, u32, *buffer, *validity),
        M::I64 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, i64, u64, *buffer, *validity),
        &M::F16 {
            buffer, validity, ..
        } => {
            let data = std::mem::take(&mut buffers.u16[buffer]);
            let data = data
                .into_iter()
                .map(<Float16Type as ArrowPrimitiveType>::Native::from_bits)
                .collect::<Vec<_>>();
            let validity = validity.map(|validity| std::mem::take(&mut buffers.u1[validity]));
            build_array_data_primitive_with_len(DataType::Float16, data.len(), data, validity)
        }
        M::F32 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, f32, u32, *buffer, *validity),
        M::F64 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, f64, u64, *buffer, *validity),
        M::Decimal128 {
            field,
            validity,
            buffer,
        } => {
            let data = std::mem::take(&mut buffers.u128[*buffer]);
            let data: Vec<i128> = ToBytes::from_bytes_vec(data);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.u1[validity]));
            let data_type = Field::try_from(field)?.data_type().clone();
            build_array_data_primitive_with_len(data_type, data.len(), data, validity)
        }
        M::Date64 {
            field,
            buffer,
            validity,
            ..
        } => build_primitive_array_data!(buffers, field, i64, u64, *buffer, *validity),
        &M::Utf8 {
            buffer,
            offsets,
            validity,
            ..
        } => {
            let data = std::mem::take(&mut buffers.u8[buffer]);
            let offsets = std::mem::take(&mut buffers.u32_offsets[offsets]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.u1[validity]));
            build_array_data_utf8_old(data, offsets.offsets, validity)
        }
        &M::LargeUtf8 {
            buffer,
            offsets,
            validity,
            ..
        } => {
            let values = std::mem::take(&mut buffers.u8[buffer]);
            let offsets = std::mem::take(&mut buffers.u64_offsets[offsets]);
            let validity = validity.map(|validity| std::mem::take(&mut buffers.u1[validity]));
            build_array_data_large_utf8(values, offsets.offsets, validity)
        }
        M::Struct {
            field,
            fields,
            validity,
        } => {
            let mut data = Vec::new();
            for field in fields {
                data.push(build_array_data_old(buffers, field)?);
            }

            let field: Field = field.try_into()?;

            let (validity, len) = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.u1[*validity]);
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
            let entries = build_array_data_old(buffers, entries)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.u32_offsets[*offsets]);
            let len = offset.len();
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.u1[*validity]);
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
            let values = build_array_data_old(buffers, item)?;
            let field: Field = field.try_into()?;

            let offset = std::mem::take(&mut buffers.u32_offsets[*offsets]);
            let len = offset.len();
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.u1[*validity]);
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
            let values = build_array_data_old(buffers, item)?;

            let offset = std::mem::take(&mut buffers.u64_offsets[*offsets]);
            let len = offset.len();
            let offset_buffer = ScalarBuffer::from(offset.offsets).into_inner();

            let validity = if let Some(validity) = validity {
                let validity = std::mem::take(&mut buffers.u1[*validity]);
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
            ..
        } => {
            let types = std::mem::take(&mut buffers.u8[*types]);
            let types: Vec<i8> = ToBytes::from_bytes_vec(types);
            let mut current_offset = vec![0; fields.len()];
            let mut offsets = Vec::new();

            for &t in &types {
                offsets.push(current_offset[t as usize]);
                current_offset[t as usize] += 1;
            }

            let mut children = Vec::new();
            for child in fields {
                children.push(build_array_data_old(buffers, child)?);
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
            let validity = validity.map(|val| std::mem::take(&mut buffers.u1[val]));

            let indices = match indices {
                I::U8(indices) => {
                    let indices = std::mem::take(&mut buffers.u8[*indices]);
                    build_array_data_primitive_with_len(
                        DataType::UInt8,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::U16(indices) => {
                    let indices = std::mem::take(&mut buffers.u16[*indices]);
                    build_array_data_primitive_with_len(
                        DataType::UInt16,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::U32(indices) => {
                    let indices = std::mem::take(&mut buffers.u32[*indices]);
                    build_array_data_primitive_with_len(
                        DataType::UInt32,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::U64(indices) => {
                    let indices = std::mem::take(&mut buffers.u64[*indices]);
                    build_array_data_primitive_with_len(
                        DataType::UInt64,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::I8(indices) => {
                    let indices = std::mem::take(&mut buffers.u8[*indices]);
                    let indices: Vec<i8> = ToBytes::from_bytes_vec(indices);
                    build_array_data_primitive_with_len(
                        DataType::Int8,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::I16(indices) => {
                    let indices = std::mem::take(&mut buffers.u16[*indices]);
                    let indices: Vec<i16> = ToBytes::from_bytes_vec(indices);
                    build_array_data_primitive_with_len(
                        DataType::Int16,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::I32(indices) => {
                    let indices = std::mem::take(&mut buffers.u32[*indices]);
                    let indices: Vec<i32> = ToBytes::from_bytes_vec(indices);
                    build_array_data_primitive_with_len(
                        DataType::Int32,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
                I::I64(indices) => {
                    let indices = std::mem::take(&mut buffers.u64[*indices]);
                    let indices: Vec<i64> = ToBytes::from_bytes_vec(indices);
                    build_array_data_primitive_with_len(
                        DataType::Int64,
                        indices.len(),
                        indices,
                        validity,
                    )?
                }
            };

            let values = match dictionary {
                V::Utf8 { buffer, offsets } => {
                    let data = std::mem::take(&mut buffers.u8[*buffer]);
                    let offsets = std::mem::take(&mut buffers.u32_offsets[*offsets]);
                    build_array_data_utf8_old(data, offsets.offsets, None)?
                }
                V::LargeUtf8 { buffer, offsets } => {
                    let data = std::mem::take(&mut buffers.u8[*buffer]);
                    let offsets = std::mem::take(&mut buffers.u64_offsets[*offsets]);
                    build_array_data_large_utf8(data, offsets.offsets, None)?
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

fn build_array_data_utf8_old(
    data: Vec<u8>,
    offsets: Vec<i32>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    build_array_data_utf8_impl(DataType::Utf8, data, offsets, validity)
}

fn build_array_data_large_utf8(
    data: Vec<u8>,
    offsets: Vec<i64>,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    build_array_data_utf8_impl(DataType::LargeUtf8, data, offsets, validity)
}

fn build_array_data_utf8_impl<O: ArrowNativeType>(
    data_type: DataType,
    data: Vec<u8>,
    offsets: Vec<O>,
    validity: Option<MutableBitBuffer>,
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
