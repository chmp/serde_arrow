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
    use {ArrayBuilder as A, DataType as T};
    match builder {
        A::Null(builder) => Ok(NullArray::new(builder.count).into_data()),
        A::Bool(builder) => build_array_data_primitive_with_len(
            T::Boolean,
            builder.buffer.len(),
            builder.buffer.buffer,
            builder.validity,
        ),
        A::I8(builder) => build_array_data_primitive(T::Int8, builder.buffer, builder.validity),
        A::I16(builder) => build_array_data_primitive(T::Int16, builder.buffer, builder.validity),
        A::I32(builder) => build_array_data_primitive(T::Int32, builder.buffer, builder.validity),
        A::I64(builder) => build_array_data_primitive(T::Int64, builder.buffer, builder.validity),
        A::U8(builder) => build_array_data_primitive(T::UInt8, builder.buffer, builder.validity),
        A::U16(builder) => build_array_data_primitive(T::UInt16, builder.buffer, builder.validity),
        A::U32(builder) => build_array_data_primitive(T::UInt32, builder.buffer, builder.validity),
        A::U64(builder) => build_array_data_primitive(T::UInt64, builder.buffer, builder.validity),
        A::F32(builder) => build_array_data_primitive(T::Float32, builder.buffer, builder.validity),
        A::F64(builder) => build_array_data_primitive(T::Float64, builder.buffer, builder.validity),
        A::Utf8(builder) => build_array_data_utf8(
            T::Utf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::LargeUtf8(builder) => build_array_data_utf8(
            T::LargeUtf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::Struct(builder) => {
            let mut data = Vec::new();
            for (_, field) in builder.named_fields {
                data.push(build_array_data(field)?);
            }

            let (validity, len) = if let Some(validity) = builder.validity {
                (Some(Buffer::from(validity.buffer)), validity.len)
            } else {
                if data.is_empty() {
                    fail!("cannot built non-nullable structs without fields");
                }
                (None, data[0].len())
            };

            let fields = builder
                .fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<Vec<_>>>()?;
            let data_type = T::Struct(fields.into());

            Ok(ArrayData::builder(data_type)
                .len(len)
                .null_bit_buffer(validity)
                .child_data(data)
                .build()?)
        }
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
        _ => todo!(),
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
