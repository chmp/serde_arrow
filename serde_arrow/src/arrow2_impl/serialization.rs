//! Build arrow2 arrays from individual buffers
//!
use crate::{
    _impl::arrow2::{
        array::{
            Array, BooleanArray, DictionaryArray, ListArray, MapArray, NullArray, PrimitiveArray,
            StructArray, UnionArray, Utf8Array,
        },
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::{DataType, Field},
        offset::OffsetsBuffer,
        types::{f16, NativeType, Offset},
    },
    internal::{
        common::{MutableBitBuffer, MutableBuffers},
        error::fail,
    },
};

use crate::internal::{
    common::{ArrayMapping, DictionaryIndex, DictionaryValue},
    conversions::ToBytes,
    error::Result,
    serialization_ng::ArrayBuilder,
};

impl ArrayBuilder {
    /// Build the arrow2 arrays
    pub fn build_arrow2_arrays(&mut self) -> Result<Vec<Box<dyn Array>>> {
        let fields = self.take_records()?;
        let arrays = fields
            .into_iter()
            .map(build_array)
            .collect::<Result<Vec<_>>>()?;
        Ok(arrays)
    }
}

fn build_array(builder: ArrayBuilder) -> Result<Box<dyn Array>> {
    use {ArrayBuilder as A, DataType as T};
    match builder {
        A::Null(builder) => Ok(Box::new(NullArray::new(T::Null, builder.count))),
        A::Bool(builder) => {
            let buffer = Bitmap::from_u8_vec(builder.buffer.buffer, builder.buffer.len);
            let validity = build_validity(builder.validity);
            Ok(Box::new(BooleanArray::try_new(
                T::Boolean,
                buffer,
                validity,
            )?))
        }
        A::I8(builder) => build_primitive_array(T::Int8, builder.buffer, builder.validity),
        A::I16(builder) => build_primitive_array(T::Int16, builder.buffer, builder.validity),
        A::I32(builder) => build_primitive_array(T::Int32, builder.buffer, builder.validity),
        A::I64(builder) => build_primitive_array(T::Int64, builder.buffer, builder.validity),
        A::U8(builder) => build_primitive_array(T::UInt8, builder.buffer, builder.validity),
        A::U16(builder) => build_primitive_array(T::UInt16, builder.buffer, builder.validity),
        A::U32(builder) => build_primitive_array(T::UInt32, builder.buffer, builder.validity),
        A::U64(builder) => build_primitive_array(T::UInt64, builder.buffer, builder.validity),
        A::F32(builder) => build_primitive_array(T::Float32, builder.buffer, builder.validity),
        A::F64(builder) => build_primitive_array(T::Float64, builder.buffer, builder.validity),
        A::Utf8(builder) => build_array_utf8_array(
            T::Utf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::LargeUtf8(builder) => build_array_utf8_array(
            T::LargeUtf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        A::Struct(builder) => {
            let mut values = Vec::new();
            for (_, field) in builder.named_fields {
                values.push(build_array(field)?);
            }

            let fields = builder
                .fields
                .iter()
                .map(Field::try_from)
                .collect::<Result<Vec<_>>>()?;
            let data_type = T::Struct(fields);
            let validity = build_validity(builder.validity);
            Ok(Box::new(StructArray::try_new(data_type, values, validity)?))
        }
        builder => fail!("Cannot build arrow2 array for {}", builder.name()),
    }
}

fn build_validity(validity: Option<MutableBitBuffer>) -> Option<Bitmap> {
    let validity = validity?;
    Some(Bitmap::from_u8_vec(validity.buffer, validity.len))
}

fn build_primitive_array<T: NativeType>(
    data_type: DataType,
    buffer: Vec<T>,
    validity: Option<MutableBitBuffer>,
) -> Result<Box<dyn Array>> {
    let buffer = Buffer::from(buffer);
    let validity = build_validity(validity);
    Ok(Box::new(PrimitiveArray::try_new(
        data_type, buffer, validity,
    )?))
}

fn build_array_utf8_array<O: Offset>(
    data_type: DataType,
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: Option<MutableBitBuffer>,
) -> Result<Box<dyn Array>> {
    Ok(Box::new(Utf8Array::new(
        data_type,
        OffsetsBuffer::try_from(offsets)?,
        Buffer::from(data),
        build_validity(validity),
    )))
}

// OLD CODE -- Delete, once reimplemented
macro_rules! build_array_primitive {
    ($buffers:expr, $ty:ty, $array:ident, $field:expr, $buffer:expr, $validity:expr) => {{
        let buffer = std::mem::take(&mut $buffers.$array[$buffer]);
        let buffer: Vec<$ty> = ToBytes::from_bytes_vec(buffer);
        let validity = build_validity_old($buffers, $validity);
        let data_type = Field::try_from($field)?.data_type;
        let array = PrimitiveArray::try_new(data_type, Buffer::from(buffer), validity)?;
        Ok(Box::new(array))
    }};
}

macro_rules! build_dictionary_from_indices {
    ($buffers:expr, $ty:ty, $array:ident, $variant:ident, $buffer:expr, $data_type:expr, $values:expr, $validity:expr) => {{
        let buffer = std::mem::take(&mut $buffers.$array[$buffer]);
        let buffer: Vec<$ty> = ToBytes::from_bytes_vec(buffer);
        let indices = PrimitiveArray::try_new(DataType::$variant, Buffer::from(buffer), $validity)?;

        Ok(Box::new(DictionaryArray::try_new(
            $data_type, indices, $values,
        )?))
    }};
}

fn build_array_old(buffers: &mut MutableBuffers, mapping: &ArrayMapping) -> Result<Box<dyn Array>> {
    use ArrayMapping as M;
    match mapping {
        M::Decimal128 {
            field,
            validity,
            buffer,
        } => {
            let buffer = std::mem::take(&mut buffers.u128[*buffer]);
            let buffer: Vec<i128> = ToBytes::from_bytes_vec(buffer);
            let validity = build_validity_old(buffers, *validity);
            let array = PrimitiveArray::try_new(
                Field::try_from(field)?.data_type,
                Buffer::from(buffer),
                validity,
            )?;
            Ok(Box::new(array))
        }
        M::F16 {
            buffer, validity, ..
        } => {
            let buffer = std::mem::take(&mut buffers.u16[*buffer]);
            let buffer: Vec<f16> = buffer.into_iter().map(f16::from_bits).collect();
            let validity = build_validity_old(buffers, *validity);
            let array = PrimitiveArray::try_new(DataType::Float16, Buffer::from(buffer), validity)?;
            Ok(Box::new(array))
        }
        M::Date64 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i64, u64, field, *buffer, *validity),
        M::Dictionary {
            field,
            dictionary,
            indices,
            validity,
        } => {
            use {DictionaryIndex as I, DictionaryValue as V};

            let data_type = Field::try_from(field)?.data_type;
            let validity = build_validity_old(buffers, *validity);
            let values: Box<dyn Array> = match dictionary {
                V::Utf8 { buffer, offsets } => {
                    let data = std::mem::take(&mut buffers.u8[*buffer]);
                    let offsets = std::mem::take(&mut buffers.u32_offsets[*offsets]);
                    Box::new(Utf8Array::new(
                        DataType::Utf8,
                        OffsetsBuffer::try_from(offsets.offsets)?,
                        Buffer::from(data),
                        None,
                    ))
                }
                V::LargeUtf8 { buffer, offsets } => {
                    let data = std::mem::take(&mut buffers.u8[*buffer]);
                    let offsets = std::mem::take(&mut buffers.u64_offsets[*offsets]);
                    Box::new(Utf8Array::new(
                        DataType::LargeUtf8,
                        OffsetsBuffer::try_from(offsets.offsets)?,
                        Buffer::from(data),
                        None,
                    ))
                }
            };

            match indices {
                I::U8(indices) => build_dictionary_from_indices!(
                    buffers, u8, u8, UInt8, *indices, data_type, values, validity
                ),
                I::U16(indices) => build_dictionary_from_indices!(
                    buffers, u16, u16, UInt16, *indices, data_type, values, validity
                ),
                I::U32(indices) => build_dictionary_from_indices!(
                    buffers, u32, u32, UInt32, *indices, data_type, values, validity
                ),
                I::U64(indices) => build_dictionary_from_indices!(
                    buffers, u64, u64, UInt64, *indices, data_type, values, validity
                ),
                I::I8(indices) => build_dictionary_from_indices!(
                    buffers, i8, u8, Int8, *indices, data_type, values, validity
                ),
                I::I16(indices) => build_dictionary_from_indices!(
                    buffers, i16, u16, Int16, *indices, data_type, values, validity
                ),
                I::I32(indices) => build_dictionary_from_indices!(
                    buffers, i32, u32, Int32, *indices, data_type, values, validity
                ),
                I::I64(indices) => build_dictionary_from_indices!(
                    buffers, i64, u64, Int64, *indices, data_type, values, validity
                ),
            }
        }
        M::List {
            field,
            item,
            offsets,
            validity,
        } => {
            let data_type = Field::try_from(field)?.data_type;
            let values = build_array_old(buffers, item)?;
            let validity = build_validity_old(buffers, *validity);
            let offsets = std::mem::take(&mut buffers.u32_offsets[*offsets]);
            let offsets = OffsetsBuffer::try_from(offsets.offsets)?;

            Ok(Box::new(ListArray::try_new(
                data_type, offsets, values, validity,
            )?))
        }
        M::LargeList {
            field,
            item,
            offsets,
            validity,
        } => {
            let data_type = Field::try_from(field)?.data_type;
            let values = build_array_old(buffers, item)?;
            let validity = build_validity_old(buffers, *validity);
            let offsets = std::mem::take(&mut buffers.u64_offsets[*offsets]);
            let offsets = OffsetsBuffer::try_from(offsets.offsets)?;

            Ok(Box::new(ListArray::try_new(
                data_type, offsets, values, validity,
            )?))
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

            let data_type = Field::try_from(field)?.data_type;

            for &t in &types {
                offsets.push(current_offset[t as usize]);
                current_offset[t as usize] += 1;
            }

            let mut children = Vec::new();
            for child in fields {
                children.push(build_array_old(buffers, child)?);
            }

            let types = Buffer::from(types);
            let offsets = Buffer::from(offsets);

            Ok(Box::new(UnionArray::try_new(
                data_type,
                types,
                children,
                Some(offsets),
            )?))
        }
        M::Map {
            field,
            entries,
            offsets,
            validity,
        } => {
            let entries = build_array_old(buffers, entries)?;
            let data_type = Field::try_from(field)?.data_type;

            let offsets = std::mem::take(&mut buffers.u32_offsets[*offsets]);
            let offsets = OffsetsBuffer::try_from(offsets.offsets)?;

            let validity = build_validity_old(buffers, *validity);

            Ok(Box::new(MapArray::try_new(
                data_type, offsets, entries, validity,
            )?))
        }
        _ => todo!(),
    }
}

fn build_array_utf8_old(
    buffers: &mut MutableBuffers,
    buffer_idx: usize,
    offsets_idx: usize,
    validity_idx: Option<usize>,
) -> Result<Box<dyn Array>> {
    let data = std::mem::take(&mut buffers.u8[buffer_idx]);
    let offsets = std::mem::take(&mut buffers.u32_offsets[offsets_idx]);
    let validity = build_validity_old(buffers, validity_idx);

    Ok(Box::new(Utf8Array::new(
        DataType::Utf8,
        OffsetsBuffer::try_from(offsets.offsets)?,
        Buffer::from(data),
        validity,
    )))
}

fn build_array_large_utf8_old(
    buffers: &mut MutableBuffers,
    buffer_idx: usize,
    offsets_idx: usize,
    validity_idx: Option<usize>,
) -> Result<Box<dyn Array>> {
    let data = std::mem::take(&mut buffers.u8[buffer_idx]);
    let offsets = std::mem::take(&mut buffers.u64_offsets[offsets_idx]);
    let validity = build_validity_old(buffers, validity_idx);

    Ok(Box::new(Utf8Array::new(
        DataType::LargeUtf8,
        OffsetsBuffer::try_from(offsets.offsets)?,
        Buffer::from(data),
        validity,
    )))
}

fn build_validity_old(buffers: &mut MutableBuffers, validity_idx: Option<usize>) -> Option<Bitmap> {
    let val = std::mem::take(&mut buffers.u1[validity_idx?]);
    Some(Bitmap::from_u8_vec(val.buffer, val.len))
}
