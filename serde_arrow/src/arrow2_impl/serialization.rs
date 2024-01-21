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
    match builder {
        ArrayBuilder::Bool(builder) => {
            let buffer = Bitmap::from_u8_vec(builder.buffer.buffer, builder.buffer.len);
            let validity = build_validity(builder.validity);
            Ok(Box::new(BooleanArray::try_new(
                DataType::Boolean,
                buffer,
                validity,
            )?))
        }
        ArrayBuilder::I8(builder) => {
            build_primitive_array(DataType::Int8, builder.buffer, builder.validity)
        }
        ArrayBuilder::I16(builder) => {
            build_primitive_array(DataType::Int16, builder.buffer, builder.validity)
        }
        ArrayBuilder::I32(builder) => {
            build_primitive_array(DataType::Int32, builder.buffer, builder.validity)
        }
        ArrayBuilder::I64(builder) => {
            build_primitive_array(DataType::Int64, builder.buffer, builder.validity)
        }
        ArrayBuilder::U8(builder) => {
            build_primitive_array(DataType::UInt8, builder.buffer, builder.validity)
        }
        ArrayBuilder::U16(builder) => {
            build_primitive_array(DataType::UInt16, builder.buffer, builder.validity)
        }
        ArrayBuilder::U32(builder) => {
            build_primitive_array(DataType::UInt32, builder.buffer, builder.validity)
        }
        ArrayBuilder::U64(builder) => {
            build_primitive_array(DataType::UInt64, builder.buffer, builder.validity)
        }
        ArrayBuilder::F32(builder) => {
            build_primitive_array(DataType::Float32, builder.buffer, builder.validity)
        }
        ArrayBuilder::F64(builder) => {
            build_primitive_array(DataType::Float64, builder.buffer, builder.validity)
        }
        ArrayBuilder::Utf8(builder) => build_array_utf8_array(
            DataType::Utf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
        ArrayBuilder::LargeUtf8(builder) => build_array_utf8_array(
            DataType::LargeUtf8,
            builder.offsets.offsets,
            builder.buffer,
            builder.validity,
        ),
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
        M::Null { buffer, .. } => {
            let buffer = std::mem::take(&mut buffers.u0[*buffer]);
            Ok(Box::new(NullArray::new(DataType::Null, buffer.len())))
        }
        M::Bool {
            buffer, validity, ..
        } => {
            let buffer = std::mem::take(&mut buffers.u1[*buffer]);
            let buffer = Bitmap::from_u8_vec(buffer.buffer, buffer.len);
            let validity = validity.map(|val| std::mem::take(&mut buffers.u1[val]));
            let validity = validity.map(|val| Bitmap::from_u8_vec(val.buffer, val.len));
            let array = BooleanArray::try_new(DataType::Boolean, buffer, validity)?;
            Ok(Box::new(array))
        }
        M::U8 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, u8, u8, field, *buffer, *validity),
        M::U16 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, u16, u16, field, *buffer, *validity),
        M::U32 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, u32, u32, field, *buffer, *validity),
        M::U64 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, u64, u64, field, *buffer, *validity),
        M::I8 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i8, u8, field, *buffer, *validity),
        M::I16 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i16, u16, field, *buffer, *validity),
        M::I32 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i32, u32, field, *buffer, *validity),
        M::I64 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i64, u64, field, *buffer, *validity),
        M::F32 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, f32, u32, field, *buffer, *validity),
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
        M::F64 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, f64, u64, field, *buffer, *validity),
        M::Date64 {
            field,
            buffer,
            validity,
            ..
        } => build_array_primitive!(buffers, i64, u64, field, *buffer, *validity),
        M::Utf8 {
            buffer,
            offsets,
            validity,
            ..
        } => build_array_utf8_old(buffers, *buffer, *offsets, *validity),
        M::LargeUtf8 {
            buffer,
            offsets,
            validity,
            ..
        } => build_array_large_utf8_old(buffers, *buffer, *offsets, *validity),
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
        M::Struct {
            field,
            fields,
            validity,
        } => {
            let mut values = Vec::new();
            for field in fields {
                values.push(build_array_old(buffers, field)?);
            }

            let data_type = Field::try_from(field)?.data_type;
            let validity = build_validity_old(buffers, *validity);

            Ok(Box::new(StructArray::try_new(data_type, values, validity)?))
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
