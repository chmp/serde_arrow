#![allow(missing_docs)]
use std::sync::Arc;

use crate::{
    _impl::arrow::{
        array::{make_array, Array, ArrayData, ArrayRef, NullArray},
        buffer::{Buffer, ScalarBuffer},
        datatypes::{ArrowNativeType, ArrowPrimitiveType, DataType, Field, Float16Type},
    },
    internal::{
        common::MutableBitBuffer,
        error::{fail, Result},
        serialization_ng::{ArrayBuilder, OuterSequenceBuilder},
    },
};

impl OuterSequenceBuilder {
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
        A::UnknownVariant(_) => Ok(NullArray::new(0).into_data()),
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
        A::F16(builder) => build_array_data_primitive(
            T::Float16,
            builder
                .buffer
                .into_iter()
                .map(|v| <Float16Type as ArrowPrimitiveType>::Native::from_bits(v.to_bits()))
                .collect(),
            builder.validity,
        ),
        A::F32(builder) => build_array_data_primitive(T::Float32, builder.buffer, builder.validity),
        A::F64(builder) => build_array_data_primitive(T::Float64, builder.buffer, builder.validity),
        A::Date64(builder) => build_array_data_primitive(
            Field::try_from(&builder.field)?.data_type().clone(),
            builder.buffer,
            builder.validity,
        ),
        A::Decimal128(builder) => build_array_data_primitive(
            T::Decimal128(builder.precision, builder.scale),
            builder.buffer,
            builder.validity,
        ),
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
        A::LargeList(builder) => build_array_data_list(
            T::LargeList(Arc::new(Field::try_from(&builder.field)?)),
            builder.offsets.offsets.len() - 1,
            builder.offsets.offsets,
            build_array_data(*builder.element)?,
            builder.validity,
        ),
        A::List(builder) => build_array_data_list(
            T::List(Arc::new(Field::try_from(&builder.field)?)),
            builder.offsets.offsets.len() - 1,
            builder.offsets.offsets,
            build_array_data(*builder.element)?,
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
        A::Map(builder) => Ok(ArrayData::builder(T::Map(
            Arc::new(Field::try_from(&builder.entry_field)?),
            false,
        ))
        .len(builder.offsets.offsets.len() - 1)
        .add_buffer(ScalarBuffer::from(builder.offsets.offsets).into_inner())
        .add_child_data(build_array_data(*builder.entry)?)
        .null_bit_buffer(builder.validity.map(|b| Buffer::from(b.buffer)))
        .build()?),
        A::DictionaryUtf8(builder) => {
            let indices = build_array_data(*builder.indices)?;
            let values = build_array_data(*builder.values)?;
            let data_type = Field::try_from(&builder.field)?.data_type().clone();

            Ok(indices
                .into_builder()
                .data_type(data_type)
                .child_data(vec![values])
                .build()?)
        }
        A::Union(builder) => {
            let data_type = Field::try_from(&builder.field)?.data_type().clone();
            let children = builder
                .fields
                .into_iter()
                .map(build_array_data)
                .collect::<Result<Vec<_>>>()?;
            let len = builder.types.len();

            Ok(ArrayData::builder(data_type)
                .len(len)
                .add_buffer(Buffer::from_vec(builder.types))
                .add_buffer(Buffer::from_vec(builder.offsets))
                .child_data(children)
                .build()?)
        }
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

fn build_array_data_list<O: ArrowNativeType>(
    data_type: DataType,
    len: usize,
    offsets: Vec<O>,
    child_data: ArrayData,
    validity: Option<MutableBitBuffer>,
) -> Result<ArrayData> {
    let offset_buffer = ScalarBuffer::from(offsets).into_inner();
    let validity = validity.map(|b| Buffer::from(b.buffer));

    Ok(ArrayData::builder(data_type)
        .len(len)
        .add_buffer(offset_buffer)
        .add_child_data(child_data)
        .null_bit_buffer(validity)
        .build()?)
}
