//! Build arrow2 arrays from individual buffers
//!
use crate::{
    _impl::arrow2::{
        array::{
            Array, DictionaryArray, DictionaryKey, ListArray, MapArray, PrimitiveArray,
            StructArray, UnionArray,
        },
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::{DataType, Field},
        offset::OffsetsBuffer,
    },
    internal::{
        error::{fail, Result},
        schema::GenericField,
        serialization::{utils::MutableBitBuffer, ArrayBuilder},
    },
};

pub fn build_array(builder: ArrayBuilder) -> Result<Box<dyn Array>> {
    use {ArrayBuilder as A, DataType as T};
    match builder {
        A::Null(_)
        | A::UnknownVariant(_)
        | A::I8(_)
        | A::I16(_)
        | A::I32(_)
        | A::I64(_)
        | A::U8(_)
        | A::U16(_)
        | A::U32(_)
        | A::U64(_)
        | A::F16(_)
        | A::F32(_)
        | A::F64(_)
        | A::Date32(_)
        | A::Date64(_)
        | A::Duration(_)
        | A::Time32(_)
        | A::Time64(_)
        | A::Decimal128(_)
        | A::Bool(_)
        | A::Utf8(_)
        | A::LargeUtf8(_)
        | A::Binary(_)
        | A::LargeBinary(_) => builder.into_array()?.try_into(),
        A::LargeList(builder) => Ok(Box::new(ListArray::try_new(
            T::LargeList(Box::new(Field::try_from(&builder.field)?)),
            OffsetsBuffer::try_from(builder.offsets.offsets)?,
            build_array(*builder.element)?,
            build_validity(builder.validity),
        )?)),
        A::List(builder) => Ok(Box::new(ListArray::try_new(
            T::List(Box::new(Field::try_from(&builder.field)?)),
            OffsetsBuffer::try_from(builder.offsets.offsets)?,
            build_array(*builder.element)?,
            build_validity(builder.validity),
        )?)),
        A::FixedSizedList(_) => fail!("FixedSizedList is not supported by arrow2"),
        A::FixedSizeBinary(_) => fail!("FixedSizeBinary is not supported by arrow2"),
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
            Ok(Box::new(StructArray::try_new(
                T::Struct(fields),
                values,
                build_validity(builder.validity),
            )?))
        }
        A::Map(builder) => Ok(Box::new(MapArray::try_new(
            T::Map(Box::new(Field::try_from(&builder.entry_field)?), false),
            OffsetsBuffer::try_from(builder.offsets.offsets)?,
            build_array(*builder.entry)?,
            build_validity(builder.validity),
        )?)),
        A::DictionaryUtf8(builder) => {
            let values = build_array(*builder.values)?;
            match *builder.indices {
                A::U8(ib) => {
                    build_dictionary_array(builder.field, T::UInt8, ib.buffer, ib.validity, values)
                }
                A::U16(ib) => {
                    build_dictionary_array(builder.field, T::UInt16, ib.buffer, ib.validity, values)
                }
                A::U32(ib) => {
                    build_dictionary_array(builder.field, T::UInt32, ib.buffer, ib.validity, values)
                }
                A::U64(ib) => {
                    build_dictionary_array(builder.field, T::UInt64, ib.buffer, ib.validity, values)
                }
                A::I8(ib) => {
                    build_dictionary_array(builder.field, T::Int8, ib.buffer, ib.validity, values)
                }
                A::I16(ib) => {
                    build_dictionary_array(builder.field, T::Int16, ib.buffer, ib.validity, values)
                }
                A::I32(ib) => {
                    build_dictionary_array(builder.field, T::Int32, ib.buffer, ib.validity, values)
                }
                A::I64(ib) => {
                    build_dictionary_array(builder.field, T::Int64, ib.buffer, ib.validity, values)
                }
                builder => fail!("Cannot use {} as an index for a dictionary", builder.name()),
            }
        }
        A::Union(builder) => {
            let data_type = Field::try_from(&builder.field)?.data_type;
            let children = builder
                .fields
                .into_iter()
                .map(build_array)
                .collect::<Result<_>>()?;
            Ok(Box::new(UnionArray::try_new(
                data_type,
                Buffer::from(builder.types),
                children,
                Some(Buffer::from(builder.offsets)),
            )?))
        }
    }
}

fn build_validity(validity: Option<MutableBitBuffer>) -> Option<Bitmap> {
    let validity = validity?;
    Some(Bitmap::from_u8_vec(validity.buffer, validity.len))
}

fn build_dictionary_array<K: DictionaryKey>(
    field: GenericField,
    data_type: DataType,
    indices: Vec<K>,
    validity: Option<MutableBitBuffer>,
    values: Box<dyn Array>,
) -> Result<Box<dyn Array>> {
    let indices = PrimitiveArray::new(data_type, Buffer::from(indices), build_validity(validity));
    let data_type = Field::try_from(&field)?.data_type;
    Ok(Box::new(DictionaryArray::try_new(
        data_type, indices, values,
    )?))
}
