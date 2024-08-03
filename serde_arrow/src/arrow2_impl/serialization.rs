//! Build arrow2 arrays from individual buffers
//!
use crate::{
    _impl::arrow2::{
        array::{Array, DictionaryArray, DictionaryKey, PrimitiveArray, UnionArray},
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::{DataType, Field},
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
        _ => builder.into_array()?.try_into(),
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
