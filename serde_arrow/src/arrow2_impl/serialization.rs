//! Build arrow2 arrays from individual buffers
//!
use crate::{
    _impl::arrow2::{
        array::{
            Array, BooleanArray, DictionaryArray, DictionaryKey, ListArray, MapArray, NullArray,
            PrimitiveArray, StructArray, UnionArray, Utf8Array,
        },
        bitmap::Bitmap,
        buffer::Buffer,
        datatypes::{DataType, Field},
        offset::OffsetsBuffer,
        types::{f16, NativeType, Offset},
    },
    internal::{
        common::MutableBitBuffer,
        error::{fail, Result},
        schema::{GenericField, SerdeArrowSchema},
        serialization::{ArrayBuilder, OuterSequenceBuilder},
    },
};

impl crate::internal::array_builder::ArrayBuilder {
    /// TODO: document me
    pub fn from_arrow2(fields: &[Field]) -> Result<Self> {
        let fields = fields
            .iter()
            .map(GenericField::try_from)
            .collect::<Result<Vec<_>>>()?;
        let schema = SerdeArrowSchema { fields };
        Ok(Self(OuterSequenceBuilder::new(&schema)?))
    }

    /// TODO: document me
    pub fn to_arrow2(&mut self) -> Result<Vec<Box<dyn Array>>> {
        self.0.build_arrow2()
    }
}

impl OuterSequenceBuilder {
    /// Build the arrow2 arrays
    pub fn build_arrow2(&mut self) -> Result<Vec<Box<dyn Array>>> {
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
        A::UnknownVariant(_) => Ok(Box::new(NullArray::new(T::Null, 0))),
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
        A::F16(builder) => build_primitive_array(
            T::Float16,
            builder
                .buffer
                .into_iter()
                .map(|v| f16::from_bits(v.to_bits()))
                .collect(),
            builder.validity,
        ),
        A::F32(builder) => build_primitive_array(T::Float32, builder.buffer, builder.validity),
        A::F64(builder) => build_primitive_array(T::Float64, builder.buffer, builder.validity),
        A::Date32(builder) => build_primitive_array(
            Field::try_from(&builder.field)?.data_type,
            builder.buffer,
            builder.validity,
        ),
        A::Date64(builder) => build_primitive_array(
            Field::try_from(&builder.field)?.data_type,
            builder.buffer,
            builder.validity,
        ),
        A::Time32(builder) => build_primitive_array(
            Field::try_from(&builder.field)?.data_type,
            builder.buffer,
            builder.validity,
        ),
        A::Time64(builder) => build_primitive_array(
            Field::try_from(&builder.field)?.data_type,
            builder.buffer,
            builder.validity,
        ),
        A::Duration(builder) => build_primitive_array(
            T::Duration(builder.unit.into()),
            builder.buffer,
            builder.validity,
        ),
        A::Decimal128(builder) => build_primitive_array(
            T::Decimal(builder.precision as usize, usize::try_from(builder.scale)?),
            builder.buffer,
            builder.validity,
        ),
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
